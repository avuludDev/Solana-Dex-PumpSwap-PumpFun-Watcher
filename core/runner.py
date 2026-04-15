import asyncio
import json
import logging
import time
from datetime import datetime
from typing import List, Optional

import aiohttp

from config.settings import load_settings
from infra.solana_stream import SolanaStream
from infra.db import Database
from infra.price_oracle import PriceOracle
from infra.parser_base import ProtocolParser
from infra.helius_metadata import fetch_token_name_symbol
from infra.sol_price import SolPriceService

from protocols.pump_fun import PumpFunParser
from protocols.pumpswap import PumpSwapParser  # аналогично
from protocols.meteora import MeteoraParser    # аналогично

from filters.amount_filter import MinAmountUsdFilter, CompositeFilter
from storage.repositories import SwapRepository, PoolRepository, TokenRepository
from core.types import SwapEvent, PoolCreatedEvent


logger = logging.getLogger(__name__)


class BotRunner:
    def __init__(self) -> None:
        self.settings = load_settings()
        # БД может быть не настроена/недоступна — тогда работаем только на логах.
        self.db: Optional[Database] = Database(self.settings.db.dsn)
        self._use_db: bool = True
        self.price_oracle = PriceOracle()
        self.sol_price_service = SolPriceService(
            interval_seconds=300.0,
            helius_http_endpoint=self.settings.solana.http_endpoint,
        )

        # Файл, куда будем периодически сохранять последний обработанный слот,
        # чтобы при рестарте не терять диапазон.
        self._last_slot_file = "last_processed_slot.txt"

        self.parsers: List[ProtocolParser] = [
            # PumpFunParser(self.settings.pump_fun_program_id),
            PumpSwapParser(
                self.settings.pumpswap_program_id,
                min_sol_lamports=self.settings.filters.min_sol_lamports,
            ),
            MeteoraParser(
                self.settings.meteora_program_id,
                min_sol_lamports=self.settings.filters.min_sol_lamports,
            ),
        ]

        self.filters = CompositeFilter([
            MinAmountUsdFilter(self.settings.filters.min_swap_usd),
            # другие фильтры
        ])

        self.swap_repo = SwapRepository(self.db)
        self.pool_repo = PoolRepository(self.db)
        self.token_repo = TokenRepository(self.db)
        # Простой in‑memory кэш метаданных токенов, чтобы не дёргать Helius
        # для одного и того же mint по нескольку раз за сессию.
        self._token_meta_cache: dict[str, tuple[Optional[str], Optional[str], Optional[int], Optional[float]]] = {}

        # Статистика режима слотов (слоты, транзакции, по протоколам, макс. обработанный слот).
        self._slots_processed: int = 0
        self._txs_processed: int = 0
        self._txs_pump_fun: int = 0
        self._txs_meteora: int = 0
        self._max_processed_slot: int = 0
        # Накопленные замеры времени, чтобы понимать, где узкое место:
        # getBlock (RPC) или обработка транзакций (парсеры/фильтры/БД).
        self._total_getblock_ms: float = 0.0
        self._total_process_ms: float = 0.0
        self._timed_blocks: int = 0
        # Можно быстро выключить обновление метаданных токенов на свапах,
        # если оно оказывается слишком тяжёлым (по умолчанию включено).
        self._update_tokens_from_swaps: bool = False

    def _reset_slot_stats(self) -> None:
        self._slots_processed = 0
        self._txs_processed = 0
        self._txs_pump_fun = 0
        self._txs_meteora = 0
        self._max_processed_slot = 0

    # -----------------------------
    # Работа с файлом последнего слота
    # -----------------------------

    def _load_last_processed_slot(self) -> Optional[int]:
        """
        Прочитать последний обработанный слот из файла (если есть).
        """
        try:
            with open(self._last_slot_file, "r", encoding="utf-8") as f:
                value = f.read().strip()
            if not value:
                return None
            return int(value)
        except FileNotFoundError:
            return None
        except Exception as exc:  # pragma: no cover
            logger.warning("Не удалось прочитать %s: %s", self._last_slot_file, exc)
            return None

    def _save_last_processed_slot(self, slot: int) -> None:
        """
        Сохранить последний обработанный слот в файл.
        """
        if slot <= 0:
            return
        try:
            with open(self._last_slot_file, "w", encoding="utf-8") as f:
                f.write(str(slot))
        except Exception as exc:  # pragma: no cover
            logger.warning("Не удалось записать %s: %s", self._last_slot_file, exc)

    def get_slot_stats(self) -> dict:
        """Текущая статистика: слотов, транзакций, по протоколам, макс. обработанный слот."""
        return {
            "slots": self._slots_processed,
            "transactions": self._txs_processed,
            "pump_fun": self._txs_pump_fun,
            "meteora": self._txs_meteora,
            "max_processed_slot": self._max_processed_slot,
        }

    async def _ensure_db(self) -> None:
        """Подключиться к БД; при ошибке выставить _use_db=False и работать без сохранения."""
        try:
            if self.db is not None:
                await self.db.connect()
                logger.info("[DB] Подключено к PostgreSQL")
        except Exception as exc:  # pragma: no cover
            logger.warning("Не удалось подключиться к БД, работаем без сохранения: %s", exc)
            self._use_db = False

    async def _handle_raw_tx(
        self, raw_tx: dict, session: aiohttp.ClientSession
    ) -> tuple[bool, list[str]]:
        """
        Обрабатывает сырую транзакцию.
        Возвращает (had_pool, protocols): создан ли пул и список протоколов, которым принадлежит tx.
        """
        had_pool = False
        protocols: list[str] = []
        for parser in self.parsers:
            if not parser.supports(raw_tx):
                continue
            protocols.append(parser.protocol_name)

        # Пулы
            for pool_event in parser.parse_pool_creations(raw_tx):
                if self.filters.filter_pool(pool_event):
                    had_pool = True
                await self._handle_pool_event(pool_event, session)

            # Свапы (в т.ч. Meteora с фильтром по сумме SOL).
            for swap_event in parser.parse_swaps(raw_tx):
                await self._handle_swap_event(swap_event, session)
        return had_pool, protocols

    async def _handle_pool_event(self, event: PoolCreatedEvent, session: aiohttp.ClientSession) -> None:
        if not self.filters.filter_pool(event):
            return

        if self.token_repo is not None and event.base_mint:
            try:
                await self.token_repo.upsert_token(
                    mint=event.base_mint,
                    name=None,
                    symbol=None,
                    decimals=None,
                    total_supply=None,
                    slot=event.slot,
                    tx_signature=event.tx_signature,
                )
            except Exception as exc:
                logger.warning("Не удалось обновить токен %s: %s", event.base_mint, exc)

        if self._use_db and self.pool_repo is not None:
            await self.pool_repo.save_pool(event)

    async def _handle_swap_event(self, event: SwapEvent, session: aiohttp.ClientSession) -> None:
        # Сумма свапа в USD по SOL-стороне (курс SOL из фонового парсера)
        WSOL = "So11111111111111111111111111111111111111112"
        sol_lamports = (
            event.in_amount if event.in_mint == WSOL else (event.out_amount if event.out_mint == WSOL else None)
        )
        sol_price = self.sol_price_service.get_sol_price_usd()
        if sol_lamports is not None and sol_price is not None:
            event.amount_usd = (sol_lamports / 1e9) * sol_price
        else:
            event.amount_usd = None

        if not self.filters.filter_swap(event):
            return


        # Обновляем базовую информацию о токенах из свапа (name/symbol/decimals через getAsset),
        # если эта опция включена. Сейчас отключено для снижения нагрузки и ускорения парсинга.
        if self._update_tokens_from_swaps and self.token_repo is not None:
            try:
                mints_to_update: set[str] = set()
                if event.in_mint and event.in_mint != "So11111111111111111111111111111111111111112":
                    mints_to_update.add(event.in_mint)
                if event.out_mint and event.out_mint != "So11111111111111111111111111111111111111112":
                    mints_to_update.add(event.out_mint)
                if mints_to_update:
                    for mint in mints_to_update:
                        if mint in self._token_meta_cache:
                            name, symbol, decimals, total_supply = self._token_meta_cache[mint]
                        else:
                            name, symbol, decimals, total_supply = await fetch_token_name_symbol(
                                self.settings.solana.http_endpoint,
                                session,
                                mint,
                            )
                            self._token_meta_cache[mint] = (name, symbol, decimals, total_supply)
                        await self.token_repo.upsert_token(
                            mint=mint,
                            name=name,
                            symbol=symbol,
                            decimals=decimals,
                            total_supply=total_supply,
                            slot=event.slot,
                            tx_signature=event.tx_signature,
                        )
            except Exception as exc:
                logger.warning(
                    "Не удалось обновить токены из свапа %s/%s: %s",
                    event.in_mint,
                    event.out_mint,
                    exc,
                )

        # Пытаемся сохранить свап, если БД доступна.
        if self._use_db and self.swap_repo is not None:
            await self.swap_repo.save_swap(event)

    async def run(self) -> None:
        await self._ensure_db()

        # Для разных программ (Pump.Fun, Meteora и т.п.) поднимаем отдельные стримы,
        # т.к. Helius logsSubscribe по mentions принимает только один pubkey.
        streams = [
            # SolanaStream(
            #     ws_endpoint=self.settings.solana.ws_endpoint,
            #     account_include=[self.settings.pump_fun_program_id],
            #     commitment=self.settings.solana.commitment,
            # ).set_http_endpoint(self.settings.solana.http_endpoint),
            SolanaStream(
                ws_endpoint=self.settings.solana.ws_endpoint,
                account_include=[self.settings.meteora_program_id],
                commitment=self.settings.solana.commitment,
            ).set_http_endpoint(self.settings.solana.http_endpoint),
        ]
        try:
            await asyncio.gather(*(s.run(self._handle_raw_tx) for s in streams))
        finally:
            if self.db is not None and self._use_db:
                # Перед закрытием соединения стараемся дописать накопленные события.
                if self.pool_repo is not None:
                    await self.pool_repo.flush()
                if self.swap_repo is not None:
                    await self.swap_repo.flush()
                await self.db.close()

    # -----------------------------
    # Воркеры, которые ходят по слотам
    # -----------------------------

    async def _get_current_slot(self, session: aiohttp.ClientSession) -> Optional[int]:
        """
        Получить текущий слот через RPC (getSlot).
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [
                {
                    "commitment": self.settings.solana.commitment,
                }
            ],
        }
        try:
            async with session.post(self.settings.solana.http_endpoint, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.warning(
                        "[HTTP] getSlot FAILED status=%s body=%s",
                        resp.status,
                        text[:300],
                    )
                    return None
                data = await resp.json()
        except Exception as exc:
            logger.exception("[HTTP] getSlot exception: %s", exc)
            return None

        result = data.get("result")
        if not isinstance(result, int):
            return None
        return result

    async def _fetch_block(self, session: aiohttp.ClientSession, slot: int) -> Optional[dict]:
        """
        Получить блок по слоту через HTTP RPC (getBlock).

        Берём encoding=jsonParsed, чтобы сразу иметь accountKeys / meta.logMessages
        в удобном виде (как сейчас ожидают парсеры Pump.Fun / Meteora).
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBlock",
            "params": [
                slot,
                {
                    "encoding": "jsonParsed",
                    "transactionDetails": "full",
                    "rewards": False,
                    "maxSupportedTransactionVersion": 0,
                    "commitment": self.settings.solana.commitment,
                },
            ],
        }

        # Делаем несколько попыток получить блок, чтобы не терять слоты
        # из‑за временных сетевых ошибок/таймаутов.
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                async with session.post(self.settings.solana.http_endpoint, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.warning(
                            "[HTTP] getBlock FAILED slot=%s status=%s body=%s (attempt %s/%s)",
                            slot,
                            resp.status,
                            text[:300],
                            attempt,
                            max_attempts,
                        )
                        # При ошибочном статусе нет смысла ретраить немедленно.
                        return None
                    data = await resp.json()
                break
            except asyncio.TimeoutError:
                logger.warning(
                    "[HTTP] getBlock timeout slot=%s (attempt %s/%s)",
                    slot,
                    attempt,
                    max_attempts,
                )
                if attempt == max_attempts:
                    return None
                continue
            except Exception as exc:
                logger.exception("[HTTP] getBlock exception slot=%s (attempt %s/%s): %s", slot, attempt, max_attempts, exc)
                return None

        result = data.get("result")
        if not isinstance(result, dict):
            return None
        return result

    async def _slot_worker(self, name: str, slot_queue: "asyncio.Queue[int]") -> None:
        """
        Воркер: берёт слоты из очереди, делает getBlock и прогоняет все транзакции
        через текущий пайплайн (_handle_raw_tx).
        """
        async with aiohttp.ClientSession() as session:
            while True:
                slot = await slot_queue.get()
                if slot is None:
                    slot_queue.task_done()
                    break

                t0 = time.monotonic()
                block = await self._fetch_block(session, slot)
                t1 = time.monotonic()
                getblock_ms = (t1 - t0) * 1000.0

                if not block:
                    # Засчитываем только время getBlock, блок пустой/не получен.
                    self._total_getblock_ms += getblock_ms
                    self._timed_blocks += 1
                    slot_queue.task_done()
                    continue

                txs = block.get("transactions") or []
                block_time = block.get("blockTime")

                t_proc_start = time.monotonic()
                for tx in txs:
                    if not isinstance(tx, dict):
                        continue
                    raw_tx = {
                        "slot": block.get("slot") or slot,
                        "transaction": tx.get("transaction"),
                        "meta": tx.get("meta"),
                        "blockTime": block_time,
                    }
                    _, protocols = await self._handle_raw_tx(raw_tx, session)
                    self._txs_processed += 1
                    for p in protocols:
                        if p == "pump_fun":
                            self._txs_pump_fun += 1
                        elif p == "meteora":
                            self._txs_meteora += 1
                t_proc_end = time.monotonic()

                process_ms = (t_proc_end - t_proc_start) * 1000.0

                self._total_getblock_ms += getblock_ms
                self._total_process_ms += process_ms
                self._timed_blocks += 1

                self._slots_processed += 1
                self._max_processed_slot = max(self._max_processed_slot, slot)
                slot_queue.task_done()

        logger.info("[SLOT-WORKER %s] завершён", name)

    async def run_single_block(self, slot: int, verbose: bool = True) -> dict:
        """
        Тест парсинга одного блока: getBlock + прогон всех транзакций через парсеры.
        Возвращает статистику (slots=1, transactions, pump_fun, meteora).
        """
        self._reset_slot_stats()
        async with aiohttp.ClientSession() as session:
            block = await self._fetch_block(session, slot)
            if not block:
                if verbose:
                    logger.info("[TEST] Блок %s не получен (getBlock вернул пусто)", slot)
                return self.get_slot_stats()

            txs = block.get("transactions") or []
            block_time = block.get("blockTime")
            if verbose:
                logger.info(
                    "[TEST] Блок %s, blockTime=%s, транзакций: %s",
                    slot,
                    block_time,
                    len(txs),
                )

            for tx in txs:
                if not isinstance(tx, dict):
                    continue
                raw_tx = {
                    "slot": block.get("slot") or slot,
                    "transaction": tx.get("transaction"),
                    "meta": tx.get("meta"),
                    "blockTime": block_time,
                }
                _, protocols = await self._handle_raw_tx(raw_tx, session)
                self._txs_processed += 1
                for p in protocols:
                    if p == "pump_fun":
                        self._txs_pump_fun += 1
                    elif p == "meteora":
                        self._txs_meteora += 1

            self._slots_processed = 1

        stats = self.get_slot_stats()
        if verbose:
            print(
                f"[TEST] Итого: транзакций {stats['transactions']}, "
                f"pump_fun: {stats['pump_fun']}, meteora: {stats['meteora']}"
            )
        return stats

    async def run_slot_workers(
        self,
        start_slot: int,
        end_slot: Optional[int] = None,
        workers: int = 8,
    ) -> None:
        """
        Альтернативный режим: асинхронные воркеры, которые ходят по слотам и
        забирают транзакции через getBlock.

        - start_slot: с какого слота начать;
        - end_slot: до какого слота идти (включительно). Если None — идём "вперёд"
          до текущего, можно вызывать периодически;
        - workers: количество параллельных воркеров.

        Пример использования:
            runner = BotRunner()
            await runner.run_slot_workers(start_slot=399_000_000, workers=4)
        """
        if end_slot is None:
            # Если end_slot не указан — ориентируемся на небольшой диапазон вперёд,
            # чтобы не сканить бесконечно в одной сессии.
            end_slot = start_slot + 10_000

        await self._ensure_db()
        self._reset_slot_stats()
        slot_queue: "asyncio.Queue[int]" = asyncio.Queue()
        for slot in range(start_slot, end_slot + 1):
            await slot_queue.put(slot)

        # Маркеры завершения для воркеров
        for _ in range(workers):
            await slot_queue.put(None)  # type: ignore[arg-type]

        tasks = [
            asyncio.create_task(self._slot_worker(f"{i}", slot_queue))
            for i in range(workers)
        ]

        try:
            await slot_queue.join()
            for t in tasks:
                await t
        finally:
            if self.db is not None and self._use_db:
                await self.db.close()

        stats = self.get_slot_stats()
        logger.info(
            "[SLOT-WORKERS] Диапазон слотов %s..%s обработан | слотов: %s, транзакций: %s "
            "(pump_fun: %s, meteora: %s)",
            start_slot,
            end_slot,
            stats["slots"],
            stats["transactions"],
            stats["pump_fun"],
            stats["meteora"],
        )

    async def _slot_producer_follow(
        self,
        slot_queue: "asyncio.Queue[int]",
        slots_back: int,
    ) -> None:
        """
        Продюсер слотов для режима "5 минут назад и дальше в реалтайме".

        - Берёт текущий слот через getSlot;
        - стартует с current_slot - slots_back;
        - далее раз в ~1 секунду обновляет current_slot и кладёт в очередь
          новые слоты по порядку.
        """
        async with aiohttp.ClientSession() as session:
            current_slot = await self._get_current_slot(session)
            if current_slot is None:
                logger.warning("[SLOT-PRODUCER] Не удалось получить текущий слот, завершаюсь")
                return

            # Пытаемся восстановиться с последнего сохранённого слота,
            # чтобы при рестарте не терять диапазон.
            saved_last = self._load_last_processed_slot()
            if saved_last is not None and saved_last < current_slot:
                next_slot = max(0, saved_last + 1)
                logger.info(
                    "[SLOT-PRODUCER] Восстанавливаюсь с сохранённого слота %s, текущий %s",
                    next_slot,
                    current_slot,
                )
            else:
                next_slot = max(0, current_slot - slots_back)
                logger.info("[SLOT-PRODUCER] Стартую с слота %s, текущий %s", next_slot, current_slot)

            while True:
                latest_slot = await self._get_current_slot(session)
                if latest_slot is None:
                    await asyncio.sleep(1.0)
                    continue

                while next_slot <= latest_slot:
                    await slot_queue.put(next_slot)
                    next_slot += 1

                await asyncio.sleep(1.0)

    async def run_slot_streaming(
        self,
        minutes_back: int = 5,
        workers: int = 6,
    ) -> None:
        """
        Режим, о котором ты написал:

        - стартуем "условно за N минут до текущего времени" (по слоту);
        - дальше непрерывно парсим в реалтайме слот за слотом.

        Приближение: считаем, что слот ≈ 0.5 секунды (2 слота в секунду),
        поэтому количество слотов назад:
            slots_back ≈ minutes_back * 60 * 2
        """
        # Грубая оценка: ~2 слота в секунду.
        slots_back = int(minutes_back * 60 * 2)

        await self._ensure_db()
        self._reset_slot_stats()
        slot_queue: "asyncio.Queue[int]" = asyncio.Queue()

        await self.sol_price_service.start_background()

        async def _print_stats_periodically(interval: float = 30.0) -> None:
            while True:
                await asyncio.sleep(interval)
                stats = self.get_slot_stats()
                self._save_last_processed_slot(stats["max_processed_slot"])
                 # Средние времена по getBlock и обработке блока, чтобы видеть узкое место.
                avg_getblock_ms = avg_process_ms = 0.0
                if self._timed_blocks > 0:
                    avg_getblock_ms = self._total_getblock_ms / self._timed_blocks
                    avg_process_ms = self._total_process_ms / self._timed_blocks
                lag_str = ""
                async with aiohttp.ClientSession() as session:
                    current_slot = await self._get_current_slot(session)
                    if current_slot is not None and stats["max_processed_slot"] > 0:
                        lag = current_slot - stats["max_processed_slot"]
                        lag_str = f", отставание от текущего блока: {lag} слотов"
                logger.info(
                    "[STATS] слотов: %s, транзакций: %s (pump_fun: %s, meteora: %s)%s, "
                    "avg_getBlock=%.1f ms, avg_process_block=%.1f ms",
                    stats["slots"],
                    stats["transactions"],
                    stats["pump_fun"],
                    stats["meteora"],
                    lag_str,
                    avg_getblock_ms,
                    avg_process_ms,
                )

                # Периодически сбрасываем batch‑буферы в БД, чтобы
                # свапы/пулы не висели только в памяти до shutdown.
                if self._use_db:
                    if self.pool_repo is not None:
                        await self.pool_repo.flush()
                    if self.swap_repo is not None:
                        await self.swap_repo.flush()

                # Периодически сохраняем последний обработанный слот,
                # чтобы при рестарте можно было продолжить без потери.
                

        stats_task = asyncio.create_task(_print_stats_periodically(30.0))

        # Запускаем воркеров, которые будут читать слоты из очереди и парсить блоки.
        workers_tasks = [
            asyncio.create_task(self._slot_worker(f"{i}", slot_queue))
            for i in range(workers)
        ]

        # Продюсер, который будет класть слоты, начиная с current_slot - slots_back
        # и дальше следить за появлением новых.
        producer_task = asyncio.create_task(self._slot_producer_follow(slot_queue, slots_back))

        # В этом режиме работаем до внешнего shutdown (Ctrl+C / остановка процесса),
        # поэтому просто ждём завершения продюсера и воркеров (по сути, бесконечно).
        try:
            await asyncio.gather(producer_task, *workers_tasks)
        finally:
            self.sol_price_service.stop()
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass

            # Финально сохраняем последний обработанный слот перед закрытием.
            self._save_last_processed_slot(self._max_processed_slot)

            for t in workers_tasks:
                t.cancel()
            if self.db is not None and self._use_db:
                # Перед закрытием соединения стараемся дописать накопленные события.
                if self.pool_repo is not None:
                    await self.pool_repo.flush()
                if self.swap_repo is not None:
                    await self.swap_repo.flush()
                await self.db.close()
                logger.info("[DB] Соединение с PostgreSQL закрыто")
            stats = self.get_slot_stats()
            lag_str = ""
            async with aiohttp.ClientSession() as session:
                current_slot = await self._get_current_slot(session)
                if current_slot is not None and stats["max_processed_slot"] > 0:
                    lag = current_slot - stats["max_processed_slot"]
                    lag_str = f", отставание: {lag} слотов"
            logger.info(
                "[STATS] итого: слотов %s, транзакций %s (pump_fun: %s, meteora: %s)%s",
                stats["slots"],
                stats["transactions"],
                stats["pump_fun"],
                stats["meteora"],
                lag_str,
            )