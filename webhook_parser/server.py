"""
Простой HTTP‑сервер для приёма вебхуков Helius и прогонки их через текущий пайплайн.

Режим работы:
1. Запускаем сервер (python -m webhook_parser.server или отдельный entrypoint).
2. При старте он сам дергает Helius Webhooks API и:
   - находит уже существующий webhook с нашим URL, либо
   - создаёт новый webhook, если подходящего нет.
3. Helius шлёт POST на https://<твой-хост>/helius-webhook с enhanced/transactions payload.
4. Внутри мы приводим payload к формату getTransaction result и вызываем
   BotRunner._handle_raw_tx(raw_tx, session).
"""

import asyncio
import json
import logging
from datetime import datetime
import urllib.parse
from typing import List, Optional

import aiohttp
from aiohttp import web

from config.settings import load_settings
from core.runner import BotRunner
from infra.logging_config import setup_logging


logger = logging.getLogger(__name__)


def _extract_raw_txs(payload: dict | list) -> List[dict]:
    """
    Попробовать вытащить список "сырых транзакций" из вебхука Helius.

    Мы ориентируемся на то, что нужно BotRunner._handle_raw_tx:
      { "slot": ..., "transaction": {...}, "meta": {...}, "blockTime": ... }

    Поддерживаем несколько типичных форматов:
    - payload["result"] — одиночный объект getTransaction;
    - payload["data"]["transactions"] — список enhanced транзакций;
    - payload["data"]["transaction"] — одна enhanced транзакция.
    """
    # Helius иногда шлёт список событий, а не один объект.
    # В этом случае рекурсивно разворачиваем каждый элемент.
    if isinstance(payload, list):
        out: List[dict] = []
        for item in payload:
            if not isinstance(item, (dict, list)):
                continue
            out.extend(_extract_raw_txs(item))
        return out

    # Вариант 0: raw‑вебхук Helius (то, что мы сейчас используем).
    # Формат близок к getTransaction:
    #   { "slot": ..., "transaction": {...}, "meta": {...}, "blockTime": ... }
    if isinstance(payload, dict) and {"slot", "transaction", "meta"} <= payload.keys():
        return [
            {
                "slot": payload.get("slot"),
                "transaction": payload.get("transaction"),
                "meta": payload.get("meta"),
                "blockTime": payload.get("blockTime"),
            }
        ]

    # Вариант 1: обычный RPC‑ответ getTransaction.
    if "result" in payload and isinstance(payload["result"], dict):
        return [payload["result"]]

    data = payload.get("data") or {}

    # Вариант 2: список транзакций.
    txs = data.get("transactions")
    if isinstance(txs, list):
        out: List[dict] = []
        for tx in txs:
            if isinstance(tx, dict):
                # Многие вебхуки Helius уже отдают поля slot / transaction / meta
                # на верхнем уровне элемента списка.
                if {"slot", "transaction", "meta"} <= tx.keys():
                    out.append(
                        {
                            "slot": tx.get("slot"),
                            "transaction": tx.get("transaction"),
                            "meta": tx.get("meta"),
                            "blockTime": tx.get("blockTime"),
                        }
                    )
                else:
                    out.append(tx)
        return out

    # Вариант 3: одна транзакция.
    single_tx = data.get("transaction")
    if isinstance(single_tx, dict):
        if {"slot", "transaction", "meta"} <= data.keys():
            return [
                {
                    "slot": data.get("slot"),
                    "transaction": data.get("transaction"),
                    "meta": data.get("meta"),
                    "blockTime": data.get("blockTime"),
                }
            ]
        return [single_tx]

    return []


async def helius_webhook_handler(request: web.Request) -> web.Response:
    app = request.app
    runner: BotRunner = app["bot_runner"]
    session: aiohttp.ClientSession = app["http_session"]

    try:
        payload = await request.json()
    except Exception:
        return web.Response(status=400, text="invalid json")

    raw_txs = _extract_raw_txs(payload)
    if not raw_txs:
        # Ничего не распознали — но вебхук технически ок.
        return web.Response(text="no txs")

    # Логируем только транзакции, в которых создаются токены/пулы
    for raw_tx in raw_txs:
        if not isinstance(raw_tx, dict):
            continue
        had_pool, _ = await runner._handle_raw_tx(raw_tx, session)
        if had_pool:
            line = json.dumps(
                {"ts": datetime.utcnow().isoformat(), "raw_tx": raw_tx},
                ensure_ascii=False,
            )
            with open("helius_raw_txs.log", "a", encoding="utf-8") as f:
                f.write(line + "\n")

    return web.Response(text="ok")


async def _ensure_helius_webhook(
    session: aiohttp.ClientSession,
) -> None:
    """
    Автоматически создать (или обнаружить существующий) webhook в Helius.

    Используем тот же api-key, что и в RPC endpoint (settings.solana.http_endpoint),
    и регистрируем webhook на публичный URL из settings.webhook_public_url.
    """
    settings = load_settings()

    webhook_url: Optional[str] = settings.webhook_public_url
    if not webhook_url:
        logger.warning(
            "webhook_public_url не задан в config.settings.load_settings(); "
            "автоматическое создание вебхука пропущено."
        )
        return

    # Пытаемся вытащить api-key из RPC endpoint вида:
    #   https://mainnet.helius-rpc.com/?api-key=XXXX
    parsed = urllib.parse.urlparse(settings.solana.http_endpoint)
    query = urllib.parse.parse_qs(parsed.query)
    api_keys = query.get("api-key") or query.get("api_key") or []
    api_key = api_keys[0] if api_keys else None

    if not api_key:
        logger.warning(
            "Не удалось извлечь api-key из settings.solana.http_endpoint; "
            "автоматическое создание вебхука пропущено."
        )
        return

    logger.info(
        "[WEBHOOK] Инициализация вебхука в Helius... url=%s, api_key_prefix=%s",
        webhook_url,
        api_key[:6] + "..." if api_key else "<none>",
    )

    webhooks_base = f"https://api-mainnet.helius-rpc.com/v0/webhooks?api-key={api_key}"

    try:
        # 1. Получаем все вебхуки и проверяем, есть ли уже с таким URL.
        logger.info("[WEBHOOK] GET %s", webhooks_base)
        async with session.get(webhooks_base, timeout=15) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.error(
                    "[WEBHOOK] Не удалось получить список вебхуков: %s %s",
                    resp.status,
                    text,
                )
                return
            try:
                existing = await resp.json()
            except Exception:
                # Если json() не удался после text(), пробуем ещё раз.
                import json

                existing = json.loads(text)

        for wh in existing:
            if wh.get("webhookURL") == webhook_url:
                logger.info(
                    "[WEBHOOK] Найден существующий webhook id=%s url=%s",
                    wh.get("webhookID"),
                    webhook_url,
                )
                return

        # 2. Если подходящего вебхука нет — создаём новый.
        # Мониторим по programId интересующие нас протоколы.
        account_addresses: List[str] = [
            settings.pump_fun_program_id,
            settings.meteora_program_id,
            settings.pumpswap_program_id,
        ]

        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": ["CREATE_POOL"],
            "accountAddresses": account_addresses,
            # Переходим на raw‑вебхуки, чтобы payload был похож на getTransaction.
            "webhookType": "raw",
            "txnStatus": "all",
            "encoding": "jsonParsed",
        }

        logger.info("[WEBHOOK] POST %s payload=%s", webhooks_base, payload)
        async with session.post(webhooks_base, json=payload, timeout=15) as resp:
            text = await resp.text()

            # Helius при успешном создании возвращает 201, а не 200.
            if resp.status not in (200, 201):
                logger.error(
                    "[WEBHOOK] Ошибка при создании вебхука: %s %s",
                    resp.status,
                    text,
                )
                return

            try:
                created = await resp.json()
            except Exception:
                import json

                created = json.loads(text)

            logger.info(
                "[WEBHOOK] Создан новый webhook id=%s url=%s",
                created.get("webhookID"),
                created.get("webhookURL"),
            )
    except Exception as exc:
        logger.exception("[WEBHOOK] Ошибка при работе с Helius Webhooks API: %s", exc)


async def _init_bot_runner() -> BotRunner:
    """
    Инициализировать BotRunner без запуска WebSocket‑стримов.
    Просто подключаемся к БД (если есть) и готовим парсеры.
    """
    runner = BotRunner()
    try:
        if runner.db is not None:
            await runner.db.connect()
    except Exception as exc:
        logger.warning(
            "WebhookRunner: не удалось подключиться к БД, работаем без неё: %s",
            exc,
        )
        runner._use_db = False
    return runner


async def _create_app() -> web.Application:
    app = web.Application()

    # Общая HTTP‑сессия для RPC/DAS‑запросов.
    http_session = aiohttp.ClientSession()
    bot_runner = await _init_bot_runner()

    app["http_session"] = http_session
    app["bot_runner"] = bot_runner

    # При старте пытаемся автоматически создать/найти вебхук в Helius.
    # Делаем это синхронно, чтобы все логи были видны сразу.
    await _ensure_helius_webhook(http_session)

    app.router.add_post("/helius-webhook", helius_webhook_handler)

    async def on_cleanup(app: web.Application) -> None:
        await http_session.close()
        if bot_runner.db is not None and bot_runner._use_db:
            await bot_runner.db.close()

    app.on_cleanup.append(on_cleanup)
    return app


def main() -> None:
    """
    Точка входа для запуска сервера:

        python -m webhook_parser.server
    """
    setup_logging(service_name="webhook_server")
    web.run_app(asyncio.run(_create_app()), port=8080)


if __name__ == "__main__":
    main()
