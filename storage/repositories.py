import json
import logging
from typing import Optional

from infra.db import Database
from core.types import SwapEvent, PoolCreatedEvent


logger = logging.getLogger(__name__)


class SwapRepository:
    def __init__(self, db: Database) -> None:
        self._db = db
        # Буфер для batch‑вставки свапов, чтобы не дёргать БД по одному ряду.
        self._buffer: list[SwapEvent] = []
        self._batch_size: int = 50

    async def save_swap(self, e: SwapEvent) -> None:
        """
        Добавить свап в буфер и при достижении порога отправить batch‑insert.
        """
        self._buffer.append(e)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def flush(self) -> None:
        """
        Сохранить все накопленные свапы одной батч‑операцией.
        """
        if not self._buffer:
            return

        events = self._buffer
        self._buffer = []

        WSOL = "So11111111111111111111111111111111111111112"

        args_seq = []
        for e in events:
            sol_lamports = (
                e.in_amount if e.in_mint == WSOL else (e.out_amount if e.out_mint == WSOL else None)
            )
            args_seq.append(
                (
                    e.protocol,
                    e.pool_address,
                    e.in_mint,
                    e.out_mint,
                    e.in_amount,
                    e.out_amount,
                    sol_lamports,
                    e.amount_usd,  # сумма свапа в USD (sol_lamports/1e9 * sol_price)
                    e.trader,
                    e.tx_signature,
                    e.slot,
                    e.timestamp,
                )
            )

        sql = """
            INSERT INTO swaps (
                protocol, pool_address, in_mint, out_mint,
                in_amount, out_amount,
                sol_lamports, amount_usd,
                trader, tx_signature, slot, timestamp
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
            )
        """

        try:
            await self._db.executemany(sql, args_seq)
        except Exception as exc:
            # Не даём потерять события: логируем и пробуем вставить по одному.
            logger.warning(
                "[DB] Batch insert swaps failed for %s events: %s. Falling back to single inserts.",
                len(events),
                exc,
            )
            for e in events:
                sol_lamports = (
                    e.in_amount if e.in_mint == WSOL else (e.out_amount if e.out_mint == WSOL else None)
                )
                try:
                    await self._db.execute(
                        sql,
                        e.protocol,
                        e.pool_address,
                        e.in_mint,
                        e.out_mint,
                        e.in_amount,
                        e.out_amount,
                        sol_lamports,
                        e.amount_usd,
                        e.trader,
                        e.tx_signature,
                        e.slot,
                        e.timestamp,
                    )
                except Exception as exc2:
                    logger.error(
                        "[DB] Failed to insert swap tx=%s slot=%s: %s",
                        e.tx_signature,
                        e.slot,
                        exc2,
                    )


class PoolRepository:
    def __init__(self, db: Database) -> None:
        self._db = db
        # Буфер для batch‑upsert пулов.
        self._buffer: list[PoolCreatedEvent] = []
        self._batch_size: int = 200

    async def save_pool(self, e: PoolCreatedEvent) -> None:
        """
        Добавить пул в буфер и при достижении порога отправить batch‑upsert.
        """
        self._buffer.append(e)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def flush(self) -> None:
        """
        Сохранить/обновить все накопленные пулы (upsert по protocol + base_mint).
        """
        if not self._buffer:
            return

        events = self._buffer
        self._buffer = []

        args_seq = []
        for e in events:
            args_seq.append(
                (
                    e.protocol,
                    e.pool_address,
                    e.base_mint,
                    e.quote_mint,
                    e.tx_signature,
                    e.slot,
                    e.timestamp,
                    e.token_name,
                    e.token_symbol,
                    json.dumps(dict(e.extra), ensure_ascii=False),
                )
            )

        sql = """
            INSERT INTO pools (
                protocol,
                pool_address,
                base_mint,
                quote_mint,
                tx_signature,
                slot,
                timestamp,
                token_name,
                token_symbol,
                extra
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
            )
            -- Ключ: (protocol, base_mint) — один активный пул на токен в рамках протокола.
            ON CONFLICT (protocol, base_mint) DO UPDATE
            SET
                base_mint    = EXCLUDED.base_mint,
                quote_mint   = EXCLUDED.quote_mint,
                tx_signature = EXCLUDED.tx_signature,
                slot         = EXCLUDED.slot,
                timestamp    = EXCLUDED.timestamp,
                token_name   = EXCLUDED.token_name,
                token_symbol = EXCLUDED.token_symbol,
                extra        = EXCLUDED.extra
        """

        try:
            await self._db.executemany(sql, args_seq)
        except Exception as exc:
            logger.warning(
                "[DB] Batch upsert pools failed for %s events: %s. Falling back to single upserts.",
                len(events),
                exc,
            )
            for e in events:
                try:
                    await self._db.execute(
                        sql,
                        e.protocol,
                        e.pool_address,
                        e.base_mint,
                        e.quote_mint,
                        e.tx_signature,
                        e.slot,
                        e.timestamp,
                        e.token_name,
                        e.token_symbol,
                        json.dumps(dict(e.extra), ensure_ascii=False),
                    )
                except Exception as exc2:
                    logger.error(
                        "[DB] Failed to upsert pool %s/%s (tx=%s slot=%s): %s",
                        e.protocol,
                        e.base_mint,
                        e.tx_signature,
                        e.slot,
                        exc2,
                    )


class TokenRepository:
    """
    Хранение основной информации о токенах (mint, name, symbol и пр.).
    Ключ — адрес mint.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

        # Таблица ожидается такого вида (создаётся миграцией в БД):
        # CREATE TABLE public.tokens (
        #   mint            text PRIMARY KEY,
        #   name            text,
        #   symbol          text,
        #   decimals        integer,
        #   total_supply    numeric, -- или bigint, в зависимости от миграции
        #   first_seen_slot bigint,
        #   first_seen_tx   text,
        #   updated_at      timestamptz DEFAULT now()
        # );

    async def upsert_token(
        self,
        mint: str,
        name: Optional[str],
        symbol: Optional[str],
        decimals: Optional[int],
        total_supply: Optional[float],
        slot: Optional[int],
        tx_signature: Optional[str],
    ) -> None:
        """
        Сохранить/обновить базовую информацию о токене.

        Если запись уже есть, обновляем только name/symbol/updated_at.
        Если нет — создаём с first_seen_*.
        """
        await self._db.execute(
            """
            INSERT INTO tokens (
                mint,
                name,
                symbol,
                decimals,
                total_supply,
                first_seen_slot,
                first_seen_tx,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, NOW()
            )
            ON CONFLICT (mint) DO UPDATE
            SET
                name       = COALESCE(EXCLUDED.name, tokens.name),
                symbol     = COALESCE(EXCLUDED.symbol, tokens.symbol),
                decimals   = COALESCE(EXCLUDED.decimals, tokens.decimals),
                total_supply = COALESCE(EXCLUDED.total_supply, tokens.total_supply),
                updated_at = NOW()
            """,
            mint,
            name,
            symbol,
            decimals,
            total_supply,
            slot,
            tx_signature,
        )