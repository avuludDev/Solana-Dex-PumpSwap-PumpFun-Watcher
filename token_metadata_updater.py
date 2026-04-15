import asyncio
import logging
from typing import Optional

import aiohttp

from config.settings import load_settings
from infra.db import Database
from infra.helius_metadata import fetch_token_name_symbol
from storage.repositories import TokenRepository


logger = logging.getLogger(__name__)


async def update_token_metadata(limit: int = 500) -> None:
    """
    Обходит токены без метаданных и подтягивает name/symbol/decimals через Helius DAS.

    Идея:
    - основной парсер только складывает mint/first_seen_* в таблицу tokens;
    - этот скрипт периодически дописывает метаданные для новых токенов.
    """
    settings = load_settings()
    db = Database(settings.db.dsn)
    await db.connect()
    repo = TokenRepository(db)

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                # Берём токены, у которых нет хотя бы одного из полей name/symbol/decimals.
                rows = await db.fetch(
                    """
                    SELECT mint
                    FROM tokens
                    WHERE name IS NULL
                       OR symbol IS NULL
                       OR decimals IS NULL
                    ORDER BY updated_at ASC
                    LIMIT $1
                    """,
                    limit,
                )

                if not rows:
                    logger.info("Все токены имеют метаданные, спим 60 секунд")
                    await asyncio.sleep(60.0)
                    continue

                logger.info("Найдено токенов без метаданных: %s", len(rows))
                for row in rows:
                    mint: str = row["mint"]
                    try:
                        name, symbol, decimals, total_supply = await fetch_token_name_symbol(
                            settings.solana.http_endpoint,
                            session,
                            mint,
                        )
                        await repo.upsert_token(
                            mint=mint,
                            name=name,
                            symbol=symbol,
                            decimals=decimals,
                            total_supply=total_supply,
                            slot=None,
                            tx_signature=None,
                        )
                        await asyncio.sleep(0.1)  # лёгкий троттлинг, чтобы не душить Helius
                    except Exception as exc:  # pragma: no cover
                        logger.warning("Не удалось обновить метаданные токена %s: %s", mint, exc)
                # Небольшая пауза между батчами даже если их много.
                await asyncio.sleep(5.0)
    finally:
        await db.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(update_token_metadata())


if __name__ == "__main__":
    main()

