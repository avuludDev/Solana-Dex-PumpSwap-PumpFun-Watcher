import asyncpg
from typing import Any, Iterable, Mapping

class Database:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(dsn=self._dsn)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def execute(self, sql: str, *args) -> str:
        assert self._pool
        async with self._pool.acquire() as conn:
            return await conn.execute(sql, *args)

    async def fetchrow(self, sql: str, *args) -> Mapping[str, Any] | None:
        assert self._pool
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(sql, *args)

    async def fetch(self, sql: str, *args) -> list[Mapping[str, Any]]:
        assert self._pool
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *args)
            return list(rows)

    async def executemany(self, sql: str, args_seq: Iterable[tuple[Any, ...]]) -> None:
        """
        Выполнить один и тот же SQL‑запрос для набора параметров (batch‑insert).
        """
        assert self._pool
        async with self._pool.acquire() as conn:
            await conn.executemany(sql, list(args_seq))