"""
Фоновое обновление цены SOL (USD). Раз в 5 минут опрос API.
Приоритет: Helius getAsset(WSOL) → CoinGecko.
"""
import asyncio
import logging
from typing import Optional

import aiohttp


logger = logging.getLogger(__name__)

# WSOL mint — по нему Helius возвращает цену SOL в token_info.price_info
WSOL_MINT = "So11111111111111111111111111111111111111112"
COINGECKO_SOL_URL = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"


class SolPriceService:
    """
    Держит последнюю известную цену SOL в USD.
    Если передан helius_http_endpoint (RPC с api-key), сначала запрашивает цену через Helius getAsset(WSOL).
    Иначе или при ошибке — fallback на CoinGecko.
    """

    def __init__(
        self,
        interval_seconds: float = 300.0,
        helius_http_endpoint: Optional[str] = None,
    ) -> None:
        self._price_usd: Optional[float] = None
        self._interval = interval_seconds
        self._helius_endpoint = helius_http_endpoint or ""
        self._task: Optional[asyncio.Task] = None

    def get_sol_price_usd(self) -> Optional[float]:
        """Текущая цена 1 SOL в USD (или None, если ещё не загружена)."""
        return self._price_usd

    async def _fetch_helius(self, session: aiohttp.ClientSession) -> bool:
        """Запросить цену SOL через Helius getAsset(WSOL). Вернуть True, если цену получили."""
        if not self._helius_endpoint:
            return False
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAsset",
            "params": {"id": WSOL_MINT},
        }
        try:
            async with session.post(
                self._helius_endpoint,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
        except Exception as exc:
            logger.debug("[SOL-PRICE] Helius getAsset: %s", exc)
            return False

        result = data.get("result") or {}
        token_info = result.get("token_info") or result.get("tokenInfo") or {}
        price_info = token_info.get("price_info") or token_info.get("priceInfo") or {}
        price = price_info.get("price_per_token") or price_info.get("pricePerToken")
        if isinstance(price, (int, float)) and price > 0:
            self._price_usd = float(price)
            logger.debug("[SOL-PRICE] Helius: %.4f USD", self._price_usd)
            return True
        return False

    async def _fetch_coingecko(self, session: aiohttp.ClientSession) -> None:
        try:
            async with session.get(COINGECKO_SOL_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning("[SOL-PRICE] CoinGecko HTTP %s", resp.status)
                    return
                data = await resp.json()
        except Exception as exc:
            logger.warning("[SOL-PRICE] CoinGecko: %s", exc)
            return

        sol = data.get("solana")
        if isinstance(sol, dict):
            usd = sol.get("usd")
            if isinstance(usd, (int, float)) and usd > 0:
                self._price_usd = float(usd)
                logger.debug("[SOL-PRICE] CoinGecko: %.4f USD", self._price_usd)
        else:
            logger.warning("[SOL-PRICE] CoinGecko неожиданный ответ")

    async def _fetch_once(self, session: aiohttp.ClientSession) -> None:
        if not await self._fetch_helius(session):
            await self._fetch_coingecko(session)

    async def _run_loop(self) -> None:
        """Раз в interval_seconds запрашивает цену SOL."""
        async with aiohttp.ClientSession() as session:
            while True:
                await self._fetch_once(session)
                await asyncio.sleep(self._interval)

    async def start_background(self) -> asyncio.Task:
        """Сразу запросить цену, затем запустить фоновый цикл обновления раз в 5 мин. Возвращает задачу."""
        if self._task is not None and not self._task.done():
            return self._task
        async with aiohttp.ClientSession() as session:
            await self._fetch_once(session)
        if self._price_usd is not None:
            logger.info("[SOL-PRICE] Начальная цена SOL: %.4f USD", self._price_usd)
        self._task = asyncio.create_task(self._run_loop())
        logger.info("[SOL-PRICE] Фоновый парсер цены SOL запущен (интервал %.0f с)", self._interval)
        return self._task

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None
