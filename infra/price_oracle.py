from typing import Optional


class PriceOracle:
    """
    Обёртка над источниками цен (Pyth, Switchboard, DEX‑ы и т.п.).

    Сейчас это только скелет, который можно расширять под реальные источники.
    """

    async def get_price_usd(self, mint: str) -> Optional[float]:
        """
        Вернуть цену 1 токена (mint) в USD.

        В реальной реализации здесь должен быть запрос к оракулу
        (Pyth, Switchboard и т.п.) или агрегатору (Jupiter и др.).
        """
        return None

    async def estimate_swap_usd(
        self,
        in_mint: str,
        out_mint: str,
        in_amount: int,
        out_amount: int,
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Оценить USD‑стоимость входящей и исходящей частей свапа.
        """
        in_price = await self.get_price_usd(in_mint)
        out_price = await self.get_price_usd(out_mint)

        in_usd = in_amount * in_price if in_price is not None else None
        out_usd = out_amount * out_price if out_price is not None else None

        return in_usd, out_usd
