import asyncio
import logging
from typing import Any, Iterable

from config.settings import load_settings
from infra.db import Database
from .whale_notifier import (
    _whale_query,
    EXCLUDED_MINTS,
    _get_liquidity_stats,
    _passes_liquidity_filter,
)


logger = logging.getLogger(__name__)


async def inspect_whales(limit: int = 50, only_mints: Iterable[str] | None = None) -> None:
    """
    Отладочный скрипт:
    1) Берёт параметры из settings.telegram (4 базовых фильтра).
    2) Строит "ослабленный" запрос по китам:
       - те же min_purchase_usd и min_purchases_count,
       - но min_whales_count = 1 и min_total_volume_usd = 0.
    3) В Python проверяет, какие токены проходят реальные пороги,
       а какие "почти проходят" и чем именно не дотягивают.
    """
    settings = load_settings()
    cfg = settings.telegram

    db = Database(settings.db.dsn)
    await db.connect()

    try:
        min_purchase_usd = cfg.min_purchase_usd
        min_purchases_count = cfg.min_purchases_count
        min_whales_count = getattr(cfg, "min_whales_count", 1)
        min_total_volume_usd = getattr(cfg, "min_total_volume_usd", 0.0)
        liquidity_window_days = getattr(cfg, "liquidity_window_days", 0) or 0
        min_buy_sell_ratio = getattr(cfg, "min_buy_sell_ratio", 1.0) or 1.0

        print("== Текущие пороги (из settings.telegram) ==")
        print(f"min_purchase_usd     = {min_purchase_usd}")
        print(f"min_purchases_count  = {min_purchases_count}")
        print(f"min_whales_count     = {min_whales_count}")
        print(f"min_total_volume_usd = {min_total_volume_usd}")
        print(f"liquidity_window_days = {liquidity_window_days}")
        print(f"min_buy_sell_ratio    = {min_buy_sell_ratio}")
        print()

        # Ослабленный запрос: смотрим максимум токенов, которые вообще попадают
        # под определение "китов" по сделкам, но без жёстких порогов по числу китов и объёму.
        sql_relaxed = _whale_query(
            min_purchase_usd=min_purchase_usd,
            min_purchases_count=min_purchases_count,
            min_whales_count=1,
            min_total_volume_usd=0.0,
        )

        rows = await db.fetch(
            sql_relaxed,
            min_purchase_usd,
            min_purchases_count,
            list(EXCLUDED_MINTS),
            1,          # ослабленный min_whales_count
            0.0,        # ослабленный min_total_volume_usd
        )

        if not rows:
            print("Нет ни одного токена, который проходит ослабленный китовый фильтр.")
            return

        # При необходимости фильтруем по конкретным mint'ам, которые нас интересуют
        only_mints_set = {m.strip() for m in only_mints} if only_mints else None
        if only_mints_set:
            rows = [r for r in rows if (r.get("token_mint") or "").strip() in only_mints_set]

        # Сортируем по объёму покупок по убыванию
        rows_sorted = sorted(rows, key=lambda r: float(r["total_volume"] or 0), reverse=True)

        print(f"Найдено токенов по ослабленному фильтру: {len(rows_sorted)}")
        print(f"Покажем топ-{limit} по объёму:\n")

        # Собираем краткую статистику
        passed_all: list[dict[str, Any]] = []
        almost: list[dict[str, Any]] = []

        for raw in rows_sorted[:limit]:
            row = dict(raw)
            mint = (row.get("token_mint") or "").strip()
            symbol = (row.get("token_symbol") or "").strip() or "—"
            name = (row.get("token_name") or "").strip()

            whales = int(row.get("whales_count") or 0)
            total_volume = float(row.get("total_volume") or 0.0)
            last_purchase = row.get("last_purchase")

            passes_whales = whales >= min_whales_count
            passes_volume = total_volume >= min_total_volume_usd
            passes_all_filters = passes_whales and passes_volume

            deficit_whales = max(0, min_whales_count - whales)
            deficit_volume = max(0.0, min_total_volume_usd - total_volume)

            # Дополнительно считаем фильтр ликвидности, как в боевом коде.
            if liquidity_window_days >= 1:
                liquidity = await _get_liquidity_stats(db, mint, liquidity_window_days)
                passes_liquidity = _passes_liquidity_filter(liquidity, min_buy_sell_ratio)
            else:
                liquidity = None
                passes_liquidity = True

            info = {
                "mint": mint,
                "symbol": symbol,
                "name": name,
                "whales": whales,
                "total_volume": total_volume,
                "passes_all": passes_all_filters,
                "passes_whales": passes_whales,
                "passes_volume": passes_volume,
                "deficit_whales": deficit_whales,
                "deficit_volume": deficit_volume,
                "last_purchase": last_purchase,
                "liquidity": liquidity,
                "passes_liquidity": passes_liquidity,
            }

            if passes_all_filters and passes_liquidity:
                passed_all.append(info)
            else:
                almost.append(info)

        # Печать токенов, которые уже проходят все пороги
        print("== Токены, которые УЖЕ проходят все 4 порога ==")
        if not passed_all:
            print("Нет токенов, которые удовлетворяют всем текущим порогам.")
        else:
            for i, info in enumerate(passed_all, 1):
                title = info["symbol"] if info["symbol"] != "—" else (info["name"] or info["mint"][:8] + "…")
                last_str = ""
                if info["last_purchase"] is not None and hasattr(info["last_purchase"], "strftime"):
                    last_str = info["last_purchase"].strftime("%Y-%m-%d %H:%M:%S")

                liq = info["liquidity"] or {}
                buy_v = float(liq.get("buy_volume") or 0)
                sell_v = float(liq.get("sell_volume") or 0)
                liq_str = f"buy=${buy_v:.0f}, sell=${sell_v:.0f}" if liq else "no_liq_data"
                print(
                    f"{i}. {title} ({info['mint']}) | китов={info['whales']} "
                    f"| объём=${info['total_volume']:.0f} | last={last_str} "
                    f"| ликвидность: {liq_str}"
                )

        print("\n== Токены, которые БЛИЗКИ к порогам (почти проходят) ==")
        if not almost:
            print("Нет токенов, которые близко подходят к порогам.")
        else:
            # Сортируем "почти" по тому, кто ближе всего к исполнению (по двум метрикам)
            almost_sorted = sorted(
                almost,
                key=lambda x: (
                    x["deficit_whales"],
                    x["deficit_volume"],
                    -x["total_volume"],
                ),
            )
            for i, info in enumerate(almost_sorted, 1):
                title = info["symbol"] if info["symbol"] != "—" else (info["name"] or info["mint"][:8] + "…")
                last_str = ""
                if info["last_purchase"] is not None and hasattr(info["last_purchase"], "strftime"):
                    last_str = info["last_purchase"].strftime("%Y-%m-%d %H:%M:%S")

                reason_parts = []
                if not info["passes_whales"]:
                    reason_parts.append(f"не хватает китов: -{info['deficit_whales']}")
                if not info["passes_volume"]:
                    reason_parts.append(f"не хватает объёма: -${info['deficit_volume']:.0f}")
                if not info["passes_liquidity"]:
                    liq = info["liquidity"] or {}
                    buy_v = float(liq.get("buy_volume") or 0)
                    sell_v = float(liq.get("sell_volume") or 0)
                    reason_parts.append(
                        f"ликвидность не проходит (buy=${buy_v:.0f}, sell=${sell_v:.0f})"
                    )
                reason = "; ".join(reason_parts) if reason_parts else "всё ок"

                print(
                    f"{i}. {title} ({info['mint']}) | китов={info['whales']} "
                    f"| объём=${info['total_volume']:.0f} | last={last_str} "
                    f"| причины: {reason}"
                )

    finally:
        await db.close()


def main() -> None:
    """
    Примеры:
      - показать топ-50 токенов по ослабленному фильтру:
          python -m bot.debug_whales

      - посмотреть конкретный mint (например, рагнутый токен O):
          python -m bot.debug_whales FDJt4qTbBthNjqV8PLcC52QhMwYkm8gdveZYVrGbpump
    """
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Если передали аргументы в командной строке — считаем их mint'ами для фильтра.
    mints = sys.argv[1:] or None
    asyncio.run(inspect_whales(only_mints=mints))


if __name__ == "__main__":
    main()

