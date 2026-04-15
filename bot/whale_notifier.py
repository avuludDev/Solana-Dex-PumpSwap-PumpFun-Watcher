"""
Telegram-бот: два этапа фильтрации.
1) Токены, которые какой-то кошелёк купил 3+ раз по 500$+ (киты).
2) По отфильтрованным токенам: за 1–3 дня объём покупок >= объёма продаж (набирает ликвидность) — только тогда отправляем уведомление со статистикой.
"""
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import aiohttp

from config.settings import load_settings
from infra.db import Database

logger = logging.getLogger(__name__)

SENT_STATE_FILE = Path("data/telegram_whale_sent.json")
DIGEST_TS_FILE = Path("data/telegram_last_digest_ts.txt")
DIGEST_INTERVAL_SECONDS = 24 * 3600  # раз в сутки
TOP_N_DAILY = 10

DEXSCREENER_TOKEN = "https://dexscreener.com/solana/"
WSOL_MINT = "So11111111111111111111111111111111111111112"

EXCLUDED_MINTS = (
    WSOL_MINT,
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
)


def _whale_query(
    min_purchase_usd: float,
    min_purchases_count: int,
    min_whales_count: int,
    min_total_volume_usd: float,
) -> str:
    # Подзапрос: (кошелёк, токен) с учётом и покупок, и продаж. Китом считаем только того, кто купил 3+ раз по min_usd
    # и при этом объём покупок > объёма продаж (не «купил 3 раза и слил»).
    # token_mint = не-SOL сторона свапа; покупка = in_mint=token, продажа = in_mint=WSOL.
    wsol = WSOL_MINT
    return f"""
    SELECT
        sub.token_mint,
        sub.token_name,
        sub.token_symbol,
        SUM(sub.purchase_count) AS purchase_count,
        COUNT(*) AS whales_count,
        SUM(sub.buy_volume) AS total_volume,
        MAX(sub.last_purchase) AS last_purchase
    FROM (
        SELECT
            CASE WHEN s.in_mint = '{wsol}' THEN s.out_mint ELSE s.in_mint END AS token_mint,
            t.name AS token_name,
            t.symbol AS token_symbol,
            s.trader,
            COUNT(CASE WHEN s.in_mint != '{wsol}' THEN 1 END) AS purchase_count,
            SUM(CASE WHEN s.in_mint != '{wsol}' THEN s.amount_usd ELSE 0 END) AS buy_volume,
            MAX(s.timestamp) AS last_purchase
        FROM swaps s
        LEFT JOIN tokens t ON t.mint = (CASE WHEN s.in_mint = '{wsol}' THEN s.out_mint ELSE s.in_mint END)
        WHERE
            s.amount_usd >= $1
            AND s.in_mint != s.out_mint
            AND s.trader IS NOT NULL
            AND (
                (s.in_mint = '{wsol}' AND s.out_mint != ALL($3::text[]))
                OR (s.out_mint = '{wsol}' AND s.in_mint != ALL($3::text[]))
            )
        GROUP BY s.trader, (CASE WHEN s.in_mint = '{wsol}' THEN s.out_mint ELSE s.in_mint END), t.name, t.symbol
        HAVING
            COUNT(CASE WHEN s.in_mint != '{wsol}' THEN 1 END) >= $2
            AND SUM(CASE WHEN s.in_mint != '{wsol}' THEN s.amount_usd ELSE 0 END)
                > COALESCE(SUM(CASE WHEN s.in_mint = '{wsol}' THEN s.amount_usd ELSE 0 END), 0)
    ) sub
    GROUP BY sub.token_mint, sub.token_name, sub.token_symbol
    HAVING COUNT(*) >= $4 AND SUM(sub.buy_volume) >= $5
    ORDER BY total_volume DESC
    """


def _liquidity_stats_query() -> str:
    """За N дней: объём покупок (in_mint=token, out_mint=WSOL) и продаж (in_mint=WSOL, out_mint=token) по токену."""
    return """
    SELECT
        COALESCE(SUM(CASE WHEN in_mint = $1 AND out_mint = $2 THEN amount_usd ELSE 0 END), 0)::float AS buy_volume,
        COALESCE(SUM(CASE WHEN in_mint = $2 AND out_mint = $1 THEN amount_usd ELSE 0 END), 0)::float AS sell_volume,
        COUNT(CASE WHEN in_mint = $1 AND out_mint = $2 THEN 1 END)::int AS buy_count,
        COUNT(CASE WHEN in_mint = $2 AND out_mint = $1 THEN 1 END)::int AS sell_count
    FROM swaps
    WHERE
        (in_mint = $1 AND out_mint = $2) OR (in_mint = $2 AND out_mint = $1)
        AND amount_usd IS NOT NULL
        AND timestamp >= NOW() - ($3::text || ' days')::interval
    """


def _load_sent_mints() -> set[str]:
    """Уже отправленные токены (mint)."""
    if not SENT_STATE_FILE.exists():
        return set()
    try:
        data = json.loads(SENT_STATE_FILE.read_text(encoding="utf-8"))
        # Поддержка старого формата (keys = [[trader, mint]]) и нового (mints = [mint])
        mints = set(data.get("mints", []))
        for k in data.get("keys", []):
            if isinstance(k, (list, tuple)) and len(k) >= 2:
                mints.add(k[1])
            elif isinstance(k, str):
                mints.add(k)
        return mints
    except Exception as e:
        logger.warning("Не удалось прочитать %s: %s", SENT_STATE_FILE, e)
        return set()


def _save_sent_mints(mints: set[str]) -> None:
    SENT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {"mints": sorted(mints)}
    SENT_STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8")


def _read_last_digest_ts() -> float | None:
    """Время последней отправки дайджеста (unix timestamp)."""
    if not DIGEST_TS_FILE.exists():
        return None
    try:
        return float(DIGEST_TS_FILE.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _save_digest_ts(ts: float) -> None:
    DIGEST_TS_FILE.parent.mkdir(parents=True, exist_ok=True)
    DIGEST_TS_FILE.write_text(str(ts), encoding="utf-8")


def _should_send_daily_digest() -> bool:
    last = _read_last_digest_ts()
    if last is None:
        return True
    return (time.time() - last) >= DIGEST_INTERVAL_SECONDS


def _format_message(
    row: dict[str, Any],
    liquidity: dict[str, Any] | None = None,
    window_days: int = 0,
) -> str:
    name = (row.get("token_name") or "").strip() or "—"
    symbol = (row.get("token_symbol") or "").strip() or "—"
    mint = row.get("token_mint") or ""
    count = row.get("purchase_count") or 0
    whales = row.get("whales_count") or 0
    total = row.get("total_volume")
    total_str = f"${total:,.0f}" if total is not None else "—"
    last = row.get("last_purchase")
    last_ts = last.strftime("%d.%m %H:%M") if last and hasattr(last, "strftime") else ""

    title = f"{symbol}" if symbol != "—" else (name if name != "—" else mint[:8] + "…")
    link = f"{DEXSCREENER_TOKEN}{mint}"

    parts = [
        f"🟢 {title}",
        f"Китов: {whales} · Покупок: {count} · Объём: {total_str}",
    ]
    if last_ts:
        parts.append(f"Последняя: {last_ts}")
    if liquidity and window_days:
        buy_v = liquidity.get("buy_volume") or 0
        sell_v = liquidity.get("sell_volume") or 0
        buy_c = liquidity.get("buy_count") or 0
        sell_c = liquidity.get("sell_count") or 0
        net = buy_v - sell_v
        net_str = f"+${net:,.0f}" if net >= 0 else f"-${abs(net):,.0f}"
        parts.append("")
        parts.append(f"За {window_days} дн: покупки ${buy_v:,.0f} ({buy_c}), продажи ${sell_v:,.0f} ({sell_c}), нетто {net_str}")
    parts.append("")
    parts.append(f"<a href=\"{link}\">DexScreener</a>")
    return "\n".join(parts)


def _format_digest_message(rows: list[dict[str, Any]]) -> str:
    """Одно сообщение: топ-N токенов по китам (короткий список со ссылками)."""
    if not rows:
        return "📊 Топ по китам за сутки: нет данных."
    lines = ["📊 <b>Топ-10 по китам</b> (объём покупок, без WSOL/USDC/USDT)\n"]
    for i, row in enumerate(rows, 1):
        symbol = (row.get("token_symbol") or "").strip() or "—"
        name = (row.get("token_name") or "").strip()
        mint = row.get("token_mint") or ""
        whales = row.get("whales_count") or 0
        total = row.get("total_volume")
        total_str = f"${total:,.0f}" if total is not None else "—"
        title = symbol if symbol != "—" else (name or mint[:8] + "…")
        link = f"{DEXSCREENER_TOKEN}{mint}"
        lines.append(f"{i}. {title} — {total_str}, китов: {whales}")
        lines.append(f"   <a href=\"{link}\">DexScreener</a>")
    return "\n".join(lines)


async def send_telegram_message(
    bot_token: str, chat_id: str, text: str, session: aiohttp.ClientSession
) -> bool:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning("Telegram API error %s: %s", resp.status, body)
                return False
            return True
    except Exception as e:
        logger.warning("Ошибка отправки в Telegram: %s", e)
        return False


async def _get_liquidity_stats(
    db: Database, token_mint: str, window_days: int
) -> dict[str, Any] | None:
    """Объём покупок/продаж по токену за последние window_days дней."""
    if window_days < 1:
        return None
    sql = _liquidity_stats_query()
    # В SQL используем ($3::text || ' days')::interval, поэтому передаём строку.
    row = await db.fetchrow(sql, token_mint, WSOL_MINT, str(window_days))
    if not row:
        return None
    return dict(row)


def _passes_liquidity_filter(
    liquidity: dict[str, Any] | None, min_ratio: float
) -> bool:
    """Пропуск только если объём покупок >= min_ratio * объём продаж (набирает ликвидность)."""
    if not liquidity:
        return True  # нет данных — не режем
    buy = float(liquidity.get("buy_volume") or 0)
    sell = float(liquidity.get("sell_volume") or 0)
    if sell <= 0:
        return buy > 0
    return buy >= min_ratio * sell


async def run_once(db: Database, cfg: Any, session: aiohttp.ClientSession) -> None:
    sent = _load_sent_mints()
    sql = _whale_query(
        cfg.min_purchase_usd,
        cfg.min_purchases_count,
        getattr(cfg, "min_whales_count", 1),
        getattr(cfg, "min_total_volume_usd", 0.0),
    )
    rows = await db.fetch(
        sql,
        cfg.min_purchase_usd,
        cfg.min_purchases_count,
        list(EXCLUDED_MINTS),
        getattr(cfg, "min_whales_count", 1),
        getattr(cfg, "min_total_volume_usd", 0.0),
    )
    window_days = getattr(cfg, "liquidity_window_days", 0) or 0
    min_ratio = getattr(cfg, "min_buy_sell_ratio", 1.0) or 1.0
    # Максимальный "возраст" последней китовой покупки, после которого токен
    # считаем устаревшим и не шлём по нему новые уведомления.
    # По умолчанию 24 часа; можно переопределить TELEGRAM_MAX_SIGNAL_AGE_HOURS.
    max_age_hours = getattr(cfg, "max_signal_age_hours", 24) or 0
    # Минимальный возраст сигнала: слишком свежие пампы ждём хотя бы N минут.
    min_age_minutes = getattr(cfg, "min_signal_age_minutes", 30) or 0
    max_per_run = getattr(cfg, "max_notifications_per_run", 10) or 10

    new_count = 0
    for row in rows:
        mint = (row.get("token_mint") or "").strip()
        if not mint or mint in sent:
            continue

        last = row.get("last_purchase")
        if last is not None and hasattr(last, "timestamp"):
            age_seconds = time.time() - last.timestamp()

            # Если последняя китовая покупка была слишком свежей — ждём.
            if min_age_minutes > 0 and age_seconds < min_age_minutes * 60:
                logger.debug(
                    "[AGE] Токен %s слишком свежий: last=%s, age=%.1f мин (min=%s мин)",
                    mint[:8],
                    last,
                    age_seconds / 60,
                    min_age_minutes,
                )
                continue

            # Если последняя китовая покупка была давно, игнорируем токен, даже если
            # по нему до сих пор есть объём в свопах (часто это уже "пост-раг" шум).
            if max_age_hours > 0 and age_seconds > max_age_hours * 3600:
                logger.debug(
                    "[AGE] Токен %s не прошёл фильтр по максимальному возрасту: last=%s, age=%.1f ч (max=%s ч)",
                    mint[:8],
                    last,
                    age_seconds / 3600,
                    max_age_hours,
                )
                continue

        # Второй фильтр: за 1–3 дня токен должен набирать ликвидность (покупок >= продаж)
        if window_days >= 1:
            liquidity = await _get_liquidity_stats(db, mint, window_days)
            if not _passes_liquidity_filter(liquidity, min_ratio):
                logger.debug("[LIQUIDITY] Токен %s не прошёл фильтр (сливают)", mint[:8])
                continue
        else:
            liquidity = None

        text = _format_message(
            dict(row),
            liquidity=liquidity,
            window_days=window_days if window_days >= 1 else 0,
        )
        ok = await send_telegram_message(cfg.bot_token, cfg.chat_id, text, session)
        if ok:
            sent.add(mint)
            new_count += 1
            if new_count >= max_per_run:
                logger.warning(
                    "[RATE] Достигнут лимит %s уведомлений за цикл, остальные — в следующий раз",
                    max_per_run,
                )
                break
        await asyncio.sleep(0.3)
    if new_count:
        _save_sent_mints(sent)
        logger.info("Отправлено уведомлений о токенах: %s", new_count)


async def run_daily_digest(
    db: Database, cfg: Any, session: aiohttp.ClientSession
) -> None:
    """Раз в сутки: топ-10 токенов по китам (одно сообщение)."""
    window_days = getattr(cfg, "liquidity_window_days", 0) or 0
    min_ratio = getattr(cfg, "min_buy_sell_ratio", 1.0) or 1.0

    sql = _whale_query(
        cfg.min_purchase_usd,
        cfg.min_purchases_count,
        getattr(cfg, "min_whales_count", 1),
        getattr(cfg, "min_total_volume_usd", 0.0),
    )
    rows = await db.fetch(
        sql,
        cfg.min_purchase_usd,
        cfg.min_purchases_count,
        list(EXCLUDED_MINTS),
        getattr(cfg, "min_whales_count", 1),
        getattr(cfg, "min_total_volume_usd", 0.0),
    )
    # Дополнительный фильтр по ликвидности и "свежести" токена, чтобы не показывать
    # уже слитые / мёртвые токены несколько дней подряд в дайджесте.
    if rows:
        filtered_rows = []
        cutoff_ts = None
        if window_days >= 1:
            cutoff_ts = time.time() - window_days * 24 * 3600

        for row in rows:
            mint = (row.get("token_mint") or "").strip()
            if not mint:
                continue

            # Тот же фильтр ликвидности, что и для единичных уведомлений.
            if window_days >= 1:
                liquidity = await _get_liquidity_stats(db, mint, window_days)
                if not _passes_liquidity_filter(liquidity, min_ratio):
                    logger.debug(
                        "[DIGEST][LIQUIDITY] Токен %s не прошёл фильтр (сливают)",
                        mint[:8],
                    )
                    continue

                # Дополнительно отбрасываем токены, по которым давно не было покупок китов.
                last = row.get("last_purchase")
                if last is None or not hasattr(last, "timestamp"):
                    continue
                if cutoff_ts is not None and last.timestamp() < cutoff_ts:
                    # Последняя покупка была раньше окна по ликвидности — считаем токен устаревшим.
                    continue

            filtered_rows.append(row)

        rows = filtered_rows

    top = [dict(r) for r in rows[:TOP_N_DAILY]]
    if not top:
        logger.info("[DIGEST] Топ-10: нет подходящих токенов, пропускаем отправку")
        _save_digest_ts(time.time())
        return
    text = _format_digest_message(top)
    if await send_telegram_message(cfg.bot_token, cfg.chat_id, text, session):
        _save_digest_ts(time.time())
        logger.info("[DIGEST] Отправлен топ-%s по китам", len(top))
    await asyncio.sleep(0.5)


async def main_loop() -> None:
    settings = load_settings()
    cfg = settings.telegram
    if not cfg.bot_token or not cfg.chat_id:
        logger.error(
            "TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID должны быть заданы. Бот не запущен."
        )
        return

    db = Database(settings.db.dsn)
    await db.connect()
    logger.info(
        "Telegram whale notifier: опрос каждые %s с, фильтр китов (покупка>=%s$, покупок>=%s, китов>=%s, объём>=%s$), "
        "ликвидность за %s дн (покупки/продажи >= %s)",
        cfg.poll_interval_seconds,
        cfg.min_purchase_usd,
        cfg.min_purchases_count,
        getattr(cfg, "min_whales_count", 1),
        getattr(cfg, "min_total_volume_usd", 0.0),
        getattr(cfg, "liquidity_window_days", 0),
        getattr(cfg, "min_buy_sell_ratio", 1.0),
    )

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    if _should_send_daily_digest():
                        await run_daily_digest(db, cfg, session)
                    await run_once(db, cfg, session)
                except Exception as e:
                    logger.exception("Ошибка в цикле опроса: %s", e)
                await asyncio.sleep(cfg.poll_interval_seconds)
    finally:
        await db.close()
