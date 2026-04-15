#!/usr/bin/env python3
"""
Запуск Telegram-бота: уведомления о кошельках с 3+ покупками одного токена от 500$.
Настройки: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (обязательно), опционально
TELEGRAM_POLL_INTERVAL, TELEGRAM_MIN_PURCHASE_USD, TELEGRAM_MIN_PURCHASES_COUNT.
"""
import asyncio
import logging
import sys

from bot.whale_notifier import main_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)

if __name__ == "__main__":
    asyncio.run(main_loop())
