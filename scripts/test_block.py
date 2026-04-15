#!/usr/bin/env python3
"""
Тест парсинга одного блока. Парсеры Pump.Fun и Meteora включаются явно.

Запуск из корня проекта:
    python -m scripts.test_block
    python -m scripts.test_block 399137649
"""
import asyncio
import sys

# Запуск из корня проекта (Solana Parser)
from config.settings import load_settings
from core.runner import BotRunner
from protocols.pump_fun import PumpFunParser
from protocols.meteora import MeteoraParser


async def main() -> None:
    slot = 399137649
    if len(sys.argv) > 1:
        try:
            slot = int(sys.argv[1])
        except ValueError:
            print(f"Использование: python -m scripts.test_block [SLOT]")
            sys.exit(1)

    settings = load_settings()
    runner = BotRunner()
    # В тесте включаем оба парсера (в main они могут быть выключены).
    runner.parsers = [
        PumpFunParser(settings.pump_fun_program_id),
        MeteoraParser(settings.meteora_program_id),
    ]

    print(f"Парсинг блока (слота) {slot}...")
    stats = await runner.run_single_block(slot, verbose=True)
    print(f"Результат: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
