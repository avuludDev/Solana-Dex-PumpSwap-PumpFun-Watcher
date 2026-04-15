import asyncio

from core.runner import BotRunner
from infra.logging_config import setup_logging


async def main():
    setup_logging(service_name="slot_indexer")
    runner = BotRunner()
    await runner.run_slot_streaming(minutes_back=5, workers=16)


if __name__ == "__main__":
    asyncio.run(main())