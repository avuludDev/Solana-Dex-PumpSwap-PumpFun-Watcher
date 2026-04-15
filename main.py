import asyncio

from core.runner import BotRunner
from infra.logging_config import setup_logging

def main() -> None:
    setup_logging(service_name="ws_stream")
    runner = BotRunner()
    asyncio.run(runner.run())

if __name__ == "__main__":
    main()