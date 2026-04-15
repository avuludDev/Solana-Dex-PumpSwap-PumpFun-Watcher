import logging
import logging.handlers
import os
from pathlib import Path


def setup_logging(service_name: str = "solana-parser") -> None:
    """
    Базовая настройка логирования:
    - лог в stdout (для docker logs);
    - лог в файл с ротацией.

    Настраивается через ENV:
    - LOG_LEVEL (default: INFO)
    - LOG_DIR (default: ./logs)
    - LOG_FILE (default: <service_name>.log)
    """

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_file_name = os.getenv("LOG_FILE", f"{service_name}.log")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / log_file_name

    # Общий формат
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Хендлер в stdout (для docker logs)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Хендлер в файл с ротацией (10 файлов по 10 МБ)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    # Сносим старые хендлеры, чтобы не накапливать их между запусками
    # и гарантированно иметь и консоль, и файл.
    for h in list(root.handlers):
        root.removeHandler(h)

    root.setLevel(getattr(logging, log_level, logging.INFO))
    root.addHandler(console_handler)
    root.addHandler(file_handler)

