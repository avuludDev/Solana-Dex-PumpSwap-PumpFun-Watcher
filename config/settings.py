import os
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class SolanaRPCConfig:
    ws_endpoint: str
    http_endpoint: str
    commitment: str = "confirmed"


@dataclass
class DatabaseConfig:
    dsn: str  # postgres://user:pass@host:port/db


@dataclass
class FilterConfig:
    min_swap_usd: float = 150.0
    # Минимальная сумма по SOL (в лампортах) для учёта свапа в протоколах,
    # где мы сначала фильтруем по SOL‑стороне (PumpSwap, Meteora).
    min_sol_lamports: int = 100_000_000  # 1 SOL
    allowed_projects: List[str] | None = None
    blocked_tokens: List[str] | None = None


@dataclass
class TelegramConfig:
    """Многоуровневая фильтрация уведомлений о токенах."""
    bot_token: str = ""
    chat_id: str = ""
    poll_interval_seconds: int = 300
    # Уровень 1: одна покупка от N $
    min_purchase_usd: float = 500.0
    # Уровень 2: один кошелёк купил токен минимум N раз
    min_purchases_count: int = 3
    # Уровень 3: минимум китов (кошельков с 3+ покупками) по токену
    min_whales_count: int = 1
    # Уровень 4: минимальный суммарный объём по токену (USD)
    min_total_volume_usd: float = 0.0
    # Фильтр по ликвидности: окно 1–3 дня, отправляем только если токен «набирает» (покупок больше, чем продаж)
    liquidity_window_days: int = 2
    # Минимальное соотношение: объём покупок / объём продаж (>= 1 = набирает ликвидность)
    min_buy_sell_ratio: float = 1.5
    # Макс. уведомлений о токенах за один цикл опроса (защита от потока при сбросе sent или слабых фильтрах)
    max_notifications_per_run: int = 10
    # Минимальный "возраст" сигнала (минут), чтобы не слать совсем свежие пампы
    min_signal_age_minutes: int = 30


@dataclass
class Settings:
    solana: SolanaRPCConfig
    db: DatabaseConfig
    filters: FilterConfig
    telegram: TelegramConfig
    # Публичный HTTPS‑URL этого сервиса, куда Helius будет слать вебхуки.
    # Например: "https://mydomain.com/helius-webhook"
    webhook_public_url: Optional[str]
    pump_fun_program_id: str
    pumpswap_program_id: str
    meteora_program_id: str


def _build_db_dsn() -> str:
    """
    DSN для PostgreSQL.
    Либо задаётся целиком через DB_DSN, либо собирается из DB_HOST, DB_PORT, DB_DATABASE, DB_USER, DB_PASSWORD.
    Локальный Supabase: контейнер db публикует порт 5433 на хосте — используй DB_PORT=5433 (или DB_DSN с :5433).
    """
    
    host = "host.docker.internal"
    port = 5433
    database = "postgres"
    user = "postgres"
    password = "ff060d2269f71d1eff91ca29f0a29d30"

    # asyncpg корректно понимает оба варианта: postgres:// и postgresql://
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def load_settings() -> Settings:
    # Сейчас конфиг берём из переменных окружения (.env можно подгружать снаружи)
    return Settings(
        solana=SolanaRPCConfig(
            ws_endpoint="wss://mainnet.helius-rpc.com/?api-key=8a4187cc-2024-4702-99f1-a9079e58d2af",
            http_endpoint="https://mainnet.helius-rpc.com/?api-key=8a4187cc-2024-4702-99f1-a9079e58d2af",
        ),
        db=DatabaseConfig(
            dsn=_build_db_dsn(),
        ),
        filters=FilterConfig(
            min_swap_usd=120.0,
            min_sol_lamports=100_000_000,
            allowed_projects=None,
            blocked_tokens=None,
        ),
        telegram=TelegramConfig(
            bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
            poll_interval_seconds=int(os.environ.get("TELEGRAM_POLL_INTERVAL", "300")),
            min_purchase_usd=float(os.environ.get("TELEGRAM_MIN_PURCHASE_USD", "500")),
            min_purchases_count=int(os.environ.get("TELEGRAM_MIN_PURCHASES_COUNT", "3")),
            min_whales_count=int(os.environ.get("TELEGRAM_MIN_WHALES_COUNT", "1")),
            min_total_volume_usd=float(os.environ.get("TELEGRAM_MIN_TOTAL_VOLUME_USD", "0")),
            liquidity_window_days=int(os.environ.get("TELEGRAM_LIQUIDITY_WINDOW_DAYS", "2")),
            min_buy_sell_ratio=float(os.environ.get("TELEGRAM_MIN_BUY_SELL_RATIO", "1")),
            max_notifications_per_run=int(os.environ.get("TELEGRAM_MAX_NOTIFICATIONS_PER_RUN", "10")),
            min_signal_age_minutes=int(os.environ.get("TELEGRAM_MIN_SIGNAL_AGE_MINUTES", "30")),
        ),
        # TODO: заменить на реальный публичный HTTPS‑адрес вашего сервера.
        webhook_public_url="https://solana-parser-webhook.mooo.com/helius-webhook",
        pump_fun_program_id="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        pumpswap_program_id="pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
        meteora_program_id="cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
    )