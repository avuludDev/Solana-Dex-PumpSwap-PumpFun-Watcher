from dataclasses import dataclass
from typing import List

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
    allowed_projects: List[str] | None = None
    blocked_tokens: List[str] | None = None


@dataclass
class Settings:
    solana: SolanaRPCConfig
    db: DatabaseConfig
    filters: FilterConfig
    pump_fun_program_id: str
    pumpswap_program_id: str
    meteora_program_id: str


def load_settings() -> Settings:
    # В реальном коде — из env/.env/конфига
    return Settings(
        solana=SolanaRPCConfig(
            ws_endpoint="wss://mainnet.helius-rpc.com/?api-key=",
            http_endpoint="https://mainnet.helius-rpc.com/?api-key="
        ),
        db=DatabaseConfig(
            dsn="postgres://user:password@localhost:5432/liquidity_watcher",
        ),
        filters=FilterConfig(
            min_swap_usd=150.0,
            min_sol_lamports=100_000_000,
            allowed_projects=None,
            blocked_tokens=None,
        ),
        pump_fun_program_id="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        pumpswap_program_id="pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
        meteora_program_id="cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
    )