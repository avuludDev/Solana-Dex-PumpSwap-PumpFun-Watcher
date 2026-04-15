from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class PoolCreatedEvent:
    """
    Нормализованное событие создания пула на любом поддерживаемом протоколе.
    base_mint — адрес токена (mint).
    """

    protocol: str          # "pump_fun", "pumpswap", "meteora", ...
    pool_address: str
    base_mint: str         # адрес токена (mint)
    quote_mint: str
    tx_signature: str
    slot: int
    timestamp: datetime
    token_name: Optional[str] = None   # название токена из Metaplex Metadata
    token_symbol: Optional[str] = None # символ токена из Metaplex Metadata
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SwapEvent:
    """
    Нормализованное событие свапа на любом поддерживаемом протоколе.
    """

    protocol: str
    pool_address: str

    in_mint: str
    out_mint: str
    in_amount: int
    out_amount: int

    # Сумма свапа в USD (по SOL-стороне: sol_lamports/1e9 * sol_price_usd)
    amount_usd: Optional[float] = None

    trader: Optional[str] = None

    tx_signature: str = ""
    slot: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)