from abc import ABC, abstractmethod
from typing import Iterable
from core.types import PoolCreatedEvent, SwapEvent

class ProtocolParser(ABC):
    """
    Интерфейс для парсинга сырых Solana-транзакций/логов конкретного протокола.
    """

    @property
    @abstractmethod
    def protocol_name(self) -> str:
        """Идентификатор протокола для статистики (например, 'pump_fun', 'meteora')."""
        ...

    @abstractmethod
    def supports(self, raw_tx: dict) -> bool:
        """Понимает, относится ли транзакция к данному протоколу (по program_id, логам и т.п.)."""
        raise NotImplementedError

    @abstractmethod
    def parse_pool_creations(self, raw_tx: dict) -> Iterable[PoolCreatedEvent]:
        raise NotImplementedError

    @abstractmethod
    def parse_swaps(self, raw_tx: dict) -> Iterable[SwapEvent]:
        raise NotImplementedError