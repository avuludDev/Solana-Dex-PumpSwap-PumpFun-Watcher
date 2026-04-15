from abc import ABC, abstractmethod
from core.types import PoolCreatedEvent, SwapEvent

class BaseFilter(ABC):
    """
    Базовый фильтр: возвращает True, если событие проходит фильтр.
    """

    @abstractmethod
    def filter_pool(self, event: PoolCreatedEvent) -> bool:
        return True  # по умолчанию пропускаем

    @abstractmethod
    def filter_swap(self, event: SwapEvent) -> bool:
        return True