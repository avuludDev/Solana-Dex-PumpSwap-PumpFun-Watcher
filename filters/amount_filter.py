from filters.base import BaseFilter
from core.types import SwapEvent, PoolCreatedEvent


class MinAmountUsdFilter(BaseFilter):
    """
    Фильтр по сумме свапа в USD (event.amount_usd).
    Пропускает только свапы с amount_usd >= min_usd.
    """

    def __init__(self, min_usd: float) -> None:
        self.min_usd = min_usd

    def filter_pool(self, event: PoolCreatedEvent) -> bool:
        return True

    def filter_swap(self, event: SwapEvent) -> bool:
        if event.amount_usd is None:
            return True  # цену ещё не подставили — не отбрасываем
        return event.amount_usd >= self.min_usd


class CompositeFilter(BaseFilter):
    """
    Композиция произвольного набора фильтров.
    Событие проходит только если прошло все фильтры.
    """

    def __init__(self, filters: list[BaseFilter]) -> None:
        self._filters = filters

    def filter_pool(self, event: PoolCreatedEvent) -> bool:
        return all(f.filter_pool(event) for f in self._filters)

    def filter_swap(self, event: SwapEvent) -> bool:
        return all(f.filter_swap(event) for f in self._filters)