from typing import Callable, List, TypeVar, Generic

T = TypeVar("T")

class EventBus(Generic[T]):
    def __init__(self) -> None:
        self._subscribers: List[Callable[[T], None]] = []

    def subscribe(self, handler: Callable[[T], None]) -> None:
        self._subscribers.append(handler)

    def publish(self, event: T) -> None:
        for handler in self._subscribers:
            handler(event)