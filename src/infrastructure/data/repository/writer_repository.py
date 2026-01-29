from abc import ABC, abstractmethod  # noqa: D100
from typing import Any


class WriterRepository(ABC):  # noqa: D101
    @abstractmethod
    def write(self, *args: Any, **kwargs: Any) -> None:  # noqa: D102
        pass
