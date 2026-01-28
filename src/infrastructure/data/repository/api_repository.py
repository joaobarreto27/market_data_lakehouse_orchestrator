from abc import ABC, abstractmethod  # noqa: D100
from typing import Any


class ApiRepository(ABC):  # noqa: D101
    @abstractmethod
    def connect(self) -> dict[str, Any]:  # noqa: D102
        pass
