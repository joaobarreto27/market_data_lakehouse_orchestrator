"""Base interface for API repository implementations."""

from abc import ABC, abstractmethod
from typing import Any


class ApiRepository(ABC):
    """Defines the contract for repositories that provide API connectivity."""

    @abstractmethod
    def connect(self) -> dict[str, Any]:
        """Establish a connection and return configuration details.

        Returns:
            dict[str, Any]: connection information such as headers, tokens,
                or base URLs.
        """
        pass
