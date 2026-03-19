"""Abstract base class defining write contracts for repositories.

Subclasses implement specific persistence mechanisms.
"""

from abc import ABC, abstractmethod
from typing import Any


class WriterRepository(ABC):
    """Interface for repository objects capable of writing data."""

    @abstractmethod
    def write(self, *args: Any, **kwargs: Any) -> None:
        """Perform a write operation with arbitrary arguments.

        Concrete implementations should document expected parameters.
        """
        pass
