from abc import ABC, abstractmethod  # noqa: D100
from typing import Dict, Optional, Tuple


class DatabaseRepository(ABC):  # noqa: D101
    @abstractmethod
    def initialize_jdbc(  # noqa: D102
        self,
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:  # noqa: D102
        pass

    @abstractmethod
    def connect_with_retry(  # noqa: D102
        self,
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        pass
