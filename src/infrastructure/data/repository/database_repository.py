"""Abstract definitions for database connection repositories.

This module contains base interfaces for classes that manage JDBC
initialization and connection retries.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple


class DatabaseRepository(ABC):
    """Contract for objects that provide database connectivity."""

    @abstractmethod
    def initialize_jdbc(
        self,
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        """Prepare and return JDBC URL and properties.

        Returns:
            Tuple[Optional[str], Optional[Dict[str, str]]]: jdbc_url and
            properties dict or ``(None, None)`` if unavailable.
        """
        pass

    @abstractmethod
    def connect_with_retry(
        self,
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        """Attempt to connect to the database, retrying on failure.

        The implementation may use exponential backoff.

        Returns:
            Tuple[Optional[str], Optional[Dict[str, str]]]: same as
            ``initialize_jdbc`` when successful.
        """
        pass
