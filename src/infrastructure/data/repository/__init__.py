"""Repository interface package.

Defines base interfaces for API, database, and write operations used by the
infrastructure layer.
"""

from .api_repository import ApiRepository
from .database_repository import DatabaseRepository
from .writer_repository import WriterRepository

__all__: list[str] = ["ApiRepository", "DatabaseRepository", "WriterRepository"]
