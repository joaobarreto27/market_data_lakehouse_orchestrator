"""Lazy loading utility for SQL query files with caching.

This module provides a helper that resolves a SQL file path based on
layer and file name, validates its existence, and reads the contents only
once when accessed.
"""

import logging
from functools import cached_property
from pathlib import Path

logger = logging.getLogger(__name__)


class SqlQueryLoader:
    """Load the contents of a .sql file lazily with caching.

    The path is constructed using the specified layer and file name. The
    file's existence is checked on first access, raising
    ``FileNotFoundError`` if it does not exist. Query text is read
    once and then cached.

    Example:
        loader = SqlQueryLoader("my_analysis", "silver")
    """

    def __init__(
        self,
        sql_file: str,
        layer: str,
        base_dir: Path | None = None,
    ) -> None:
        """Initialize the loader with SQL file metadata.

        Args:
            sql_file (str): name of the SQL file without ".sql" extension.
            layer (str): data layer name (e.g. "bronze", "silver", "gold").
            base_dir (Path | None): optional base directory overriding the
                default location, useful for testing or custom layouts.
        """
        self.sql_file: str = sql_file
        self.layer: str = layer
        self._base_dir = (
            base_dir if base_dir is not None else Path(__file__).resolve().parent
        )

    @cached_property
    def path(self) -> Path:
        """Absolute path to the SQL file.

        The existence of the file is validated the first time this property
        is accessed, and a ``FileNotFoundError`` is raised if not found.
        """
        path = self._base_dir.joinpath(
            "jobs", self.layer, self.sql_file, "sql", "query", f"{self.sql_file}.sql"
        )

        if not path.is_file():
            msg = f"SQL file not found: {path}"
            logger.error(msg)
            raise FileNotFoundError(msg)

        return path

    @cached_property
    def query(self) -> str:
        """Full contents of the SQL file (read once and cached).

        The file is opened using UTF-8 encoding.
        """
        return self.path.read_text(encoding="utf-8")
