"""Enumeration of supported SGBD (database engines)."""

from enum import Enum


class SgbdEnum(Enum):
    """Identifiers for SQL database management systems."""

    postgresql = 1
    mysql = 2
    sqlserver = 3
    sqlite = 4
