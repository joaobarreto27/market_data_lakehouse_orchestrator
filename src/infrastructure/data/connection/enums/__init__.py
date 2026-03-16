"""Enums defining supported database and SGBD types.

These are used throughout the infrastructure layer when
specifying connection parameters.
"""

from .database_enum import DatabaseEnum
from .sgdb_enum import SgbdEnum

__all__: list[str] = ["DatabaseEnum", "SgbdEnum"]
