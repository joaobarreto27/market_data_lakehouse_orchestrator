"""Database connection enums package.

Provides enumerations for database types and SGBDs used
throughout the infrastructure layer.
"""

from .enums import DatabaseEnum, SgbdEnum

__all__: list[str] = ["DatabaseEnum", "SgbdEnum"]
