"""Top-level data infrastructure package.

This package aggregates enums, repository modules, and utility classes
used across the infrastructure layer of the market data lakehouse
orchestrator. It exposes a simplified API for importing core components
such as database connectors, writers, and Spark session managers.
"""

from .connection import DatabaseEnum, SgbdEnum
from .http_base import HttpBaseEnum
from .jobs.bronze import BronzeEnum, QuotesEnum
from .repository_modules import (
    bronze_repository_modules,
    gold_repository_modules,
    silver_repository_modules,
)
from .utils import (
    ConnectAPI,
    ConnectionDatabase,
    DatabaseWriter,
    EnvManager,
    JsonWriter,
    ParquetWriter,
    SparkSessionManager,
)

__all__: list[str] = [
    "BronzeEnum",
    "bronze_repository_modules",
    "ConnectAPI",
    "ConnectionDatabase",
    "DatabaseEnum",
    "DatabaseWriter",
    "EnvManager",
    "HttpBaseEnum",
    "JsonWriter",
    "ParquetWriter",
    "SgbdEnum",
    "SparkSessionManager",
    "QuotesEnum",
    "silver_repository_modules",
    "gold_repository_modules",
]
