"""Enumeration of data lakehouse storage buckets."""

from enum import Enum


class StorageEnum(Enum):
    """Storage bucket identifiers for different environments."""

    market_lakehouse_dev = "market-lakehouse-dev"
    market_lakehouse_prd = "market-lakehouse-prd"
