"""Enumeration of source systems providing data."""

from enum import Enum


class SourceSystemEnum(Enum):
    """Identifiers for external data sources used by the pipeline."""

    brapi = 1
