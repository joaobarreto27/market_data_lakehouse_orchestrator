"""Enumeration of data lakehouse layers."""

from enum import Enum


class LayerEnum(Enum):
    """Layer identifiers used throughout the pipeline."""

    bronze = 1
    silver = 2
    gold = 3
