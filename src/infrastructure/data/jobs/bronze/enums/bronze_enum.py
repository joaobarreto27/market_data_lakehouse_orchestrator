"""Enumerations representing data entities in the Bronze layer."""

from enum import Enum


class BronzeEnum(Enum):
    """Identifiers for different Bronze-layer quote datasets."""

    quotes_petr4 = 1
    quotes_vale3 = 2
    quotes_petr3 = 3
    quotes_usim5 = 4
    quotes_ggbr4 = 5
    quotes_usd = 6
