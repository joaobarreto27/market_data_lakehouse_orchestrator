"""Stock symbol constants used across job modules."""

from enum import Enum


class QuotesEnum(Enum):
    """Enumerated tickers for supported market quotes."""

    PETR4 = "PETR4"
    VALE3 = "VALE3"
    PETR3 = "PETR3"
    USIM5 = "USIM5"
    GGBR4 = "GGBR4"
    USD = "USD"
