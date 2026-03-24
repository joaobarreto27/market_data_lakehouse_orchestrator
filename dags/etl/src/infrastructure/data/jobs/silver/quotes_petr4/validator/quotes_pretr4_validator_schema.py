"""Schema validation for PETR4 stock quote events.

This module defines Pydantic validation schemas for market quote data
from the PETR4 stock ticker, including OHLC prices, volume, and related
market metrics.
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, PositiveFloat, PositiveInt


class QuotesPetr4ValidatorSchema(BaseModel):
    """Validation schema for PETR4 stock quote events.

    Ensures data quality and type correctness for market quote information
    including prices, volumes, ranges, and corporate metrics.
    """

    symbol: str
    shortName: str
    longName: str
    currency: str
    regularMarketPrice: PositiveFloat
    regularMarketDayHigh: PositiveFloat
    regularMarketDayLow: PositiveFloat
    regularMarketDayRange: str
    regularMarketChange: float
    regularMarketChangePercent: float
    regularMarketTime: datetime
    marketCap: Optional[PositiveFloat]
    regularMarketVolume: PositiveInt
    regularMarketPreviousClose: PositiveFloat
    regularMarketOpen: PositiveFloat
    fiftyTwoWeekRange: str
    fiftyTwoWeekLow: PositiveFloat
    fiftyTwoWeekHigh: PositiveFloat
    priceEarnings: Optional[PositiveFloat]
    earningsPerShare: Optional[PositiveFloat]
    logourl: str
