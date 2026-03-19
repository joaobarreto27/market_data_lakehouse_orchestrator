"""Validação para o schema de eventos de cotação de PETR4."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, PositiveFloat, PositiveInt


class QuotesPetr4ValidatorSchema(BaseModel):
    """Schema de validação para os eventos de cotação de PETR4."""

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
