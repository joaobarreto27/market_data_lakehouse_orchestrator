"""Gold layer job package offering analytics persistence components."""

from .quotes_petr4 import (
    QuotesPetr4GoldCommandRepository,
    QuotesPetr4GoldQueryRepository,
)

__all__: list[str] = [
    "QuotesPetr4GoldCommandRepository",
    "QuotesPetr4GoldQueryRepository",
]
