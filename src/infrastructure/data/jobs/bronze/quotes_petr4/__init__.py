# ruff: noqa: D104
from .command import QuotesPetr4BronzeCommandRepository
from .query import QuotesPetr4BronzeQueryRepository

__all__: list[str] = [
    "QuotesPetr4BronzeQueryRepository",
    "QuotesPetr4BronzeCommandRepository",
]
