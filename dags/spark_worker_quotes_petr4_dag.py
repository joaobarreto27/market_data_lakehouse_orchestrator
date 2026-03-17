from airflow.decorators import dag, task  # type:ignore  # noqa: D100, F401

from src.worker.quotes_pretr4 import (  # noqa: F401
    process_bronze,
    process_gold,
    process_silver,
)
