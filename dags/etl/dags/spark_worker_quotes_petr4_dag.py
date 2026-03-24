from datetime import timedelta  # noqa: D100

from airflow.decorators import dag, task  # type:ignore  # noqa: F401
from pendulum import datetime as pend_datetime  # type: ignore
from src.worker.quotes_pretr4 import (  # noqa: F401
    process_bronze,
    process_gold,
    process_silver,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="spark_market_quotes_petr4_dag",
    description="ETL for PETR4 stock quotes using Spark and Lakehouse architecture",
    schedule="0 20 * * 1-6",
    start_date=pend_datetime(2026, 3, 18, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=10,
    default_args=default_args,
    tags=["finance", "spark", "lakehouse"],
)
def petr4_lakehouse_pipeline():  # noqa: D103
    @task(task_id="extract_to_bronze")
    def task_bronze_layer():
        """Extracts raw data from API and saves to Bronze."""
        return process_bronze()

    @task(task_id="transform_to_silver")
    def task_silver_layer(data_raw_json: dict):
        """Validates and transforms data to Parquet in Silver."""
        return process_silver(data_json_raw=data_raw_json)

    @task(task_id="load_to_gold")
    def task_gold_layer(_data_silver_processed):
        """Aggregates and loads data into the analytics database."""
        return process_gold()

    raw_data = task_bronze_layer()
    silver_data = task_silver_layer(raw_data)
    task_gold_layer(silver_data)


# Instance the DAG
spark_market_quotes_petr4_dag = petr4_lakehouse_pipeline()
