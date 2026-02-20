"""Worker responsável por executar o ETL diário de cotações de PETR4."""

from datetime import date
from typing import Any

from pandas import DataFrame

from infrastructure import (
    BronzeEnum,
    ConnectionDatabase,
    DatabaseEnum,
    HttpBaseEnum,
    JsonWriter,
    ParquetWriter,
    QuotesEnum,
    SgbdEnum,
    SourceSystemEnum,
    SparkSessionManager,
)
from infrastructure import (
    bronze_repository_modules as bronze_repository,
)
from infrastructure.data.utils import ReadJsonFile


def main() -> None:
    """Executa o ETL."""
    spark_manager = SparkSessionManager(sgbd_name=SgbdEnum.postgresql.name)
    spark = getattr(spark_manager, "spark", None)

    table_name = BronzeEnum.quotes_petr4.name
    source_system = SourceSystemEnum.brapi.name

    connection = ConnectionDatabase(
        environment="prd",
        db_name=DatabaseEnum.market_data_lakehouse_orchestrator.name,
        sgbd_name=SgbdEnum.postgresql.name,
    ).connect_with_retry()

    data_json_raw: list[Any] = bronze_repository.QuotesPetr4QueryRepository(
        base_url=HttpBaseEnum.api_endpoint.value
    ).get_daily_closing(quotes=QuotesEnum.PETR4.value)

    today = date.today()
    path_file_bronze = (
        f"bronze/{source_system}/{table_name}/"
        f"year={today.year}/"
        f"month={today.strftime('%m')}/"
        f"day={today.strftime('%d')}/"
        f"{table_name}.json"
    )

    JsonWriter().write(data_json_raw, path_file_bronze)

    df: DataFrame = ReadJsonFile().read(path_file=path_file_bronze)

    path_file_silver = (
        f"silver/finance/{table_name}/"
        f"year={today.year}/"
        f"month={today.strftime('%m')}/"
        f"day={today.strftime('%d')}/"
        f"{table_name}.parquet"
    )

    ParquetWriter().write(df, path_file_silver)  # type:ignore


if __name__ == "__main__":
    main()
