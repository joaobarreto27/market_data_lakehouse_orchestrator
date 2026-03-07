"""Worker responsável por executar o ETL diário de cotações de PETR4."""

from datetime import date
from typing import Any

from pandas import DataFrame

from infrastructure import (
    BronzeEnum,
    HttpBaseEnum,
    JsonWriter,
    ParquetWriter,
    QuotesEnum,
    SourceSystemEnum,
)
from infrastructure import (
    bronze_repository_modules as bronze_repository,
)
from infrastructure.data.utils import ReadJsonFile


def main() -> None:
    """Executa o ETL."""
    table_name = BronzeEnum.quotes_petr4.name
    source_system = SourceSystemEnum.brapi.name

    data_json_raw: list[Any] = bronze_repository.QuotesPetr4QueryRepository(
        base_url=HttpBaseEnum.api_endpoint.value
    ).get_daily_closing(quotes=QuotesEnum.PETR4.value)  # type:ignore

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
