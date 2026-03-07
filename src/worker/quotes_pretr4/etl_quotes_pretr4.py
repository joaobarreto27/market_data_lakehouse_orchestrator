"""Worker responsible for executing daily ETL for PETR4 stock quotes."""

import logging
from datetime import date
from typing import Any

import pandas as pd

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def main() -> None:
    """Execute daily ETL pipeline for PETR4 stock quotes."""
    try:
        logger.info("Starting PETR4 daily ETL pipeline")

        table_name = BronzeEnum.quotes_petr4.name
        source_system = SourceSystemEnum.brapi.name

        logger.info(f"Table: {table_name}, Source System: {source_system}")

        # Bronze Layer: Fetch raw data from API
        logger.info("Querying API for daily closing quotes")
        data_json_raw: list[Any] = bronze_repository.QuotesPetr4BronzeQueryRepository(
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

        logger.info(f"Writing raw JSON data to: {path_file_bronze}")
        JsonWriter().write(data_json_raw, path_file_bronze)
        logger.info("Raw data successfully written to Bronze layer")

        # Silver Layer: Read, transform, and persist
        logger.info("Reading Bronze JSON file for transformation")
        df: pd.DataFrame = ReadJsonFile().read(path_file=path_file_bronze)
        logger.info(
            f"""DataFrame loaded with {len(df)} rows and {len(df.columns)} columns.
            Applying Silver layer transformations and writing to Parquet."""
        )

        path_file_silver = (
            f"silver/finance/{table_name}/"
            f"year={today.year}/"
            f"month={today.strftime('%m')}/"
            f"day={today.strftime('%d')}/"
            f"{table_name}.parquet"
        )

        logger.info(f"Writing transformed data to: {path_file_silver}")
        ParquetWriter().write(df, path_file_silver)  # type:ignore
        logger.info("Transformed data successfully written to Silver layer")

        logger.info("PETR4 daily ETL pipeline completed successfully")

    except Exception as e:
        logger.exception("PETR4 daily ETL pipeline failed during execution")
        raise e


if __name__ == "__main__":
    main()
