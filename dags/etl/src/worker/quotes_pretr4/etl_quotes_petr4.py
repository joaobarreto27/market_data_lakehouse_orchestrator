"""Daily ETL orchestration pipeline for PETR4 stock quotes.

This module orchestrates the complete ETL (Extract, Transform, Load) process
for PETR4 stock price data across bronze, silver, and gold layers of the
data lake architecture. It manages data ingestion from external APIs,
transformation and validation, and loading into analytics databases.

The pipeline is designed to run on a daily schedule within Apache Airflow,
with comprehensive logging for monitoring and troubleshooting.
"""

import logging
import os
from typing import Any

from dotenv import load_dotenv
from infrastructure import (
    BronzeEnum,
    ConnectionDatabase,
    DatabaseEnum,
    HttpBaseEnum,
    LayerEnum,
    QuotesEnum,
    SgbdEnum,
    SourceSystemEnum,
    SparkSessionManager,
    StorageEnum,
)
from infrastructure import (
    bronze_repository_modules as bronze_repository,
)
from infrastructure import (
    gold_repository_modules as gold_repository,
)
from infrastructure import (
    silver_repository_modules as silver_repository,
)
from infrastructure.data.utils import LayerPathResolver

# Configure logging for Airflow compatibility
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: logging.Logger = logging.getLogger(__name__)


def _get_spark_instance(environment: str) -> SparkSessionManager:
    """Get the appropriate Spark session manager for the given environment.

    Selects between SQLite (development) and PostgreSQL (production) based on
    the environment parameter.

    Args:
        environment (str): Environment type ('dev' or 'prd').

    Returns:
        SparkSessionManager: Configured Spark session manager for the environment.
    """
    sgbd_name = (
        SgbdEnum.sqlite.name if environment == "dev" else SgbdEnum.postgresql.name
    )
    return SparkSessionManager(sgbd_name=sgbd_name)


def _get_environment() -> str:
    load_dotenv()
    return os.getenv("ENVIRONMENT", "dev")


def _get_storage() -> str:
    return os.getenv("STORAGE_BUCKET", StorageEnum.market_lakehouse_prd.value)


def process_bronze() -> Any:
    """Extract raw PETR4 stock data from external API and persist to Bronze layer.

    Fetches daily closing price data from the BRAPI API for PETR4 stock ticker
    and writes the raw JSON response to the Bronze layer with date-based
    partitioning for data lake organization.

    Args:
        environment: Target environment used to resolve the storage path
            (e.g., 'dev' or 'prd').
        storage: The storage name.

    Returns:
        dict: Raw JSON data structure from the API response containing stock quotes.

    Raises:
        ValueError: If API request fails or returns empty data.
        IOError: If file write operation to Bronze layer fails.
    """
    try:
        logger.info("[BRONZE_INIT] Starting Bronze layer data extraction for PETR4")
        table_name = BronzeEnum.quotes_petr4.name
        source_system = SourceSystemEnum.brapi.name
        environment: str = _get_environment()
        storage: str = _get_storage()
        spark: SparkSessionManager = _get_spark_instance(environment)

        logger.info(f"[BRONZE_QUERY] Querying API endpoint for {table_name} quotes")

        data_json_raw = bronze_repository.QuotesPetr4BronzeQueryRepository(
            base_url=HttpBaseEnum.api_endpoint.value,
            environment=environment,
            db_name=DatabaseEnum.market_data_lakehouse_orchestrator.name,
        ).get_daily_closing(quotes=QuotesEnum.PETR4.value)

        bronze_repository.QuotesPetr4BronzeCommandRepository(
            data_json=data_json_raw,
            path_file=LayerPathResolver(
                layer=LayerEnum.bronze.name, table=table_name, environment=environment
            ).resolver_layer(storage=storage, source_system=source_system),
        ).writer_bronze(spark)

        logger.info("[BRONZE_SUCCESS] Bronze layer extraction completed successfully")
        return data_json_raw

    except Exception as e:
        logger.exception("[BRONZE_ERROR] Bronze layer extraction failed")
        raise e


def process_silver(data_json_raw: Any) -> Any:
    """Transform and validate Bronze layer data, then persist to Silver layer.

    Reads raw Bronze layer data, applies schema validation and data transformations,
    and writes the cleaned and validated data to the Silver layer in Parquet format
    with optimized columnar storage.

    Args:
        data_json_raw: Raw JSON data dictionary from Bronze layer extraction.
        environment: Target environment used to resolve the storage path
            (e.g., 'dev' or 'prd').
        storage: The storage name.

    Returns:
        dict: Validated and transformed data structure ready for analytics.

    Raises:
        ValidationError: If data fails schema validation.
        IOError: If file write operation to Silver layer fails.
    """
    try:
        logger.info("[SILVER_INIT] Starting Silver layer transformation for PETR4")
        table_name = BronzeEnum.quotes_petr4.name
        environment: str = _get_environment()
        storage: str = _get_storage()

        logger.info("[SILVER_VALIDATE] Validating data schema")
        spark: SparkSessionManager = _get_spark_instance(environment)
        table_name = BronzeEnum.quotes_petr4.name

        data = silver_repository.QuotesPetr4SilverQueryRepository(
            data_json=data_json_raw,
            spark_session=spark,
        ).validate_schema()

        logger.info(
            f"""[SILVER_TRANSFORM] Creating DataFrame with
            {len(data) if isinstance(data, list) else 1} records"""
        )
        df = spark.createDataFrame(data)

        logger.info("[SILVER_WRITE] Persisting transformed data to Silver layer")
        silver_repository.QuotesPetr4SilverCommandRepository(
            path_file_silver=LayerPathResolver(
                layer=LayerEnum.silver.name, table=table_name, environment=environment
            ).resolver_layer(storage=storage, domain="finance"),
            df=df,
        ).write_silver(spark=spark)

        logger.info(
            "[SILVER_SUCCESS] Silver layer transformation completed successfully"
        )
        return data

    except Exception as e:
        logger.exception("[SILVER_ERROR] Silver layer transformation failed")
        raise e


def process_gold() -> None:
    """Load transformed Silver layer data to Gold layer for analytics.

    Reads validated Silver layer data, applies aggregations and business logic,
    and persists the final dataset to the Gold layer analytics database tables
    for reporting and business intelligence consumption.

    Args:
        storage: The storage name.
        environment: Target environment used to resolve the storage path
            (e.g., 'dev' or 'prd').

    Raises:
        ValueError: If database connection fails or write operation fails.
    """
    try:
        logger.info("[GOLD_INIT] Starting Gold layer preparation for PETR4")

        logger.info("[GOLD_READ] Reading Silver layer data for aggregation")
        environment = _get_environment()
        storage = _get_storage()
        spark = _get_spark_instance(environment)

        sgbd_name = (
            SgbdEnum.sqlite.name if environment == "dev" else SgbdEnum.postgresql.name
        )
        connection = ConnectionDatabase(
            environment=environment,
            db_name=DatabaseEnum.market_data_lakehouse_orchestrator.name,
            sgbd_name=sgbd_name,
        )
        connection.connect_with_retry()

        df = gold_repository.QuotesPetr4GoldQueryRepository().read_silver_parquet(
            spark_session=spark,
            path_file=LayerPathResolver(
                layer=LayerEnum.silver.name,
                table=BronzeEnum.quotes_petr4.name,
                environment=environment,
            ).resolver_layer(storage=storage, domain="finance"),
        )

        logger.info(
            "[GOLD_WRITE] Writing aggregated data to Gold layer analytics database"
        )
        gold_repository.QuotesPetr4GoldCommandRepository().writer_gold(
            spark_session=spark,
            connection=connection,
            df=df,
            table_name=BronzeEnum.quotes_petr4.name,
        )

        logger.info("[GOLD_SUCCESS] Gold layer preparation completed successfully")

    except Exception as e:
        logger.exception("[GOLD_ERROR] Gold layer preparation failed")
        raise e


if __name__ == "__main__":
    load_dotenv()

    logger.info("🚀 Manual Execution Detected")
    try:
        raw_data = process_bronze()
        process_silver(raw_data)
        process_gold()
        logger.info("✅ ETL Pipeline completed successfully!")
    except Exception as e:
        logger.error(f"❌ Error during manual execution: {e}")
