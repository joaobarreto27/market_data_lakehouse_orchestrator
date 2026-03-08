"""Utilities for reading datasets via PySpark from various sources.

Provides helper methods to read from JDBC, local parquet files, and S3
using a shared Spark session.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .connect_database import ConnectionDatabase
from .sql_query_loader import SqlQueryLoader

logger: logging.Logger = logging.getLogger(__name__)


class PySparkDataReader:
    """Reader helper that wraps common PySpark input operations."""

    def __init__(self, spark: SparkSession) -> None:
        """Create a new reader using the provided SparkSession.

        Args:
            spark (SparkSession): active Spark session to use for reads.
        """
        self.spark = spark

    def read_from_jdbc(
        self, query_loader: SqlQueryLoader, db_connection: ConnectionDatabase
    ) -> DataFrame:
        """Execute the SQL provided by a loader over JDBC.

        Args:
            query_loader (SqlQueryLoader): loader that supplies the SQL text.
            db_connection (ConnectionDatabase): connection manager for JDBC.

        Returns:
            DataFrame: result of the query.

        Raises:
            RuntimeError: if JDBC credentials are not provided.
        """
        query_text = query_loader.query

        jdbc_url, properties = db_connection.connect_with_retry()

        if not jdbc_url or not properties:
            logger.error("JDBC connection did not return valid credentials.")
            raise RuntimeError("invalid JDBC credentials")

        wrapped_query = f"({query_text}) AS custom_query"

        df = self.spark.read.jdbc(
            url=jdbc_url, table=wrapped_query, properties=properties
        )
        return df

    def read_from_path_local(self, path_file: Path) -> DataFrame:
        """Read a parquet file from a local filesystem path.

        Args:
            path_file (Path): local path to the parquet file.

        Returns:
            DataFrame: loaded dataset.
        """
        df = self.spark.read.parquet(path_file.as_posix())  # type: ignore
        return df

    def read_from_s3_parquet(self, s3_path: str) -> DataFrame:
        """Load a parquet dataset located on S3.

        Args:
            s3_path (str): S3 URI to the parquet data.

        Returns:
            DataFrame: loaded dataset.
        """
        df = self.spark.read.parquet(s3_path)
        return df
