"""Module for writing DataFrame data to Parquet format.

Provides utilities for persisting PySpark DataFrames as Parquet files,
with support for both local file systems and S3 cloud storage.
"""

import logging
import shutil
from pathlib import Path

from pandas import DataFrame
from pyspark.sql import SparkSession

from ..repository import WriterRepository

logger = logging.getLogger(__name__)


class ParquetWriter(WriterRepository):
    """Writer for persisting DataFrames to Parquet format.

    Supports writing to local file system and S3 cloud storage with
    atomic file operations for data integrity.
    """

    def __init__(self, spark: SparkSession = None) -> None:  # type: ignore
        """Initialize ParquetWriter with optional SparkSession.

        Args:
            spark: PySpark SparkSession instance for S3 operations.
                  Required only for write_to_s3 operations.
        """
        self.spark = spark

    def write(self, df: DataFrame, path_file: str | Path) -> None:
        """Write DataFrame to local file system in Parquet format.

        Performs atomic write operation using temporary file and rename to ensure
        data integrity. Creates parent directories if they don't exist.

        Args:
            df: PySpark DataFrame to persist.
            path_file: Destination file path for Parquet file.

        Raises:
            ValueError: If DataFrame is empty.
            IOError: If file write operation fails.
        """
        if isinstance(path_file, str) and path_file.startswith("s3://"):
            self.write_to_s3(df, path_file)
            return

        if isinstance(path_file, str):
            path_file = Path(path_file)

        if df.rdd.isEmpty():
            msg = f"Cannot write empty DataFrame to {path_file}"
            logger.error(msg)
            raise ValueError(msg)

        path_file = Path(path_file)
        temp_path = path_file.with_suffix(".tmp")

        try:
            path_file.parent.mkdir(parents=True, exist_ok=True)

            df.write.parquet(temp_path.as_posix(), mode="overwrite")

            if path_file.exists():
                shutil.rmtree(path_file)

            shutil.move(temp_path.as_posix(), path_file.as_posix())
            logger.info(f"Parquet file written successfully to {path_file}")

        except IOError as e:
            if temp_path.exists():
                shutil.rmtree(temp_path)
            logger.exception(f"Failed to write parquet file to {path_file}")
            raise IOError(f"Failed to write parquet file to {path_file}") from e
        except Exception as e:
            if temp_path.exists():
                shutil.rmtree(temp_path)
            logger.exception(f"Unexpected error while writing parquet to {path_file}")
            raise e

    def write_to_s3(self, df: DataFrame, s3_path: str) -> None:
        """Write DataFrame to S3 Parquet using PySpark."""
        if self.spark is None:
            msg = "SparkSession required for S3 operations"
            logger.error(msg)
            raise ValueError(msg)
        if df.rdd.isEmpty():
            msg = f"Cannot write empty DataFrame to S3 {s3_path}"
            logger.error(msg)
            raise ValueError(msg)

        try:
            spark_df = self.spark.createDataFrame(df)
            spark_df.write.parquet(s3_path, mode="overwrite")
            logger.info(f"Parquet file written to S3: {s3_path}")
        except Exception as e:
            logger.exception(f"Failed to write parquet to S3 {s3_path}")
            raise e
