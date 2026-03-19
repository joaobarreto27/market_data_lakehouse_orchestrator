"""Module for writing data to JSON format.

Provides utilities for persisting data structures as JSON files,
with support for both local file systems and S3 cloud storage.
"""

import json
import logging
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from ..repository import WriterRepository

logger = logging.getLogger(__name__)


class JsonWriter(WriterRepository):
    """Writer for persisting data structures to JSON format.

    Supports writing dictionaries and lists to local files and S3 cloud storage
    with automatic parent directory creation and atomic file operations.
    """

    def __init__(self, spark: SparkSession = None) -> None:  # type: ignore
        """Initialize JsonWriter with optional SparkSession.

        Args:
            spark: PySpark SparkSession instance for S3 operations.
                  Required only for write_to_s3 operations.
        """
        self.spark = spark

    def write(self, data_json: Any, path_file: str | Path) -> None:
        """Write data to local file system in JSON format.

        Performs atomic write operation using temporary file and rename to ensure
        data integrity. Creates parent directories if they don't exist.

        Args:
            data_json: Data structure (list or dict) to persist.
            path_file: Destination file path for JSON file.

        Raises:
            ValueError: If data is empty or None.
            IOError: If file write operation fails.
        """
        if isinstance(path_file, str) and path_file.startswith("s3://"):
            self.write_to_s3(data_json, path_file)
            return

        if isinstance(path_file, str):
            path_file = Path(path_file)

        if data_json is None or (
            isinstance(data_json, (list, dict)) and len(data_json) == 0
        ):
            msg = f"Cannot write empty data to {path_file}"
            logger.error(msg)
            raise ValueError(msg)

        path_file = Path(path_file)
        temp_path = path_file.with_suffix(".tmp")

        try:
            path_file.parent.mkdir(parents=True, exist_ok=True)
            with open(temp_path, "w", encoding="utf-8") as file:
                json.dump(data_json, file, indent=4, ensure_ascii=False, default=str)

            temp_path.rename(path_file)
            logger.info(f"JSON file written successfully to {path_file}")

        except IOError as e:
            if temp_path.exists():
                temp_path.unlink()
            logger.exception(f"Failed to write JSON file to {path_file}")
            raise IOError(f"Failed to write JSON file to {path_file}") from e
        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            logger.exception(f"Unexpected error while writing JSON to {path_file}")
            raise e

    def write_to_s3(self, data_json: Any, s3_path: str) -> None:
        """Write JSON data to S3 using PySpark."""
        if self.spark is None:
            msg = "SparkSession required for S3 operations"
            logger.error(msg)
            raise ValueError(msg)

        if data_json is None or (
            isinstance(data_json, (list, dict)) and len(data_json) == 0
        ):
            msg = f"Cannot write empty data to S3 {s3_path}"
            logger.error(msg)
            raise ValueError(msg)

        try:
            # Convert to DataFrame using PySpark directly
            if isinstance(data_json, dict):
                spark_df = self.spark.createDataFrame([data_json], schema=None)  # type: ignore
            elif isinstance(data_json, list):
                spark_df = self.spark.createDataFrame(data_json, schema=None)  # type: ignore
            else:
                msg = f"Invalid data type for S3 write: {type(data_json).__name__}"
                logger.error(msg)
                raise ValueError(msg)

            spark_df.write.json(s3_path, mode="overwrite")
            logger.info(f"JSON data written to S3: {s3_path}")
        except Exception as e:
            logger.exception(f"Failed to write to S3 {s3_path}")
            raise e
