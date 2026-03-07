import json  # noqa: D100
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from ..repository import WriterRepository


class JsonWriter(WriterRepository):  # noqa: D101
    def __init__(self, spark: SparkSession = None):  # type: ignore  # noqa: D107
        self.spark = spark

    def write(self, data_json, path_file) -> None:  # noqa: D102
        if data_json is None or (
            isinstance(data_json, (list, dict)) and len(data_json) == 0
        ):
            raise ValueError("Data to write is empty or None")

        path_file = Path(path_file)
        temp_path = path_file.with_suffix(".tmp")

        try:
            path_file.parent.mkdir(parents=True, exist_ok=True)
            with open(temp_path, "w", encoding="utf-8") as file:
                json.dump(data_json, file, indent=4, ensure_ascii=False, default=str)

            temp_path.rename(path_file)

        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise e

    def write_to_s3(self, data_json: Any, s3_path: str) -> None:
        """Write JSON data to S3 using PySpark."""
        if self.spark is None:
            raise ValueError("SparkSession is required for S3 operations")

        if data_json is None or (
            isinstance(data_json, (list, dict)) and len(data_json) == 0
        ):
            raise ValueError("Data to write is empty or None")

        # Convert to DataFrame using PySpark directly
        if isinstance(data_json, dict):
            spark_df = self.spark.createDataFrame([data_json], schema=None)  # type: ignore
        elif isinstance(data_json, list):
            spark_df = self.spark.createDataFrame(data_json, schema=None)  # type: ignore
        else:
            raise ValueError("data_json must be dict or list")

        spark_df.write.json(s3_path, mode="overwrite")
