# noqa: D100
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession


class ReadJsonFile:  # noqa: D101
    def __init__(self, spark: SparkSession = None):  # type: ignore # noqa: D107
        self.spark = spark

    def read(self, path_file) -> pd.DataFrame:  # noqa: D102
        path_file = Path(path_file)
        current_dir: Path = Path(__file__).resolve().parent.parent.parent.parent.parent
        path: Path = current_dir.joinpath(path_file)

        try:
            if not path.is_file():
                raise FileNotFoundError(f"Configuration file not found at: {path_file}")
            df: pd.DataFrame = pd.read_json(path_file)
            return df
        except Exception as e:
            raise e

    def read_from_s3(self, s3_path: str) -> DataFrame:
        """Read JSON from S3 using PySpark."""
        if self.spark is None:
            raise ValueError("SparkSession is required for S3 operations")
        return self.spark.read.json(s3_path)
