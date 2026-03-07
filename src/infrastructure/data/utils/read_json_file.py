# noqa: D100
import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class ReadJsonFile:  # noqa: D101
    def __init__(self, spark: SparkSession = None) -> None:  # type: ignore # noqa: D107
        self.spark = spark

    def read(self, path_file) -> pd.DataFrame:  # noqa: D102
        path_file = Path(path_file)
        current_dir: Path = Path(__file__).resolve().parent.parent.parent.parent.parent
        path: Path = current_dir.joinpath(path_file)

        try:
            if not path.is_file():
                msg = f"JSON file not found at: {path_file}"
                logger.error(msg)
                raise FileNotFoundError(msg)
            df: pd.DataFrame = pd.read_json(path_file)
            logger.info(f"JSON file read successfully from {path_file}")
            return df
        except FileNotFoundError:
            raise
        except Exception as e:
            logger.exception(f"Failed to read JSON file from {path_file}")
            raise e

    def read_from_s3(self, s3_path: str) -> DataFrame:
        """Read JSON from S3 using PySpark."""
        if self.spark is None:
            msg = "SparkSession required for S3 operations"
            logger.error(msg)
            raise ValueError(msg)
        try:
            df = self.spark.read.json(s3_path)
            logger.info(f"JSON data read from S3: {s3_path}")
            return df
        except Exception as e:
            logger.exception(f"Failed to read JSON from S3 {s3_path}")
            raise e
