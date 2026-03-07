# noqa: D100
import logging
import shutil
from pathlib import Path

from pandas import DataFrame
from pyspark.sql import SparkSession

from ..repository import WriterRepository

logger = logging.getLogger(__name__)


class ParquetWriter(WriterRepository):  # noqa: D101
    def __init__(self, spark: SparkSession = None):  # type: ignore  # noqa: D107
        self.spark = spark

    def write(self, df: DataFrame, path_file: Path) -> None:  # noqa: D102
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
