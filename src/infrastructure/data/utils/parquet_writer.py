# noqa: D100
import shutil
from pathlib import Path

from pandas import DataFrame
from pyspark.sql import SparkSession

from ..repository import WriterRepository


class ParquetWriter(WriterRepository):  # noqa: D101
    def __init__(self, spark: SparkSession = None):  # type: ignore  # noqa: D107
        self.spark = spark

    def write(self, df: DataFrame, path_file: Path) -> None:  # noqa: D102
        if df.rdd.isEmpty():
            raise ValueError("Dataframe is empty")

        path_file = Path(path_file)
        temp_path = path_file.with_suffix(".tmp")

        try:
            path_file.parent.mkdir(parents=True, exist_ok=True)

            df.write.parquet(temp_path.as_posix(), mode="overwrite")

            if path_file.exists():
                shutil.rmtree(path_file)

            shutil.move(temp_path.as_posix(), path_file.as_posix())

        except Exception as e:
            if temp_path.exists():
                shutil.rmtree(temp_path)
            raise e

    def write_to_s3(self, df: DataFrame, s3_path: str) -> None:
        """Write DataFrame to S3 Parquet using PySpark."""
        if self.spark is None:
            raise ValueError("SparkSession is required for S3 operations")
        if df.empty:
            raise ValueError("Dataframe is empty")

        spark_df = self.spark.createDataFrame(df)
        spark_df.write.parquet(s3_path, mode="overwrite")
