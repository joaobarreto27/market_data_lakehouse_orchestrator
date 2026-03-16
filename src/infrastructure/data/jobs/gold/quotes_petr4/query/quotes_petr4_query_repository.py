"""Module for reading PETR4 data from Silver layer for Gold layer processing.

Provides repository class for reading parquet files from Silver layer
and preparing data for analytics and reporting.
"""

from pyspark.sql import DataFrame

from .....utils import PySparkDataReader


class QuotesPetr4GoldQueryRepository:
    """Repository for reading PETR4 quotes from Silver layer.

    Handles reading and loading Silver layer data for transformation
    into Gold layer analytics and reporting datasets.
    """

    def read_silver_parquet(self, spark_session, path_file: str) -> DataFrame:
        """Read Silver layer parquet file into DataFrame.

        Args:
            spark_session: PySpark session for data reading.
            path_file: Str Path to the Silver layer parquet file.

        Returns:
            PySpark DataFrame with the loaded data.
        """
        df = PySparkDataReader(spark=spark_session).read_from_s3_parquet(
            s3_path=path_file
        )
        return df
