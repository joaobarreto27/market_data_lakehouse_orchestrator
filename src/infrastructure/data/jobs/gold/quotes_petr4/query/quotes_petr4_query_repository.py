from .....utils import PySparkDataReader  # noqa: D100, D104


class QuotesPetr4GoldQueryRepository:  # noqa: D101
    def read_silver_parquet(self, spark_session, path_file):  # noqa: D102
        df = PySparkDataReader(spark=spark_session).read_from_path_local(
            path_file=path_file
        )
        return df
