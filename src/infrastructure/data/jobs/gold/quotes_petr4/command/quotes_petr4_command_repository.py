from .....utils import DatabaseWriter  # noqa: D100, D104


class QuotesPetr4GoldCommandRepository:  # noqa: D101
    def writer_gold(self, spark_session, connection, df, table_name):  # noqa: D102
        DatabaseWriter(
            spark=spark_session,
            connect=connection,
        ).write(df=df, table_name=table_name)
