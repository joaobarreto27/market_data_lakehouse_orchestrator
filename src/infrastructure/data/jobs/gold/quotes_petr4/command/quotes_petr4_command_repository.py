import logging  # noqa: D100

from .....utils import DatabaseWriter

logger = logging.getLogger(__name__)


class QuotesPetr4GoldCommandRepository:  # noqa: D101
    def writer_gold(self, spark_session, connection, df, table_name):  # noqa: D102
        try:
            logger.info(f"Starting Gold layer write to table: {table_name}")
            DatabaseWriter(
                spark=spark_session,
                connect=connection,
            ).write(df=df, table_name=table_name)
            logger.info(f"Gold data written to table {table_name}")
        except Exception as e:
            logger.exception(f"Failed to write Gold data to table {table_name}")
            raise e
