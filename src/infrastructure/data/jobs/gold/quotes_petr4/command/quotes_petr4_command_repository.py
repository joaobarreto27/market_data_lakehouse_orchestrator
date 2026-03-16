"""Module for persisting PETR4 analytics data to Gold layer.

Provides repository class for writing transformed and aggregated data
to the Gold layer for analytics and business reporting.
"""

import logging

from .....utils import DatabaseWriter

logger = logging.getLogger(__name__)


class QuotesPetr4GoldCommandRepository:
    """Repository for persisting PETR4 quotes to Gold layer.

    Handles writing transformed and aggregated PETR4 data to database
    tables in the Gold layer for analytics and business intelligence.
    """

    def writer_gold(self, spark_session, connection, df, table_name: str) -> None:
        """Write transformed Gold layer data to analytics database.

        Persists the aggregated and transformed DataFrame to the Gold layer
        in the analytics database for reporting and business intelligence.

        Args:
            spark_session: PySpark session for data processing.
            connection: Database connection manager.
            df: PySpark DataFrame with Gold layer data.
            table_name: Target table name in the analytics database.

        Raises:
            ValueError: If database connection fails or table write fails.
        """
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
