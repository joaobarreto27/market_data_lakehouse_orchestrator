"""Module for managing data loading into databases using PySpark."""

import logging
from typing import Literal

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from ..repository import WriterRepository
from .connect_database import ConnectionDatabase

logger = logging.getLogger(__name__)


class DatabaseWriter(WriterRepository):
    """Manages data loading into databases with Spark."""

    def __init__(  # noqa: D417
        self,
        spark: SparkSession,
        connect: ConnectionDatabase,
    ) -> None:
        """Initialize the connection for loading data into the database.

        Args:
            spark (SparkSession): Active Spark session.
            connect (ConnectionDatabase): Database connection manager.
        """
        self.spark = spark
        self.connect = connect

    def write(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        """Save a PySpark DataFrame to the database.

        Args:
            df (DataFrame): PySpark DataFrame to save.
            table_name (str): Name of the table in the database.
            mode (str): Write mode: 'append', 'overwrite', 'ignore', 'error'.
                Defaults to 'append'.

        Raises:
            ValueError: If DBMS is not supported or JDBC URL not initialized.
        """
        try:
            if self.connect.sgbd_name == "sqlite":
                if self.connect.sqlite_conn is None:
                    self.connect.initialize_jdbc()

                df_pandas: pd.DataFrame = df.toPandas()

                PandasIfExists = Literal["fail", "replace", "append"]

                pandas_mode: PandasIfExists  # pyright: ignore[reportInvalidTypeForm]

                if mode == "overwrite":
                    pandas_mode = "replace"
                elif mode == "ignore":
                    pandas_mode = "fail"
                else:
                    pandas_mode = "append"

                df_pandas.to_sql(
                    name=table_name,
                    con=self.connect.sqlite_conn,
                    if_exists=pandas_mode,
                    index=False,
                )
                logger.info(f"DataFrame written to SQLite table: {table_name}")

            elif self.connect.sgbd_name == "postgresql":
                jdbc_url, properties = self.connect.initialize_jdbc()
                if jdbc_url is None:
                    msg = "PostgreSQL JDBC URL not initialized"
                    logger.error(msg)
                    raise ValueError(msg)
                df.write.jdbc(
                    url=jdbc_url, table=table_name, mode=mode, properties=properties
                )
                logger.info(f"DataFrame written to PostgreSQL table: {table_name}")

            else:
                msg = f"Unsupported DBMS: {self.connect.sgbd_name}"
                logger.error(msg)
                raise ValueError(msg)
        except (ValueError, AttributeError) as e:
            logger.exception(f"Failed to write to database table {table_name}")
            raise e
        except Exception as e:
            logger.exception(f"Unexpected error writing to table {table_name}")
            raise e
