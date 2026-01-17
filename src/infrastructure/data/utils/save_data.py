"""Module for managing data loading into databases using PySpark."""

from typing import Literal

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from .connect_database import ConnectionDatabase


class DatabaseWriter:
    """Manages data loading into databases with Spark."""

    def __init__(self, spark: SparkSession, connect: ConnectionDatabase) -> None:
        """Initialize the connection for loading data into the database.

        Args:
            spark (SparkSession): Active Spark session.
            connect (ConnectionDatabase): Database connection manager.
        """
        self.spark = spark
        self.connect = connect

    def save_data(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        """Save a PySpark DataFrame to the database.

        Args:
            df (DataFrame): PySpark DataFrame to save.
            table_name (str): Name of the table in the database.
            mode (str): Write mode: 'append', 'overwrite', 'ignore', 'error'.
                Defaults to 'append'.

        Raises:
            ValueError: If DBMS is not supported or JDBC URL not initialized.
        """
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
            print(f"DataFrame successfully written to '{table_name}' table in SQLite.")

        elif self.connect.sgbd_name == "postgresql":
            jdbc_url, properties = self.connect.initialize_jdbc()
            if jdbc_url is None:
                raise ValueError("JDBC URL was not initialized.")
            df.write.jdbc(
                url=jdbc_url, table=table_name, mode=mode, properties=properties
            )
            print(
                f"DataFrame successfully written to '{table_name}' table in PostgreSQL."
            )

        else:
            raise ValueError(f"DBMS '{self.connect.sgbd_name}' is not supported.")
