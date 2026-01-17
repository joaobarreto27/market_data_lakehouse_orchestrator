"""Module for managing JDBC connections with PySpark to PostgreSQL."""

import sqlite3
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from dotenv import dotenv_values
from pyspark.sql import SparkSession


class ConnectionDatabase:
    """Manages JDBC connection with PostgreSQL via PySpark."""

    def __init__(
        self,
        sgbd_name: str,
        environment: str,
        db_name: str,
        connection_folder: str = "databases_connection",
    ) -> None:
        """Initialize connection parameters.

        Args:
            sgbd_name (str): Database management system name (postgresql or sqlite).
            environment (str): Environment type (e.g., dev, prod).
            db_name (str): Database name.
            connection_folder (str): Folder path for connection configs.
                Defaults to 'databases_connection'.
        """
        self.sgbd_name = sgbd_name
        self.environment = environment
        self.db_name = db_name
        self.connection_folder = connection_folder
        self.current_dir: Optional[Path] = None
        self.path_file: Optional[Path] = None
        self.path: Optional[Path] = None
        self.jdbc_url: Optional[str] = None
        self.properties: Optional[Dict[str, str]] = None
        self.sqlite_conn: Optional[sqlite3.Connection] = None

    def initialize_jdbc(self) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        """Create JDBC URL and properties for PySpark connection.

        Returns:
            tuple[str | None, dict[str,str] | None]: JDBC URL and connection properties.
                Returns (None, None) for SQLite connections.

        Raises:
            FileNotFoundError: If configuration file not found.
            ValueError: If DBMS is not supported.
        """
        self.current_dir = Path(__file__).resolve().parent
        self.path = self.current_dir.parent.joinpath(
            self.connection_folder, self.sgbd_name
        )

        if self.sgbd_name == "postgresql":
            self.path_file = self.path.joinpath(
                f".env.{self.environment}_{self.db_name}"
            )

            if not self.path_file.is_file():
                raise FileNotFoundError(
                    f"Configuration file not found at: {self.path_file}"
                )

            env_vars = dotenv_values(dotenv_path=self.path_file)

            self.jdbc_url = f"jdbc:postgresql://{env_vars['DB_HOST']}:{env_vars['DB_PORT']}/{self.db_name}"
            user = env_vars.get("DB_USER") or ""
            password = env_vars.get("DB_PASSWORD") or ""
            self.properties = {
                "user": user,
                "password": password,
                "driver": "org.postgresql.Driver",
            }
            return self.jdbc_url, self.properties

        elif self.sgbd_name == "sqlite":
            db_folder = self.path
            db_folder.mkdir(parents=True, exist_ok=True)
            db_path = db_folder / f"{self.db_name}.db"
            self.sqlite_conn = sqlite3.connect(db_path)
            print(f"Connected to local SQLite: {db_path}")
            return None, None

        else:
            raise ValueError(f"DBMS '{self.sgbd_name}' is not supported.")

    def connect_with_retry(
        self, max_retries: int = 5, wait_seconds: int = 5
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        """Test JDBC connection with retry logic.

        Args:
            max_retries (int): Maximum number of retry attempts. Defaults to 5.
            wait_seconds (int): Seconds to wait between retries. Defaults to 5.

        Returns:
            tuple[str | None, dict[str,str] | None]: JDBC URL and connection properties.
                Returns (None, None) for SQLite connections.

        Raises:
            Exception: If connection fails after max retries.
        """
        for attempt in range(1, max_retries + 1):
            try:
                if self.sgbd_name == "sqlite":
                    if self.sqlite_conn is None:
                        self.initialize_jdbc()
                    return None, None

                elif self.jdbc_url is None or self.properties is None:
                    self.initialize_jdbc()

                # Create a temporary SparkSession to test the connection
                spark: SparkSession = SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
                assert self.jdbc_url is not None and self.properties is not None
                df = spark.read.jdbc(
                    url=self.jdbc_url,
                    table="(SELECT 1) AS test",
                    properties=self.properties,
                )
                df.collect()  # Force execution to verify connection
                print("Successfully connected!")
                return self.jdbc_url, self.properties

            except Exception as e:
                print(f"[Attempt {attempt}/{max_retries}] Connection failed: {e}")
                if attempt == max_retries:
                    raise
                time.sleep(wait_seconds)
        raise RuntimeError("Should not reach this point")
