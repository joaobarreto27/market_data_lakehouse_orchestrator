"""Module for managing JDBC connections with PySpark to PostgreSQL."""

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import boto3
from dotenv import dotenv_values
from pyspark.sql import SparkSession

from ..repository import DatabaseRepository
from .secret_resolver import SecretResolver

logger = logging.getLogger(__name__)


class ConnectionDatabase(DatabaseRepository):
    """Manages JDBC connection with PostgreSQL via PySpark."""

    def __init__(  # noqa: D417
        self,
        environment: str,
        db_name: str,
        sgbd_name: str = "postgresql",
        connection_folder: str = "connection",
        aws_secret_name: Optional[str] = None,
        aws_region: str = "us-east-1",
        config_path: str = "secret_config.json",
    ) -> None:
        """Initialize connection parameters.

        Args:
            sgbd_name (str): Database management system name (postgresql or sqlite).
            environment (str): Environment type (e.g., dev, prod).
            db_name (str): Database name.
            connection_folder (str): Folder path for connection configs.
                Defaults to 'connection'.
            aws_secret_name (str, optional): The AWS Secrets Manager secret name.
            aws_region (str): AWS region. Defaults to 'us-east-1'.
        """
        self.sgbd_name = sgbd_name
        self.environment = environment
        self.db_name = db_name
        self.connection_folder = connection_folder
        self.aws_secret_name = aws_secret_name
        self.aws_region = aws_region

        self.current_dir: Optional[Path] = None
        self.path_file: Optional[Path] = None
        self.path: Optional[Path] = None
        self.jdbc_url: Optional[str] = None
        self.properties: Optional[Dict[str, str]] = None
        self.sqlite_conn: Optional[sqlite3.Connection] = None

        if aws_secret_name:
            self.aws_secret_name = aws_secret_name
        else:
            try:
                resolver = SecretResolver(config_path)
                self.aws_secret_name = resolver.resolve(sgbd_name, db_name)
                logger.info(f"Secret resolvida dinamicamente: {self.aws_secret_name}")
            except (FileNotFoundError, KeyError) as e:
                logger.warning(
                    f"Não foi possível resolver secret pelo JSON: {e}. "
                    "O sistema tentará usar .env local se necessário."
                )
                self.aws_secret_name = None

    def _get_aws_secret(self) -> dict:
        """Fetch and parse credentials from AWS Secrets Manager."""
        if not self.aws_secret_name:
            raise ValueError(
                "AWS Secret Name não definido. Verifique o JSON de config."
            )

        session = boto3.session.Session()  # type: ignore
        client = session.client(
            service_name="secretsmanager", region_name=self.aws_region
        )

        try:
            response = client.get_secret_value(SecretId=self.aws_secret_name)
            return json.loads(response["SecretString"])
        except Exception as e:
            logger.error(f"Erro ao buscar secret {self.aws_secret_name}: {e}")
            raise e

    def initialize_jdbc(self) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        """Create JDBC URL and properties for PySpark connection."""
        self.current_dir = Path(__file__).resolve().parent
        self.path = self.current_dir.parent.joinpath(
            self.connection_folder, self.sgbd_name
        )

        if self.sgbd_name == "postgresql":
            if self.aws_secret_name:
                credentials = self._get_aws_secret()
                host = credentials.get("host")
                port = credentials.get("port", "5432")
                database = credentials.get("database", self.db_name)
                user = credentials.get("username", "")
                password = credentials.get("password", "")
                driver = credentials.get("driver", "org.postgresql.Driver")

            else:
                self.path_file = self.path.joinpath(
                    f".env.{self.environment}_{self.db_name}"
                )

                if not self.path_file.is_file():
                    msg = f"Configuration file not found at: {self.path_file}"
                    logger.error(msg)
                    raise FileNotFoundError(msg)

                logger.info(f"Fetching credentials from local file: {self.path_file}")
                env_vars = dotenv_values(dotenv_path=self.path_file)

                host = env_vars.get("DB_HOST")
                port = env_vars.get("DB_PORT", "5432")
                database = self.db_name
                user = env_vars.get("DB_USER") or ""
                password = env_vars.get("DB_PASSWORD") or ""
                driver = "org.postgresql.Driver"

            if not host:
                raise ValueError(
                    "Database host could not be determined from AWS or .env"
                )

            self.jdbc_url = f"jdbc:{self.sgbd_name}://{host}:{port}/{database}"
            self.properties = {
                "user": user,
                "password": password,
                "driver": driver,
            }
            logger.info(f"JDBC properties generated successfully for {self.sgbd_name}.")
            return self.jdbc_url, self.properties

        elif self.sgbd_name == "sqlite":
            db_folder = self.path
            db_folder.mkdir(parents=True, exist_ok=True)
            db_path = db_folder / f"{self.db_name}.db"
            self.sqlite_conn = sqlite3.connect(db_path)
            logger.info(f"Connected to local SQLite: {db_path}")
            return None, None

        else:
            msg = f"DBMS '{self.sgbd_name}' is not supported."
            logger.error(msg)
            raise ValueError(msg)

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
        logger.info(
            f"Attempting connection to {self.sgbd_name} (Max retries: {max_retries})"
        )

        for attempt in range(1, max_retries + 1):
            try:
                if self.sgbd_name == "sqlite":
                    if self.sqlite_conn is None:
                        self.initialize_jdbc()
                    return None, None

                elif self.jdbc_url is None or self.properties is None:
                    self.initialize_jdbc()

                spark: SparkSession = SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
                assert self.jdbc_url is not None and self.properties is not None
                df = spark.read.jdbc(
                    url=self.jdbc_url,
                    table="(SELECT 1) AS test",
                    properties=self.properties,
                )
                df.collect()
                logger.info(
                    f"Successfully connected to {self.sgbd_name} on attempt {attempt}"
                )
                return self.jdbc_url, self.properties

            except Exception as e:
                if attempt == max_retries:
                    logger.exception(
                        f"""Failed to connect {e} to {self.sgbd_name}
                        after {max_retries} attempts"""
                    )
                    raise

                logger.warning(
                    f"Connection attempt {attempt} failed. Retrying in {wait_seconds}s"
                )
                time.sleep(wait_seconds)
        raise RuntimeError("Should not reach this point")
