import logging  # noqa: D100
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .connect_database import ConnectionDatabase
from .sql_query_loader import SqlQueryLoader

logger: logging.Logger = logging.getLogger(__name__)


class PySparkDataReader:
    """O 'Músico': Responsável por ler dados de diversas fontes usando PySpark."""

    def __init__(self, spark: SparkSession) -> None:  # noqa: D107
        self.spark = spark

    def read_from_jdbc(  # noqa: D102
        self, query_loader: SqlQueryLoader, db_connection: ConnectionDatabase
    ) -> DataFrame:
        query_text = query_loader.query

        jdbc_url, properties = db_connection.connect_with_retry()

        if not jdbc_url or not properties:
            logger.error("A conexão JDBC não retornou credenciais válidas.")
            raise

        wrapped_query = f"({query_text}) AS custom_query"

        df = self.spark.read.jdbc(
            url=jdbc_url, table=wrapped_query, properties=properties
        )
        return df

    def read_from_path_local(self, path_file: Path):  # noqa: D102
        df = self.spark.read.parquet(path_file.as_posix())  # type: ignore
        return df

    def read_from_s3_parquet(self, s3_path: str) -> DataFrame:  # noqa: D102
        df = self.spark.read.parquet(s3_path)
        return df
