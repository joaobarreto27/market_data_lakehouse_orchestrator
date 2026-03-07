"""Worker responsável por executar o ETL diário de cotações de PETR4."""

from typing import Any

from infrastructure import (
    BronzeEnum,
    ConnectionDatabase,
    DatabaseEnum,
    HttpBaseEnum,
    LayerEnum,
    QuotesEnum,
    SgbdEnum,
    SourceSystemEnum,
    SparkSessionManager,
)
from infrastructure import (
    bronze_repository_modules as bronze_repository,
)
from infrastructure import (
    gold_repository_modules as gold_repository,
)
from infrastructure import (
    silver_repository_modules as silver_repository,
)
from infrastructure.data.utils import LayerPathResolver


def process_bronze() -> Any:
    """Extrai dados da API e escreve na camada bronze.

    Returns:
        Any: Dados JSON brutos extraídos da API.
    """
    table_name = BronzeEnum.quotes_petr4.name
    source_system = SourceSystemEnum.brapi.name

    data_json_raw = bronze_repository.QuotesPetr4BronzeQueryRepository(
        base_url=HttpBaseEnum.api_endpoint.value
    ).get_daily_closing(quotes=QuotesEnum.PETR4.value)

    bronze_repository.QuotesPetr4BronzeCommandRepository(
        data_json=data_json_raw,
        path_file=LayerPathResolver(
            layer=LayerEnum.bronze.name, table=table_name
        ).resolver_layer(source_system=source_system),
    )

    return data_json_raw


def process_silver(data_json_raw: Any, spark: Any) -> Any:
    """Transforma e carrega dados na camada silver.

    Args:
        data_json_raw: Dados JSON brutos da camada bronze.
        spark: Sessão PySpark para processamento de dados.

    Returns:
        Any: Dados transformados da camada silver.
    """
    table_name = BronzeEnum.quotes_petr4.name

    data = silver_repository.QuotesPetr4SilverQueryRepository(
        data_json=data_json_raw,
        spark_session=spark,
    ).validate_schema()  # type: ignore

    df = spark.createDataFrame(data)

    silver_repository.QuotesPetr4SilverCommandRepository(
        path_file_silver=LayerPathResolver(
            layer=LayerEnum.silver.name, table=table_name
        ).resolver_layer(domain="finance"),
        df=df,
    ).write_silver()

    return data


def process_gold(spark: Any, connection) -> None:
    """Processa e carrega dados na camada gold.

    Args:
        spark: Sessão PySpark para processamento de dados.
        connection: Conexão com o banco de dados.

    Implementação futura para transformações analíticas.
    """
    df = gold_repository.QuotesPetr4GoldQueryRepository().read_silver_parquet(
        spark_session=spark,
        path_file=LayerPathResolver(
            layer=LayerEnum.silver.name, table=BronzeEnum.quotes_petr4.name
        ).resolver_layer(domain="finance"),
    )
    gold_repository.QuotesPetr4GoldCommandRepository().writer_gold(
        spark_session=spark,
        connection=connection,
        df=df,
        table_name=BronzeEnum.quotes_petr4.name,
    )


def main() -> None:
    """Executa o ETL completo de bronze, silver e gold."""
    spark = SparkSessionManager(sgbd_name=SgbdEnum.postgresql.name)

    connection = ConnectionDatabase(
        environment="prd",
        db_name=DatabaseEnum.market_data_lakehouse_orchestrator.name,
    )
    connection.connect_with_retry()

    data_json_raw = process_bronze()
    process_silver(data_json_raw, spark)
    process_gold(spark=spark, connection=connection)


if __name__ == "__main__":
    main()
