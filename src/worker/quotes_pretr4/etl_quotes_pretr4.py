"""Worker responsável por executar o ETL diário de cotações de PETR4."""

from datetime import datetime  # noqa: F401

from pyspark.sql.functions import lit  # noqa: F401
