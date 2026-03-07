from functools import cached_property  # noqa: D100
from pathlib import Path


class SqlQueryLoader:
    """Carrega o conteúdo de um arquivo .sql de forma lazy (apenas primeira consulta).

    Com cache automático e validação de existência do arquivo.

    Exemplo de uso:
        loader = SqlQueryLoader("minha_analise", "silver")
        print(loader.query)         # → lê e cacheia na primeira vez
        print(loader.path)          # → caminho validado
    """

    def __init__(
        self,
        sql_file: str,
        layer: str,
        base_dir: Path | None = None,
    ) -> None:
        """Args

        sql_file: Nome do arquivo SQL (sem a extensão .sql)
        layer: Camada do data lake/warehouse (bronze, silver, gold...)
        base_dir: Diretório base opcional (para testes ou ambientes diferentes)
        """
        self.sql_file: str = sql_file
        self.layer: str = layer
        self._base_dir = (
            base_dir if base_dir is not None else Path(__file__).resolve().parent
        )

    @cached_property
    def path(self) -> Path:
        """Caminho absoluto do arquivo .sql.

        Valida a existência na primeira consulta → levanta FileNotFoundError
        se não existir.
        """
        path = self._base_dir.joinpath(
            "jobs", self.layer, self.sql_file, "sql", "query", f"{self.sql_file}.sql"
        )

        if not path.is_file():
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {path}")

        return path

    @cached_property
    def query(self) -> str:
        """Conteúdo completo do arquivo SQL (lido uma única vez e cacheado).

        Usa encoding UTF-8.
        """
        return self.path.read_text(encoding="utf-8")
