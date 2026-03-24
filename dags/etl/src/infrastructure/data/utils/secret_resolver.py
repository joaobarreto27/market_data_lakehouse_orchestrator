import json  # noqa: D100
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SecretResolver:  # noqa: D101
    def __init__(self, config_path: str = "secret_config.json"):  # noqa: D107
        current_dir = Path(__file__).resolve().parent
        path = current_dir.parent.joinpath("connection", config_path)
        self.config_path = Path(path)

        self._config_cache = None

    def _load_config(self):
        if self._config_cache is None:
            if not self.config_path.is_file():
                raise FileNotFoundError(
                    f"Config JSON não encontrado: {self.config_path}"
                )
            with open(self.config_path, "r") as f:
                self._config_cache = json.load(f)
        return self._config_cache

    def resolve(self, sgbd: str, db: str) -> str:
        """Busca no JSON o path da secret AWS baseado no SGDB e Banco."""
        config = self._load_config()
        if not self.config_path.is_file():
            raise FileNotFoundError(
                f"Config JSON não encontrado em: {self.config_path}"
            )

        with open(self.config_path, "r") as f:
            config = json.load(f)

        try:
            data = config["connections"][sgbd][db]
            # Monta o padrão: prefix/sgbd/db/env/alias
            return f"{data['prefix']}/{sgbd}/{db}/{data['environment']}/{data['alias']}"
        except KeyError as e:
            raise KeyError(
                f"""Configuração não mapeada para SGDB: '{sgbd}' e
                Database: '{db}' no JSON."""
                f"Faltando chave: {e}"
            ) from None
