from typing import Any, Optional  # noqa: D100

from pydantic import ValidationError

from .....utils import ConnectAPI, EnvManager
from ..validator import QuotesPetr4ValidatorSchema


class QuotesPetr4QueryRepository:  # noqa: D101
    def __init__(self, base_url: str) -> None:
        """Inicializa a consulta a API com a URL."""
        self.data_json: dict[str, Any] = {}
        self.data: dict[str, Any] = {}
        self.base_url = base_url

    def get_token(self) -> str:  # noqa: D102
        api_token: Optional[str] = EnvManager().get_token()
        if not api_token:
            raise RuntimeError("Token da API não encontrado em variáveis de ambiente")
        return api_token

    def get_daily_closing(self, quotes):  # noqa: D102
        url = f"{self.base_url}/{quotes}"
        api_token = self.get_token()
        self.data_json = ConnectAPI(url=url).connect(bearer_token=api_token)
        if self.data_json:
            self.data = next(iter(self.data_json.values()))
        else:
            raise ValueError("Dicionário vazio, tente novamente!")

        validated_data = []

        for item in self.data:
            try:
                stock = QuotesPetr4ValidatorSchema.model_validate(item)
                validated_data.append(stock.model_dump())
            except ValidationError as e:
                print(f"Erro de validação no ticker {item.get('symbol')}: {e}")  # pyright: ignore[reportAttributeAccessIssue]
                raise e
        return validated_data
