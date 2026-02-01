from typing import Any  # noqa: D100

from pydantic import ValidationError

from .....utils import ConnectAPI, SparkSessionManager
from ..validator import QuotesPetr4ValidatorSchema


class QuotesPetr4QueryRepository:  # noqa: D101
    def __init__(self, base_url: str) -> None:
        """Inicializa a consulta a API com a URL."""
        self.data_json: dict[str, Any] = {}
        self.data: dict[str, Any] = {}
        self.session = SparkSessionManager()
        self.base_url = base_url

    def get_daily_closing(self, quotes):  # noqa: D102
        url = f"{self.base_url}/{quotes}"
        self.data_json = ConnectAPI(url=url).connect()
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
        print(validated_data)
        return validated_data
