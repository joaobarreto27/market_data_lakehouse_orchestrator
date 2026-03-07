from pydantic import ValidationError  # noqa: D100

from ..validator import QuotesPetr4ValidatorSchema


class QuotesPetr4SilverQueryRepository:  # noqa: D101
    def __init__(self, data_json, spark_session) -> None:  # noqa: D107
        self.data_json = data_json
        self.spark_session = spark_session

    def validate_schema(self):  # noqa: D102
        data_json = next(iter(self.data_json.values()))

        validated_data = []

        for item in data_json:
            try:
                stock = QuotesPetr4ValidatorSchema.model_validate(item)
                validated_data.append(stock.model_dump())
            except ValidationError as e:
                print(f"Erro de validação no ticker {item.get('symbol')}: {e}")  # pyright: ignore[reportAttributeAccessIssue]
                raise e
        return data_json
