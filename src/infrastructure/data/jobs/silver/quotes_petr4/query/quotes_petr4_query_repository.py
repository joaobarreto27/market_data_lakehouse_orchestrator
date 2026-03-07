import logging  # noqa: D100

from pydantic import ValidationError

from ..validator import QuotesPetr4ValidatorSchema

logger = logging.getLogger(__name__)


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
                logger.exception(f"Erro de validação no ticker {item.get('symbol')}")
                raise e
        return data_json
