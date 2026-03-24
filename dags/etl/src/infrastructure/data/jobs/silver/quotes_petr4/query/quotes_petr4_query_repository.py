"""Module for validating and transforming PETR4 data in Silver layer.

Provides validation schema and repository class for transforming raw Bronze
layer data according to the defined schema.
"""

import logging

from pydantic import ValidationError

from ..validator import QuotesPetr4ValidatorSchema

logger = logging.getLogger(__name__)


class QuotesPetr4SilverQueryRepository:
    """Repository for validating PETR4 quotes in Silver layer.

    Handles schema validation and data transformation for PETR4 stock
    price data moving from Bronze to Silver layer.
    """

    def __init__(self, data_json: dict, spark_session) -> None:
        """Initialize Silver validator with data and Spark session.

        Args:
            data_json: Raw JSON data from Bronze layer.
            spark_session: PySpark session for data transformation.
        """
        self.data_json = data_json
        self.spark_session = spark_session

    def validate_schema(self) -> list[dict]:
        """Validate and transform data according to schema.

        Validates each record against the QuotesPetr4ValidatorSchema and
        returns the transformed data.

        Returns:
            List of validated and transformed data dictionaries.

        Raises:
            ValidationError: If any record fails schema validation.
        """
        data_json = next(iter(self.data_json.values()))

        validated_data = []

        for item in data_json:
            try:
                stock = QuotesPetr4ValidatorSchema.model_validate(item)
                validated_data.append(stock.model_dump())
            except ValidationError as e:
                logger.exception(
                    f"Validation error for ticker symbol " f"'{item.get('symbol')}'"
                )
                raise e
        return data_json
