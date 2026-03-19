"""Module for persisting PETR4 stock data to Bronze layer.

Provides repository class for writing raw API response data
to the Bronze layer of the data lake.
"""

import logging
from typing import Any

from .....utils import JsonWriter

logger = logging.getLogger(__name__)


class QuotesPetr4BronzeCommandRepository:
    """Repository for persisting PETR4 quotes to Bronze layer.

    Handles writing raw API response data to JSON files in the Bronze layer
    with proper error handling and logging.
    """

    def __init__(self, data_json: Any, path_file: str) -> None:
        """Initialize Bronze writer with data and target path.

        Args:
            data_json: Raw JSON data from API response.
            path_file: Destination file path in Bronze layer.
        """
        self.data_json = data_json
        self.path_file = path_file
        logger.debug(f"BronzeCommandRepository initialized for path: {path_file}")

    def writer_bronze(self, spark) -> None:
        """Write raw Bronze layer data to JSON file.

        Persists the raw JSON data from API to the Bronze layer using
        atomic file operations for data integrity.

        Raises:
            IOError: If file write operation fails.
        """
        try:
            logger.info(f"Starting Bronze layer write to: {self.path_file}")
            JsonWriter(spark).write_to_s3(
                data_json=self.data_json, s3_path=self.path_file
            )
            logger.info(f"Bronze data written to {self.path_file}")
        except Exception as e:
            logger.exception(f"Failed to write Bronze data to {self.path_file}")
            raise e
