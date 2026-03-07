import logging  # noqa: D100

from .....utils import JsonWriter

logger = logging.getLogger(__name__)


class QuotesPetr4BronzeCommandRepository:  # noqa: D101
    def __init__(self, data_json, path_file) -> None:  # noqa: D107
        self.data_json = data_json
        self.path_file = path_file
        logger.debug(f"BronzeCommandRepository initialized for path: {path_file}")

    def writer_bronze(self) -> None:  # noqa: D102
        try:
            logger.info(f"Starting Bronze layer write to: {self.path_file}")
            JsonWriter().write(data_json=self.data_json, path_file=self.path_file)
            logger.info(f"Bronze data written to {self.path_file}")
        except Exception as e:
            logger.exception(f"Failed to write Bronze data to {self.path_file}")
            raise e
