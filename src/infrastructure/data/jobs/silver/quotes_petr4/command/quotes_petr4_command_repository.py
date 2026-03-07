import logging  # noqa: D100

from .....utils import ParquetWriter

logger = logging.getLogger(__name__)


class QuotesPetr4SilverCommandRepository:  # noqa: D101
    def __init__(self, path_file_silver, df) -> None:  # noqa: D107
        self.path_file_silver = path_file_silver
        self.df = df

    def write_silver(self):  # noqa: D102
        try:
            logger.info(f"Starting Silver layer write to: {self.path_file_silver}")
            ParquetWriter().write(df=self.df, path_file=self.path_file_silver)
            logger.info(f"Silver data written to {self.path_file_silver}")
        except Exception as e:
            logger.exception(f"Failed to write Silver data to {self.path_file_silver}")
            raise e
