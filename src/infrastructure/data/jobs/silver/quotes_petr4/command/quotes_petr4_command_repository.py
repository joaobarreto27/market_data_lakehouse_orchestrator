"""Module for persisting PETR4 transformed data to Silver layer.

Provides repository class for writing validated and transformed data
to the Silver layer of the data lake in Parquet format.
"""

import logging
from pathlib import Path

from pandas import DataFrame

from .....utils import ParquetWriter

logger = logging.getLogger(__name__)


class QuotesPetr4SilverCommandRepository:
    """Repository for persisting PETR4 quotes to Silver layer.

    Handles writing validated and transformed PETR4 data from Bronze layer
    to Parquet files in the Silver layer with proper partitioning.
    """

    def __init__(self, path_file_silver: Path, df: DataFrame) -> None:
        """Initialize Silver writer with target path and DataFrame.

        Args:
            path_file_silver: Destination file path in Silver layer.
            df: PySpark DataFrame with validated data to persist.
        """
        self.path_file_silver = path_file_silver
        self.df = df

    def write_silver(self) -> None:
        """Write validated Silver layer data to Parquet file.

        Persists the validated DataFrame to the Silver layer using
        atomic file operations for data integrity.

        Raises:
            IOError: If file write operation fails.
        """
        try:
            logger.info(f"Starting Silver layer write to: {self.path_file_silver}")
            ParquetWriter().write(df=self.df, path_file=self.path_file_silver)
            logger.info(f"Silver data written to {self.path_file_silver}")
        except Exception as e:
            logger.exception(f"Failed to write Silver data to {self.path_file_silver}")
            raise e
