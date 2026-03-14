"""Module for resolving data lake layer paths with date partitioning.

This module provides utilities for generating file paths for bronze and silver
layers in the data lake architecture, with support for date-based partitioning.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class LayerPathResolver:
    """Resolver for data lake layer paths with partitioning support.

    This class handles path resolution for different data lake layers
    (bronze and silver),
    automatically generating partitioned paths based on date intervals.
    """

    def __init__(self, layer: str, table: str, environment: str = "dev") -> None:
        """Initialize path resolver with layer and table information.

        Args:
            layer: The data lake layer name ('bronze' or 'silver').
            table: The table or dataset name.
            environment: The environment ('dev' or 'prd').
        """
        self.layer: str = layer
        self.table: str = table
        self.environment: str = environment

    def resolver_layer(
        self,
        source_system: Optional[str] = None,
        domain: Optional[str] = None,
        date_interval: Optional[date] = None,
    ) -> str:
        """Resolve the appropriate path for the configured layer.

        Routes to bronze or silver path resolution based on the configured layer.

        Args:
            source_system: The source system name (required for bronze layer).
            domain: The business domain (required for silver layer).
            date_interval: The date for partitioning. Defaults to today's date.

        Returns:
            String path pointing to the layer-specific file location.

        Raises:
            ValueError: If layer is unsupported.
        """
        if self.layer == "bronze":
            path = self._get_bronze_path(
                source_system=source_system, date_interval=date_interval
            )
        elif self.layer == "silver":
            path = self._get_silver_path(domain=domain, date_interval=date_interval)
        else:
            msg = f"Unsupported layer: {self.layer}"
            logger.error(msg)
            raise ValueError(msg)

        if self.environment == "prd":
            return f"s3://market-data-lakehouse-bucket/{path}"
        else:
            return path

    def _get_bronze_path(
        self, source_system: Optional[str], date_interval: Optional[date]
    ) -> str:
        """Generate the path for bronze layer data.

        Args:
            source_system: The source system name.
            date_interval: The date for partitioning.

        Returns:
            String path for bronze layer file.

        Raises:
            ValueError: If source_system is not provided.
        """
        date_partition = self._generate_date_partition(date_interval)

        if source_system:
            path_file: str = (
                Path(self.layer)
                / source_system
                / self.table
                / date_partition
                / f"{self.table}.json"
            ).as_posix()
            logger.info(f"Bronze path resolved for {self.table}")
            return path_file
        else:
            msg = f"Source system required for Bronze layer {self.table}"
            logger.error(msg)
            raise ValueError(msg)

    def _get_silver_path(
        self, domain: Optional[str], date_interval: Optional[date]
    ) -> str:
        """Generate the path for silver layer data.

        Args:
            domain: The business domain name.
            date_interval: The date for partitioning.

        Returns:
            String path for silver layer file.

        Raises:
            ValueError: If domain is not provided.
        """
        date_partition = self._generate_date_partition(date_interval)

        if domain:
            path_file: str = (
                Path(self.layer)
                / domain
                / self.table
                / date_partition
                / f"{self.table}.parquet"
            ).as_posix()
            logger.info(f"Silver path resolved for {self.table}")
            return path_file
        else:
            msg = f"Domain required for Silver layer {self.table}"
            logger.error(msg)
            raise ValueError(msg)

    @staticmethod
    def _generate_date_partition(date_interval: Optional[date]) -> str:
        """Generate a date-based partition string in year/month/day format.

        Args:
            date_interval: The date to partition by. Defaults to today if None.

        Returns:
            A partition string in the format 'year=YYYY/month=MM/day=DD'.
        """
        date_interval = date_interval or date.today()

        date_partition: str = (
            f"year={date_interval.year}/"
            f"month={date_interval.strftime('%m')}/"
            f"day={date_interval.strftime('%d')}"
        )
        return date_partition
