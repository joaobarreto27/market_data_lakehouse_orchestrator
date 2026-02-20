from datetime import date  # noqa: D100
from pathlib import Path
from typing import Optional


class LayerPathResolver:  # noqa: D101
    def __init__(self, layer: str, table: str) -> None:  # noqa: D107
        self.layer = layer
        self.table = table

    def resolver_layer(  # noqa: D102
        self,
        source_system: Optional[str] = None,
        domain: Optional[str] = None,
        date_interval: Optional[date] = None,
    ):  # noqa: D102
        if self.layer == "bronze":
            return self._get_bronze_path(
                source_system=source_system, date_interval=date_interval
            )
        elif self.layer == "silver":
            return self._get_silver_path(domain=domain, date_interval=date_interval)
        else:
            raise ValueError("Specify a corresponding layer.")

    def _get_bronze_path(
        self, source_system: Optional[str], date_interval: Optional[date]
    ):

        date_partition = self._generate_date_partition(date_interval)

        if source_system:
            path_file = (
                Path(self.layer)
                / source_system
                / self.table
                / date_partition
                / f"{self.table}.json"
            )
            return path_file
        else:
            raise ValueError(
                """The 'source_system' parameter is required for the Bronze tier."""
            )

    def _get_silver_path(self, domain: Optional[str], date_interval: Optional[date]):

        date_partition = self._generate_date_partition(date_interval)

        if domain:
            path_file = (
                Path(self.layer)
                / domain
                / self.table
                / date_partition
                / f"{self.table}.parquet"
            )
            return path_file
        else:
            raise ValueError(
                """The 'domain' parameter is required for the Silver tier."""
            )

    @staticmethod
    def _generate_date_partition(date_interval) -> str:
        if not date_interval:
            date_interval = date.today()

        date_particion = (
            f"year={date_interval.year}/"
            f"month={date_interval.strftime('%m')}/"
            f"day={date_interval.strftime('%d')}"
        )
        return date_particion
