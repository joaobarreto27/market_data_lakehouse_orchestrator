"""Utility for constructing SQL-compatible date/time ranges.

The class helps compute a begin/end timestamp pair based on a number of
past days and an optional explicit end date.
"""

from datetime import date, timedelta


class RangeDateParameter:
    """Encapsulates a datetime range used for SQL queries.

    Attributes:
        days_interval (int): number of days before the end date for the
            beginning of the range.
        begin_date (date): calculated start date.
        end_date (date): provided or defaulted end date.
    """

    def __init__(self, days_interval: int = 0, end_date: date | None = None) -> None:
        """Create a range parameter object.

        Args:
            days_interval (int): number of days prior to ``end_date`` to use
                as the beginning of the range. Must be non-negative.
            end_date (date | None): the last date of the range. If ``None``
                the current date is used.

        Raises:
            ValueError: if ``days_interval`` is negative, if ``end_date`` is
                not a date, or if ``end_date`` is before ``begin_date``.
        """
        self.days_interval: int = days_interval
        self.end_date = end_date
        today = date.today()
        self.begin_date = today - timedelta(days=self.days_interval)

        if self.days_interval < 0:
            raise ValueError("The parameter days_interval expects a positive value.")

        if not self.end_date:
            self.end_date = today

        if not isinstance(self.end_date, date):
            raise ValueError(
                """end_date must be a date object (e.g., date(2025, 1, 1)) or
                None (to use the current date)."""
            )

        if self.end_date < self.begin_date:
            raise ValueError("The end_date parameter cannot be less than begin_date.")

    def get_sql_range(self) -> tuple[str, str]:
        """Return the begin and end timestamps suitable for SQL queries.

        The times are set to 00:00:00 on the begin_date and 23:59:59 on the
        end_date.

        Returns:
            tuple[str, str]: (begin_timestamp, end_timestamp)
        """
        _begin_date = f"{self.begin_date} 00:00:00"
        _end_date = f"{self.end_date} 23:59:59"

        return _begin_date, _end_date
