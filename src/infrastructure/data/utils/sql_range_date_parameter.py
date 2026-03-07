from datetime import date, timedelta  # noqa: D100


class RangeDateParameter:  # noqa: D101
    def __init__(self, days_interval: int = 0, end_date: date | None = None) -> None:  # noqa: D107
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

    def get_sql_range(self) -> tuple[str, str]:  # noqa: D102
        _begin_date = f"{self.begin_date} 00:00:00"
        _end_date = f"{self.end_date} 23:59:59"

        return _begin_date, _end_date
