from .....utils import ParquetWriter  # noqa: D100


class QuotesPetr4SilverCommandRepository:  # noqa: D101
    def __init__(self, path_file_silver, df) -> None:  # noqa: D107
        self.path_file_silver = path_file_silver
        self.df = df

    def write_silver(self):  # noqa: D102
        ParquetWriter().write(df=self.df, path_file=self.path_file_silver)
