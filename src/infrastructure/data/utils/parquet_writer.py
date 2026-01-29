# noqa: D100
from ..repository import WriterRepository


class ParquetWriter(WriterRepository):  # noqa: D101
    def write(self, df, path_file) -> None:  # noqa: D102
        if df.empty:
            raise ValueError("Dataframe is empty")

        temp_path = path_file.with_suffix(".tmp")

        path_file.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(temp_path, index=False, compression="snappy")

        temp_path.rename(path_file)
