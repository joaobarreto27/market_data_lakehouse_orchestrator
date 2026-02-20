# noqa: D100
from pathlib import Path

from pandas import DataFrame

from ..repository import WriterRepository


class ParquetWriter(WriterRepository):  # noqa: D101
    def write(self, df: DataFrame, path_file: Path) -> None:  # noqa: D102
        if df.empty:
            raise ValueError("Dataframe is empty")

        path_file = Path(path_file)
        temp_path = path_file.with_suffix(".tmp")

        try:
            path_file.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(
                temp_path, index=False, compression="snappy", engine="pyarrow"
            )

            temp_path.rename(path_file)

        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise e
