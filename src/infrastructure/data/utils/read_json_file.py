# noqa: D100
from pathlib import Path
from typing import Any

import pandas as pd


class ReadJsonFile:  # noqa: D101
    def read(self, path_file) -> Any:  # noqa: D102
        path_file = Path(path_file)
        current_dir: Path = Path(__file__).resolve().parent.parent.parent.parent.parent
        path: Path = current_dir.joinpath(path_file)

        try:
            if not path.is_file():
                raise FileNotFoundError(f"Configuration file not found at: {path_file}")
            df: pd.DataFrame = pd.read_json(path_file)
            return df
        except Exception as e:
            raise e
