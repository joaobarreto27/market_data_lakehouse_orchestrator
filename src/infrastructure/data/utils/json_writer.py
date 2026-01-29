import json  # noqa: D100

from ..repository import WriterRepository


class JsonWriter(WriterRepository):  # noqa: D101
    def write(self, data_json, path_file) -> None:  # noqa: D102
        if data_json is None or (
            isinstance(data_json, (list, dict)) and len(data_json) == 0
        ):
            raise ValueError("Data to write is empty or None")

        temp_path = path_file.with_sufix(".tmp")

        try:
            path_file.parent.mkdir(parents=True, exist_ok=True)
            with open(temp_path, "w", encoding="utf-8") as file:
                json.dump(data_json, file, indent=4, ensure_ascii=False)

            temp_path.rename(path_file)

        except Exception as e:
            if temp_path.exists():
                temp_path.unlink()
            raise e
