from .....utils import JsonWriter  # noqa: D100


class QuotesPetr4BronzeCommandRepository:  # noqa: D101
    def __init__(self, data_json, path_file) -> None:  # noqa: D107
        self.data_json = data_json
        self.path_file = path_file

    def writer_bronze(self) -> None:  # noqa: D102
        JsonWriter().write(data_json=self.data_json, path_file=self.path_file)
