from abc import ABC, abstractmethod  # noqa: D100


class WriterRepository(ABC):  # noqa: D101
    @abstractmethod
    def save_data(self) -> None:  # noqa: D102
        pass
