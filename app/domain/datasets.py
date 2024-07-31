from json import dump
from glob import glob
from pathlib import Path
from os.path import join, basename
from abc import ABC, abstractmethod

from app.model.datasets import MedallionDataset


class BaseDatasetManager(ABC):
    @abstractmethod
    def setup_dataset(**kwargs) -> None: ...

    @abstractmethod
    def write_data(**kwargs) -> None: ...


class MedallionDatasetManager(BaseDatasetManager):
    def __init__(self, dataset: MedallionDataset) -> None:
        self.__dataset = dataset
        self.__path = Path(self.__dataset.mount_location)

    def get_dataset(self) -> MedallionDataset:
        return self.__dataset

    def get_path(self) -> str:
        return self.__path

    def get_path_string(self) -> str:
        return self.__path.as_posix()

    def setup_dataset(self) -> None:
        """Setups dataset mount in case it doesn't exist"""
        self.__path.mkdir(parents=True, exist_ok=True)

    def create_directory(self, directory: str) -> None:
        """Creates a new folder in the dataset mount"""
        path = self.__path / directory
        path.mkdir(parents=True, exist_ok=True)

    def write_data(self, filename: str, data: dict[str, str]) -> None:
        filepath = self.__path / filename
        dump(data, open(filepath, "w"))

    def move_files(
        self, source: str, destination: str, file_pattern: str = "*"
    ) -> None:
        source = self.__path / source
        destination = self.__path / destination

        processed_files = glob(join(source, file_pattern))
        for file_path in processed_files:
            old_path = Path(file_path)
            new_path = Path(join(destination, basename(file_path)))

            old_path.rename(new_path)


source = "spark-warehouse/datasets/raw/"
destination = "spark-warehouse/datasets/raw/processed"
