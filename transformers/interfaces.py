from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class ISparkSessionManager(ABC):
    @abstractmethod
    def get_spark_session(self) -> SparkSession:
        pass

    @abstractmethod
    def stop_spark(self):
        pass


class IDataTransformer(ABC):
    @abstractmethod
    def transform_data(self, df: DataFrame) -> DataFrame:
        pass


class IStorageManager(ABC):
    @abstractmethod
    def save_data(self, df: DataFrame, path: str):
        pass
