from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class IStorageManager(ABC):
    @abstractmethod
    def save_to_s3(self, df: DataFrame, path: str, partition_col: str = None):
        """Saves DataFrame to S3 at the specified path."""
        pass
