from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class IStorageManager(ABC):
    """
       Interface for storage management operations to be implemented by any storage manager class.

       Adheres to:
           Interface Segregation Principle (ISP): Provides a specific interface for storage management, ensuring classes only implement what they need.
       """
    @abstractmethod
    def save_data(self, df: DataFrame, path: str, partition_col: str = None):
        """
        Saves a DataFrame to an S3 path.

        Args:
            df (DataFrame): The DataFrame to save.
            path (str): The S3 path where the DataFrame will be saved.
            partition_col (str, optional): Column to partition the data by. Defaults to None.
        """
        pass
