from src.interfaces.i_storage_manager import IStorageManager
from pyspark.sql import SparkSession, DataFrame
from src.managers.s3_storage_manager import S3StorageManager


class MockStorageManager(IStorageManager):
    """
    Mock implementation of the IStorageManager interface for testing purposes.

    Methods:
        save_to_s3(df: DataFrame, path: str, partition_col: str = None):
            Mocks the save operation to an S3 path.

    Adheres to:
        Liskov Substitution Principle (LSP): This mock implementation can be substituted for any other implementation of IStorageManager without altering the correct behavior of the program.
    """

    def save_data(self, df: DataFrame, path: str, partition_col: str = None):
        """
        Mocks the save operation to an S3 path.

        Args:
            df (DataFrame): The DataFrame to save.
            path (str): The S3 path where the DataFrame will be saved.
            partition_col (str, optional): Column to partition the data by. Defaults to None.
        """
        print(f"Mock save to {path}")


def test_storage_manager():
    """
    Tests the IStorageManager implementations to ensure they adhere to the Liskov Substitution Principle (LSP).

    The test substitutes S3StorageManager with MockStorageManager and verifies that the behavior remains consistent.
    """
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])

    storage_manager: IStorageManager = MockStorageManager()
    storage_manager.save_data(df, "mock/path")

    storage_manager = S3StorageManager(spark)
    storage_manager.save_data(df, "s3a://bucket/path")


test_storage_manager()
if __name__ == "__main__":
    test_storage_manager()
