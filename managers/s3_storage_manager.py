from interfaces.i_storage_manager import IStorageManager
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from config.config import Config
from decorators.decorators import log_decorator, timing_decorator


class S3StorageManager(IStorageManager):
    """
    S3StorageManager class to handle storage operations in AWS S3 using Spark.

    Attributes:
        spark (SparkSession): Spark session for data operations.

    Adheres to:
        Single Responsibility Principle (SRP): This class is responsible for managing storage operations in S3.
        Dependency Inversion Principle (DIP): Implements the IStorageManager interface, adhering to the abstraction.
    """
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    @log_decorator
    @timing_decorator
    def save_to_s3(self, df: DataFrame, path: str, partition_col: str = None):
        """
               Saves a DataFrame to an S3 path.

               Args:
                   df (DataFrame): The DataFrame to save.
                   path (str): The S3 path where the DataFrame will be saved.
                   partition_col (str, optional): Column to partition the data by. Defaults to None.
               """
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", Config.AWS_ACCESS_KEY)
        hadoop_conf.set("fs.s3a.secret.key", Config.AWS_SECRET_KEY)
        hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.multipart.size", "104857600")
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

        if partition_col:
            df.write.partitionBy(partition_col).mode('overwrite').parquet(path)
        else:
            df.write.mode('overwrite').parquet(path)
        print(f"DataFrame saved to {path}")
