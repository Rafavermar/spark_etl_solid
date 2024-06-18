from src.interfaces.i_spark_session_manager import ISparkSessionManager
from pyspark.sql import SparkSession
from src.config.config import Config


class SparkSessionManager(ISparkSessionManager):
    """
    SparkSessionManager class to manage the lifecycle of a Spark session.

    Attributes:
        spark (SparkSession): Spark session for data operations.

    Adheres to: Single Responsibility Principle (SRP): This class is responsible for managing the Spark session.
    Dependency Inversion Principle (DIP): Implements the ISparkSessionManager interface, adhering to the abstraction.
    """
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .master(Config.SPARK_URL) \
            .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
            .config("spark.executor.cores", Config.SPARK_EXECUTOR_CORES) \
            .config("spark.cores.max", Config.SPARK_CORES_MAX) \
            .config("spark.local.dir", Config.LOCAL_DIR) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", Config.AWS_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", Config.AWS_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,"
                                           "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                                    '.SimpleAWSCredentialsProvider') \
            .config("spark.hadoop.fs.s3a.impl.disable.cache", "true") \
            .config("hadoop.home.dir", "C:/winutils") \
            .getOrCreate()

    def get_spark_session(self) -> SparkSession:
        return self.spark

    def stop_spark(self):
        self.spark.stop()
