from interfaces.i_spark_session_manager import ISparkSessionManager
from pyspark.sql import SparkSession
from config.config import Config


class SparkSessionManager(ISparkSessionManager):
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .master(Config.SPARK_URL) \
            .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
            .config("spark.executor.cores", Config.SPARK_EXECUTOR_CORES) \
            .config("spark.cores.max", Config.SPARK_CORES_MAX) \
            .config("spark.local.dir", "C:/Users/RafaelVera-Marañón/tmp/spark-temp") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", Config.AWS_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", Config.AWS_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,"
                                           "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                                    '.SimpleAWSCredentialsProvider') \
            .config("hadoop.home.dir", "C:/winutils") \
            .getOrCreate()

    def get_spark_session(self) -> SparkSession:
        return self.spark

    def stop_spark(self):
        self.spark.stop()
