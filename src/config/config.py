import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """
        Configuration class to load environment variables for Spark and AWS.

        Attributes:
            SPARK_APP_NAME (str): Name of the Spark application.
            SPARK_URL (str): URL of the Spark master.
            SPARK_EXECUTOR_MEMORY (str): Memory allocated to Spark executors.
            SPARK_EXECUTOR_CORES (int): Number of cores allocated to Spark executors.
            SPARK_CORES_MAX (int): Maximum number of cores for Spark.
            DATA_DIR (str): Directory for storing data.
            REMOTE_DATA_URL (str): URL for remote data source.
            LOCAL_FILENAME (str): Local filename for downloaded data.
            POLICE_STATION_URL (str): URL for police station data.
            BRONZE_S3_PATH (str): S3 path for Bronze data storage.
            AWS_ACCESS_KEY (str): AWS access key for S3.
            AWS_SECRET_KEY (str): AWS secret key for S3.
            AWS_S3_BUCKET (str): AWS S3 bucket name.
            PYSPARK_JOB_PATH (str): Local path to the PySpark job script

        Adheres to:
            Single Responsibility Principle (SRP): This class is solely responsible for loading configuration settings.
        """
    USE_S3 = os.getenv('USE_S3')
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME')
    SPARK_URL = os.getenv('SPARK_URL')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY')
    SPARK_EXECUTOR_CORES = int(os.getenv('SPARK_EXECUTOR_CORES'))
    SPARK_CORES_MAX = int(os.getenv('SPARK_CORES_MAX'))
    DATA_DIR = os.getenv('DATA_DIR')
    REMOTE_DATA_URL = os.getenv('REMOTE_DATA_URL')
    LOCAL_FILENAME = os.getenv('LOCAL_FILENAME')
    POLICE_STATION_URL = os.getenv('POLICE_STATION_URL')
    BRONZE_S3_PATH = os.getenv('BRONZE_S3_PATH')
    SILVER_S3_PATH = os.getenv('SILVER_S3_PATH')
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
    AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET')
    LOCAL_DIR = os.getenv('LOCAL_DIR')
    PYSPARK_JOB_PATH = os.getenv('PYSPARK_JOB_PATH')
    LOG_BUCKET = os.getenv('LOG_BUCKET')
    EC2_KEY_NAME = os.getenv('EC2_KEY_NAME')

    @staticmethod
    def get_data_path(filename):
        return os.path.join(Config.DATA_DIR, filename)
