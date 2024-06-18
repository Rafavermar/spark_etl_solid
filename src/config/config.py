import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """
        Configuration class to load environment variables for Spark and AWS.

        Attributes:
            AWS_ACCESS_KEY (str): AWS access key for S3.
            AWS_S3_BUCKET (str): AWS S3 bucket name.
            AWS_SECRET_KEY (str): AWS secret key for S3.
            BRONZE_S3_PATH (str): S3 path for Bronze data storage.
            DATA_DIR (str): Directory for storing data.
            LOCAL_DIR (str): Directory for local temporary files.
            LOCAL_FILENAME (str): Local filename for downloaded data.
            LOCAL_FILENAME_STATIONS (str): Local filename for police station data.
            POLICE_STATION_URL (str): URL for police station data.
            REMOTE_DATA_URL (str): URL for remote data source.
            SILVER_S3_PATH (str): S3 path for Silver data storage.
            SPARK_APP_NAME (str): Name of the Spark application.
            SPARK_CORES_MAX (int): Maximum number of cores for Spark.
            SPARK_EXECUTOR_CORES (int): Number of cores allocated to Spark executors.
            SPARK_EXECUTOR_MEMORY (str): Memory allocated to Spark executors.
            SPARK_URL (str): URL of the Spark master.

        Adheres to:
            Single Responsibility Principle (SRP): This class is solely responsible for loading configuration settings.
        """
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
    BRONZE_S3_PATH = os.getenv('BRONZE_S3_PATH')
    SILVER_S3_PATH = os.getenv('SILVER_S3_PATH')
    DATA_DIR = os.getenv('DATA_DIR')
    LOCAL_DIR = os.getenv('LOCAL_DIR')
    LOCAL_FILENAME = os.getenv('LOCAL_FILENAME')
    LOCAL_FILENAME_STATIONS = os.getenv('LOCAL_FILENAME_STATIONS')
    POLICE_STATION_URL = os.getenv('POLICE_STATION_URL')
    REMOTE_DATA_URL = os.getenv('REMOTE_DATA_URL')
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME')
    SPARK_CORES_MAX = int(os.getenv('SPARK_CORES_MAX'))
    SPARK_EXECUTOR_CORES = int(os.getenv('SPARK_EXECUTOR_CORES'))
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY')
    SPARK_URL = os.getenv('SPARK_URL')

    @staticmethod
    def get_data_path(filename):
        return os.path.join(Config.DATA_DIR, filename)
