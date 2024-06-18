import os
from pyspark.sql import SparkSession
from src.config.config import Config
from src.decorators.decorators import log_decorator, timing_decorator
import boto3


class DataLoader:
    def __init__(self, spark_session: SparkSession, use_s3: bool = True):
        self.spark_session = spark_session
        self.use_s3 = use_s3
        self.s3_client = boto3.client('s3')

    @log_decorator
    @timing_decorator
    def load_data(self):
        if self.use_s3:
            s3_path = os.path.join(Config.BRONZE_S3_PATH, Config.LOCAL_FILENAME).replace("\\", "/")
            print("Loading data from S3...")
            df = self.spark_session.read.csv(f"s3a://{Config.AWS_S3_BUCKET}/{s3_path}", header=True, inferSchema=True)
        else:
            local_file_path = Config.get_data_path(Config.LOCAL_FILENAME)
            print("Loading data from local file...")
            df = self.spark_session.read.csv(local_file_path, header=True, inferSchema=True)
        return df

    @log_decorator
    @timing_decorator
    def load_police_station_data(self):
        if self.use_s3:
            s3_path = os.path.join(Config.BRONZE_S3_PATH, "police-station.csv").replace("\\", "/")
            print("Loading police station data from S3...")
            df = self.spark_session.read.csv(f"s3a://{Config.AWS_S3_BUCKET}/{s3_path}", header=True, inferSchema=True)
        else:
            local_file_path = Config.get_data_path("police-station.csv")
            print("Loading police station data from local file...")
            df = self.spark_session.read.csv(local_file_path, header=True, inferSchema=True)
        return df
