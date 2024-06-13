import os
import requests
from pyspark.sql import SparkSession
from config.config import Config
from decorators.decorators import log_decorator, timing_decorator
import boto3


class DataLoader:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.s3_client = boto3.client('s3')

    @staticmethod
    def download_file(url: str, local_path: str):
        """
        Download a file from a URL and save it locally.
        """
        response = requests.get(url, stream=True)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"File downloaded and saved to {local_path}")

    @log_decorator
    @timing_decorator
    def load_data(self, use_s3=True):
        if use_s3:
            s3_path = os.path.join(Config.BRONZE_S3_PATH, Config.LOCAL_FILENAME).replace("\\", "/")
            print("Loading data from S3...")
            df = self.spark_session.read.csv(f"s3a://{Config.AWS_S3_BUCKET}/{s3_path}", header=True, inferSchema=True)
        else:
            local_file_path = os.path.join(Config.DATA_DIR, Config.LOCAL_FILENAME)
            if not os.path.exists(local_file_path):
                print(f"{local_file_path} not found. Downloading from {Config.REMOTE_DATA_URL}...")
                self.download_file(Config.REMOTE_DATA_URL, local_file_path)
            print("Loading data from local file...")
            df = self.spark_session.read.csv(local_file_path, header=True, inferSchema=True)
        return df

    @log_decorator
    @timing_decorator
    def load_police_station_data(self, use_s3=True):
        if use_s3:
            s3_path = os.path.join(Config.BRONZE_S3_PATH, "police-station.csv").replace("\\", "/")
            print("Loading police station data from S3...")
            df = self.spark_session.read.csv(f"s3a://{Config.AWS_S3_BUCKET}/{s3_path}", header=True, inferSchema=True)
        else:
            local_file_path = os.path.join(Config.DATA_DIR, "police-station.csv")
            if not os.path.exists(local_file_path):
                print(f"{local_file_path} not found. Downloading from {Config.POLICE_STATION_URL}...")
                self.download_file(Config.POLICE_STATION_URL, local_file_path)
            print("Loading police station data from local file...")
            df = self.spark_session.read.csv(local_file_path, header=True, inferSchema=True)
        return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName(Config.SPARK_APP_NAME).getOrCreate()
    data_loader = DataLoader(spark)
    data_loader.load_data(use_s3=True)
    data_loader.load_police_station_data(use_s3=True)
