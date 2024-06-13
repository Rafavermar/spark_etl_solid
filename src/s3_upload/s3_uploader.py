import boto3
import os
import requests
from src.config.config import Config
from src.config.environment_setup import EnvironmentSetup
import logging
from boto3.s3.transfer import TransferConfig
from src.decorators.decorators import log_decorator
from src.decorators.decorators import timing_decorator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class S3Uploader:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.data_dir = os.path.abspath(Config.DATA_DIR)

    @staticmethod
    @log_decorator
    @timing_decorator
    def download_file(url, local_path):
        logging.info(f"Starting download for {local_path}")

        if not os.path.exists(local_path):
            logging.info(f"Downloading {url}")
            response = requests.get(url, stream=True)
            with open(local_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
            logging.info(f"Downloaded {url} to {local_path}")
        else:
            logging.info(f"{local_path} already exists. Skipping download.")

    @log_decorator
    @timing_decorator
    def upload_file(self, local_path, s3_bucket, s3_path):
        logging.info(f"Uploading {local_path} to s3://{s3_bucket}/{s3_path}")

        config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                multipart_chunksize=1024 * 25, use_threads=True)

        self.s3_client.upload_file(local_path, s3_bucket, s3_path, Config=config)
        logging.info(f"Uploaded {local_path} to s3://{s3_bucket}/{s3_path}")

    def ensure_files_in_local(self):
        """
        Ensures that both Chicago_crime_data.csv and police-station.csv are downloaded to the local DATA_DIR.
        """
        crime_local_path = os.path.join(self.data_dir, Config.LOCAL_FILENAME)
        self.download_file(Config.REMOTE_DATA_URL, crime_local_path)

        police_local_path = os.path.join(self.data_dir, "police-station.csv")
        self.download_file(Config.POLICE_STATION_URL, police_local_path)

    def download_and_upload_file(self, url, local_path, s3_bucket, s3_path):
        self.download_file(url, local_path)
        self.upload_file(local_path, s3_bucket, s3_path)


if __name__ == "__main__":
    logging.info("Starting s3_uploader.py")

    EnvironmentSetup.ensure_data_directory()

    s3_uploader = S3Uploader()

    s3_uploader.ensure_files_in_local()

    crime_local_path_to_upload = os.path.join(s3_uploader.data_dir, Config.LOCAL_FILENAME)
    crime_s3_path_to_upload = os.path.join("Bronze", Config.LOCAL_FILENAME).replace("\\", "/")
    s3_uploader.upload_file(crime_local_path_to_upload, Config.AWS_S3_BUCKET, crime_s3_path_to_upload)

    police_local_path_to_upload = os.path.join(s3_uploader.data_dir, "police-station.csv")
    police_s3_path_to_upload = os.path.join("Bronze", "police-station.csv").replace("\\", "/")
    s3_uploader.upload_file(police_local_path_to_upload, Config.AWS_S3_BUCKET, police_s3_path_to_upload)
