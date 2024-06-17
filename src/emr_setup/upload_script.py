import boto3
import logging
import os
from src.config.config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class S3Uploader:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def upload_script(self, local_path, s3_bucket, s3_key):
        self.s3_client.upload_file(local_path, s3_bucket, s3_key)
        logging.info(f"Uploaded {local_path} to s3://{s3_bucket}/{s3_key}")


if __name__ == "__main__":
    uploader = S3Uploader()
    script_path = Config.PYSPARK_JOB_PATH
    script_s3_key = 'pyspark_jobs/pyspark_job.py'
    uploader.upload_script(script_path, Config.AWS_S3_BUCKET, script_s3_key)
