import boto3
import os
import requests
from config.config import Config


def download_and_upload_file(url, local_path, s3_bucket, s3_path):
    response = requests.get(url, stream=True)
    with open(local_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)
    print(f"Downloaded {url} to {local_path}")

    s3_client = boto3.client('s3')
    s3_client.upload_file(local_path, s3_bucket, s3_path)
    print(f"Uploaded {local_path} to s3://{s3_bucket}/{s3_path}")


if __name__ == "__main__":
    # Download and upload crime data
    crime_local_path = os.path.join(Config.DATA_DIR, Config.LOCAL_FILENAME)
    crime_s3_path = os.path.join('Bronze', Config.LOCAL_FILENAME)
    download_and_upload_file(Config.REMOTE_DATA_URL, crime_local_path, Config.AWS_S3_BUCKET, crime_s3_path)

    # Download and upload police station data
    police_local_path = os.path.join(Config.DATA_DIR, "police-station.csv")
    police_s3_path = os.path.join('Bronze', "police-station.csv")
    download_and_upload_file(Config.POLICE_STATION_URL, police_local_path, Config.AWS_S3_BUCKET, police_s3_path)
