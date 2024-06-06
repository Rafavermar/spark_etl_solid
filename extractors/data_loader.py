import os
import requests
from pyspark.sql import SparkSession
from config.config import Config
from decorators.decorators import log_decorator, timing_decorator
from tqdm import tqdm


class DataLoader:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    @log_decorator
    @timing_decorator
    def load_data(self):
        local_file_path = Config.get_data_path(Config.LOCAL_FILENAME)
        if not os.path.exists(local_file_path):
            print("Downloading data...")
            response = requests.get(Config.REMOTE_DATA_URL, stream=True)
            total_size = int(response.headers.get('content-length', 0))
            with open(local_file_path, 'wb') as f:
                for data in tqdm(response.iter_content(1024), total=total_size // 1024, unit='KB'):
                    f.write(data)

        print("Loading data from local file...")
        df = self.spark_session.read.csv(local_file_path, header=True, inferSchema=True)
        return df