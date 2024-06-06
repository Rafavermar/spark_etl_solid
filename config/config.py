import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SPARK_APP_NAME = os.getenv('SPARK_APP_NAME')
    SPARK_URL = os.getenv('SPARK_URL')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY')
    SPARK_EXECUTOR_CORES = int(os.getenv('SPARK_EXECUTOR_CORES'))
    SPARK_CORES_MAX = int(os.getenv('SPARK_CORES_MAX'))
    DATA_DIR = os.getenv('DATA_DIR')
    REMOTE_DATA_URL = os.getenv('REMOTE_DATA_URL')
    LOCAL_FILENAME = os.getenv('LOCAL_FILENAME')
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

    @staticmethod
    def get_data_path(filename):
        return os.path.join(Config.DATA_DIR, filename)


# Ensure the data directory exists
if not os.path.exists(Config.DATA_DIR):
    os.makedirs(Config.DATA_DIR)

