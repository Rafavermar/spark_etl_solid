import os

from src.config.config import Config
from src.interfaces.i_storage_manager import IStorageManager
from pyspark.sql import DataFrame


class LocalStorageManager(IStorageManager):
    def __init__(self, output_dir: str):
        self.output_dir = Config.LOCAL_STORAGE_PATH
        os.makedirs(self.output_dir, exist_ok=True)

    def save_data(self, df: DataFrame, filename: str,
                  partition_col: str = None):
        full_path = os.path.join(self.output_dir, filename)
        if partition_col:
            df.write.partitionBy(partition_col).mode('overwrite').parquet(
                full_path)
        else:
            df.write.mode('overwrite').parquet(full_path)
