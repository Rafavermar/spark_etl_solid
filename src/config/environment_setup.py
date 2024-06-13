import os
from src.config.config import Config


class EnvironmentSetup:
    @staticmethod
    def ensure_data_directory():
        if not os.path.exists(Config.DATA_DIR):
            os.makedirs(Config.DATA_DIR)
