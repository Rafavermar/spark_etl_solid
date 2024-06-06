import in_n_out as ino
from extractors.data_loader import DataLoader
from transformers.spark_manager import SparkManager


def provide_spark_manager() -> SparkManager:
    return SparkManager()


def provide_data_loader() -> DataLoader:
    spark_manager = provide_spark_manager()
    return DataLoader(spark_manager.get_spark_session())


ino.register_provider(provide_spark_manager)
ino.register_provider(provide_data_loader)