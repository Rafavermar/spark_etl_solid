import in_n_out as ino
from extractors.data_loader import DataLoader
from transformers.spark_manager import SparkManager
from pyspark.sql.functions import col
from decorators.decorators import log_decorator, timing_decorator
from pyspark.sql import SparkSession


@log_decorator
@timing_decorator
@ino.inject
def main(spark_manager: SparkManager, data_loader: DataLoader):
    df = data_loader.load_data()
    df.show(5)
    df.filter(col("X Coordinate").isNotNull()).show()
    processed_df = spark_manager.timestamp_countby_dayofweek(df)
    processed_df.show()
    spark_manager.stop_spark()


def provide_spark_manager() -> SparkManager:
    return SparkManager()


def provide_data_loader() -> DataLoader:
    spark_manager = provide_spark_manager()
    return DataLoader(spark_manager.get_spark_session())


ino.register_provider(provide_spark_manager)
ino.register_provider(provide_data_loader)

if __name__ == "__main__":
    main()