import in_n_out as ino
import containers

from src.config.config import Config
from src.extractors.data_loader import DataLoader
from src.managers.spark_session_manager import SparkSessionManager
from src.managers.data_transformer import DataTransformer
from pyspark.sql.functions import col
from src.decorators.decorators import log_decorator, timing_decorator
import os


@log_decorator
@timing_decorator
@ino.inject
def main(spark_session_manager: SparkSessionManager, data_loader: DataLoader, data_transformer: DataTransformer):
    """
      Main function to execute the ETL process using Spark, loading data, and applying transformations.

      Args:
          spark_session_manager (SparkSessionManager): Manager for Spark session.
          data_loader (DataLoader): Loader for data from local or remote sources.
          data_transformer (DataTransformer): Transformer for data processing and aggregation.

      Adheres to: Dependency Inversion Principle (DIP): High-level module (main function) depends on abstractions (
      interfaces and injected dependencies) rather than concrete implementations.
    """
    use_s3 = Config.USE_S3

    df = data_loader.load_data(use_s3=use_s3)
    df.show(5)

    df_filtered = df.filter(col("X Coordinate").isNotNull())
    df_filtered.show()

    processed_df = data_transformer.timestamp_countby_dayofweek(df)
    processed_df.show()

    combined_df = data_transformer.add_and_count_crimes_from_specific_day(df_filtered, '2018-11-12')
    combined_df.show()

    grouped_df = data_transformer.group_and_count_crimes_by_type(df_filtered)
    grouped_df.show()

    spark_session_manager.stop_spark()


if __name__ == "__main__":

    main()

    from src.emr_setup import emr_cluster

    emr_cluster.main()
