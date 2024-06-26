import argparse
import in_n_out as ino
from src.containers import provide_spark_session_manager
from src.extractors.data_loader import DataLoader
from src.managers.local_storage_manager import LocalStorageManager
from src.managers.s3_storage_manager import S3StorageManager
from src.managers.spark_session_manager import SparkSessionManager
from src.managers.data_transformer import DataTransformer
from pyspark.sql.functions import col
from src.decorators.decorators import log_decorator, timing_decorator


@log_decorator
@timing_decorator
@ino.inject
def main(spark_session_manager: SparkSessionManager, data_loader: DataLoader,
         data_transformer: DataTransformer):
    """
      Main function to execute the ETL process using Spark, loading data, and applying transformations.

      Args:
          spark_session_manager (SparkSessionManager): Manager for Spark session.
          data_loader (DataLoader): Loader for data from local or remote sources.
          data_transformer (DataTransformer): Transformer for data processing and aggregation.

      Adheres to: Dependency Inversion Principle (DIP): High-level module (main function) depends on abstrations (
      interfaces and injected dependencies) rather than concrete implementations.
      :param use_s3:
    """

    df = data_loader.load_data()
    df.show(5)

    df_filtered = df.filter(col("X Coordinate").isNotNull())
    df_filtered.show()

    processed_df = data_transformer.timestamp_countby_dayofweek(df)
    processed_df.show()

    combined_df = data_transformer.add_and_count_crimes_from_specific_day(
        df_filtered, '2018-11-12')
    combined_df.show()

    grouped_df = data_transformer.group_and_count_crimes_by_type(df_filtered)
    grouped_df.show()

    spark_session_manager.stop_spark()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the ETL job with optional S3 loading.")
    parser.add_argument("--use-s3", action="store_true",
                        help="Load data from S3 instead of local files.")
    parser.add_argument("--save-local", action="store_true",
                        help="Save data locally instead of on S3.")
    args = parser.parse_args()


    def provide_data_loader() -> DataLoader:
        print("Registrando DataLoader")
        spark_session_manager = provide_spark_session_manager()
        return DataLoader(spark_session_manager.get_spark_session(),
                          use_s3=args.use_s3)


    ino.register_provider(provide_data_loader)
    print("DataLoader registrado")

    spark_session_manager = provide_spark_session_manager()

    if args.save_local:
        storage_manager = LocalStorageManager(output_dir='./data/output')
    else:
        storage_manager = S3StorageManager(
            spark_session_manager.get_spark_session())

    data_transformer = DataTransformer(storage_manager)

    main(spark_session_manager, provide_data_loader(), data_transformer)

# from src.emr_setup import emr_cluster

# emr_cluster.main()
