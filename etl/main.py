import in_n_out as ino
import containers
from extractors.data_loader import DataLoader
from managers.s3_storage_manager import S3StorageManager
from managers.spark_session_manager import SparkSessionManager
from managers.data_transformer import DataTransformer
from pyspark.sql.functions import col
from decorators.decorators import log_decorator, timing_decorator


@log_decorator
@timing_decorator
@ino.inject
def main(spark_session_manager: SparkSessionManager, data_loader: DataLoader, data_transformer: DataTransformer):
    df = data_loader.load_data()
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
