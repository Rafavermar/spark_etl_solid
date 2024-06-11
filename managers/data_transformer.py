from interfaces.i_data_transformer import IDataTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, lit, format_string, year, month
from managers.s3_storage_manager import S3StorageManager


class DataTransformer(IDataTransformer):
    def __init__(self, storage_manager: S3StorageManager):
        self.storage_manager = storage_manager

    def timestamp_countby_dayofweek(self, df: DataFrame) -> DataFrame:
        result_df = df.withColumn("timestamp", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")) \
            .withColumn("day_of_week", date_format(col("timestamp"), "E")) \
            .groupBy("day_of_week").count()
        self.storage_manager.save_to_s3(result_df, "s3a://spark-etl-rvm/Silver/timestamp_countby_dayofweek.parquet")
        return result_df

    def group_and_count_crimes_by_type(self, df: DataFrame) -> DataFrame:
        result_df = df.groupBy('Primary Type').count().orderBy('count', ascending=False)
        self.storage_manager.save_to_s3(result_df, "s3a://spark-etl-rvm/Silver/group_and_count_crimes_by_type.parquet")
        return result_df

    def add_and_count_crimes_from_specific_day(self, df: DataFrame, date_str='2018-11-12') -> DataFrame:
        df = df.withColumn('Date', to_timestamp(col('Date'), 'MM/dd/yyyy hh:mm:ss a'))

        date_target = to_timestamp(lit(date_str), 'yyyy-MM-dd')

        one_day = df.filter(col('Date').cast("date") == date_target.cast("date"))
        print(f"Count of crimes on {date_str}: {one_day.count()}")

        combined_df = df.union(one_day).orderBy('Date', ascending=False)
        combined_df.show(5)

        combined_df = combined_df.withColumn(
            'year_month',
            format_string("%d-%02d", year(col('Date')), month(col('Date')))
        )

        self.storage_manager.save_to_s3(combined_df,
                                        "s3a://spark-etl-rvm/Silver/add_and_count_crimes_from_specific_day.parquet",
                                        'year_month')
        return combined_df
