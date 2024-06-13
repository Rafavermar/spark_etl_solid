from src.config.config import Config
from src.interfaces.i_data_transformer import IDataTransformer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, lit, format_string, year, month
from src.managers.s3_storage_manager import S3StorageManager


class DataTransformer(IDataTransformer):
    """
    DataTransformer class to perform various data transformation operations using Spark.

    Attributes:
        storage_manager (IStorageManager): Manager for handling storage operations.

    Adheres to:
        Open/Closed Principle (OCP): This class can be extended with new transformation methods without modifying existing code.
        Dependency Inversion Principle (DIP): Depends on the storage manager abstraction, not a concrete implementation.
    """
    def __init__(self, storage_manager: S3StorageManager):
        self.storage_manager = storage_manager

    def timestamp_countby_dayofweek(self, df: DataFrame) -> DataFrame:
        """
        Transform the DataFrame to count occurrences by day of the week and save the result to S3.

        Args:
            df (DataFrame): Input DataFrame containing the data.

        Returns:
            DataFrame: Transformed DataFrame with counts by day of the week.
        """
        result_df = df.withColumn("timestamp", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")) \
            .withColumn("day_of_week", date_format(col("timestamp"), "E")) \
            .groupBy("day_of_week").count()
        self.storage_manager.save_to_s3(result_df, f"s3a://{Config.SILVER_S3_PATH}/timestamp_countby_dayofweek.parquet")
        return result_df

    def group_and_count_crimes_by_type(self, df: DataFrame) -> DataFrame:
        """
        Group the DataFrame by crime type, count occurrences, and save the result to S3.

        Args:
            df (DataFrame): Input DataFrame containing the data.

        Returns:
            DataFrame: Transformed DataFrame with counts by crime type.
        """
        result_df = df.groupBy('Primary Type').count().orderBy('count', ascending=False)
        self.storage_manager.save_to_s3(result_df, f"s3a://{Config.SILVER_S3_PATH}/group_and_count_crimes_by_type"
                                                   f".parquet")
        return result_df

    def add_and_count_crimes_from_specific_day(self, df: DataFrame, date_str='2018-11-12') -> DataFrame:
        """
        Add crimes from a specific day to the DataFrame, count occurrences, and save the result to S3.

        Args:
            df (DataFrame): Input DataFrame containing the data.
            date_str (str): The specific date to filter and count crimes.

        Returns:
            DataFrame: Transformed DataFrame with crimes from the specific day.
        """
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
                                        f"s3a://{Config.SILVER_S3_PATH}/add_and_count_crimes_from_specific_day.parquet",
                                        'year_month')
        return combined_df
