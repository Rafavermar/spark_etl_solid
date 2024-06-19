from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, desc
from abc import ABC, abstractmethod


class DataLoader:
    """
    DataLoader class to handle data loading operations.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_csv(self, path: str):
        return self.spark.read.option("header", "true").csv(path)


class DataCleaner:
    """
    DataCleaner class to handle data cleaning operations.
    """

    def clean_crime_data(self, df):
        df = df.withColumn("Date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
        # Filter out rows with null district values
        df = df.filter(col("District").isNotNull())
        return df

    def clean_police_data(self, df):
        df = df.withColumn("district", col("district").cast("integer"))
        return df


class DataTransformer(ABC):
    """
    Abstract base class for data transformation operations.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_loader = DataLoader(spark)
        self.data_cleaner = DataCleaner()

    @abstractmethod
    def transform(self):
        pass


class CrimePoliceDataTransformer(DataTransformer):
    """
    Concrete class for transforming and joining crime and police data.
    """

    def __init__(self, spark: SparkSession, crime_data_path: str, police_data_path: str):
        super().__init__(spark)
        self.crime_data_path = crime_data_path
        self.police_data_path = police_data_path

    def transform(self):
        # Load data
        df_crime = self.data_loader.load_csv(self.crime_data_path)
        df_police = self.data_loader.load_csv(self.police_data_path)

        # Clean data
        df_crime_cleaned = self.data_cleaner.clean_crime_data(df_crime)
        df_police_cleaned = self.data_cleaner.clean_police_data(df_police)

        # Join data
        df_joined = df_crime_cleaned.join(df_police_cleaned,
                                          df_crime_cleaned["District"] == df_police_cleaned["district"], "inner")

        # Select most recent 1000 records
        df_limited = df_joined.orderBy(desc("Date")).limit(1000)

        # Save result to S3
        df_limited.write.mode("overwrite").parquet("s3://spark-etl-rvm/Silver-emr/joined_data")

        return df_limited


def main():
    spark = SparkSession.builder.appName("EMRJob").getOrCreate()

    crime_data_path = "s3://spark-etl-rvm/Bronze/Chicago_crime_data.csv"
    police_data_path = "s3://spark-etl-rvm/Bronze/police-station.csv"

    transformer = CrimePoliceDataTransformer(spark, crime_data_path, police_data_path)
    transformer.transform()

    spark.stop()


if __name__ == "__main__":
    main()
