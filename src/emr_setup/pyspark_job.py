"""This module performs data transformation and joining of crime and police
data using PySpark.

Classes: DataLoader: Handles data loading operations. DataCleaner: Handles
data cleaning operations. DataPipeline: Abstract base class for data
transformation operations. CrimePoliceDataTransformer: Concrete class for
transforming and joining crime and police data.

Functions:
    main: Entry point for the script.
"""

from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp, desc


class DataLoader:
    """
    DataLoader class to handle data loading operations.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_csv(self, path: str) -> DataFrame:
        """
        Load a CSV file into a DataFrame.

        Args:
            path (str): Path to the CSV file.

        Returns:
            DataFrame: Loaded DataFrame.
        """
        return self.spark.read.option("header", "true").csv(path)


class DataCleaner:
    """
    DataCleaner class to handle data cleaning operations.
    """
    def __init__(self):
        """
        Initialize the DataCleaner. This constructor is currently empty but
        can be expanded in the future if any initialization is needed.
        """

    def clean_crime_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the crime data.

        Args:
            df (DataFrame): DataFrame containing crime data.

        Returns:
            DataFrame: Cleaned DataFrame.
        """
        df = df.withColumn("Date",
                           to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
        df = df.filter(col("District").isNotNull())
        return df

    def clean_police_data(self, df: DataFrame) -> DataFrame:
        """
        Clean the police station data.

        Args:
            df (DataFrame): DataFrame containing police station data.

        Returns:
            DataFrame: Cleaned DataFrame.
        """
        df = df.withColumn("district", col("district").cast("integer"))
        return df


class DataPipeline(ABC):
    """
    Abstract base class for data transformation operations.
    """

    @abstractmethod
    def load(self) -> None:
        """
        Load data from sources.
        """

    @abstractmethod
    def transform(self) -> DataFrame:
        """
        Transform the loaded data.

        Returns:
            DataFrame: Transformed DataFrame.
        """

    @abstractmethod
    def write(self) -> None:
        """
        Write the transformed data to the destination.

        Args:
            df (DataFrame): Transformed DataFrame.
        """


class CrimePoliceDataTransformer(DataPipeline):
    """
    Concrete class for transforming and joining crime and police data.
    """

    def __init__(self, spark: SparkSession, crime_data_path: str,
                 police_data_path: str) -> None:
        self.spark = spark
        self.data_loader = DataLoader(spark)
        self.data_cleaner = DataCleaner()
        self.crime_data_path = crime_data_path
        self.police_data_path = police_data_path
        self.df_crime: Optional[DataFrame] = None
        self.df_police: Optional[DataFrame] = None
        self.df_transformed: Optional[DataFrame] = None

    def load(self) -> None:
        """
        Load data from CSV files.
        """
        self.df_crime = self.data_loader.load_csv(self.crime_data_path)
        self.df_police = self.data_loader.load_csv(self.police_data_path)

    def transform(self) -> DataFrame:
        """
        Transform and join the loaded data.

        Returns:
            DataFrame: Transformed DataFrame.
        """
        if self.df_crime is None or self.df_police is None:
            raise ValueError("Dataframes not loaded properly")

        df_crime_cleaned = self.data_cleaner.clean_crime_data(self.df_crime)
        df_police_cleaned = self.data_cleaner.clean_police_data(self.df_police)

        self.df_transformed = df_crime_cleaned.join(
            df_police_cleaned,
            df_crime_cleaned["District"] == df_police_cleaned["district"],
            "inner"
        )

        self.df_transformed = (self.df_transformed.orderBy(desc("Date"))
                               .limit(1000))
        return self.df_transformed

    def write(self) -> None:
        """
        Write the transformed data to S3.

        """
        if self.df_transformed is None:
            raise ValueError("Transformed Dataframe is not available")

        self.df_transformed.write.mode("overwrite").parquet(
            "s3://spark-etl-rvm/Silver-emr/joined_data")


def main() -> None:
    """
    Main function to run the data pipeline.
    """
    spark = SparkSession.builder.appName("EMRJob").getOrCreate()

    crime_data_path = "s3://spark-etl-rvm/Bronze/Chicago_crime_data.csv"
    police_data_path = "s3://spark-etl-rvm/Bronze/police-station.csv"

    transformer = CrimePoliceDataTransformer(spark, crime_data_path,
                                             police_data_path)

    transformer.load()

    transformer.transform()

    transformer.write()

    spark.stop()


if __name__ == "__main__":
    main()

# use mypy, flake8, pylint
