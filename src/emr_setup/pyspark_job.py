from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("EMRJob").getOrCreate()

    df_crime = spark.read.option("header", "true").csv("s3://spark-etl-rvm/Bronze/Chicago_crime_data.csv")
    df_police = spark.read.option("header", "true").csv("s3://spark-etl-rvm/Bronze/police-station.csv")

    df_joined = df_crime.join(df_police, df_crime["District"] == df_police["District"], "inner")

    df_joined.limit(1000).write.mode("overwrite").parquet("s3://spark-etl-rvm/Silver-emr/joined_data")

    spark.stop()


if __name__ == "__main__":
    main()
