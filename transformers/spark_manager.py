from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, lit, year, month, concat_ws
from config.config import Config
from decorators.decorators import log_decorator, timing_decorator


class SparkManager:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .master(Config.SPARK_URL) \
            .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
            .config("spark.executor.cores", Config.SPARK_EXECUTOR_CORES) \
            .config("spark.cores.max", Config.SPARK_CORES_MAX) \
            .config("spark.local.dir", "C:/Users/RafaelVera-Marañón/tmp/spark-temp") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", Config.AWS_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", Config.AWS_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,"
                                           "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a'
                                                                    '.SimpleAWSCredentialsProvider') \
            .config("hadoop.home.dir", "C:/winutils") \
            .getOrCreate()

    def get_spark_session(self):
        return self.spark

    @log_decorator
    @timing_decorator
    def timestamp_countby_dayofweek(self, df: DataFrame):
        total_count = df.count()
        result_df = df.withColumn("timestamp", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")) \
            .withColumn("day_of_week", date_format(col("timestamp"), "E")) \
            .groupBy("day_of_week").count() \
            .withColumn("total", lit(int(total_count)))

        print("Schema and Data Count:")
        result_df.printSchema()
        print("Total count:", total_count)

        self.save_to_s3(result_df, "s3a://spark-etl-rvm/Silver/timestamp_countby_dayofweek.parquet")
        return result_df

    @log_decorator
    @timing_decorator
    def group_and_count_crimes_by_type(self, df):
        result_df = df.groupBy('Primary Type').count().orderBy('count', ascending=False)
        result_df.show(10)
        return result_df

    @log_decorator
    @timing_decorator
    def add_and_count_crimes_from_specific_day(self, df, date_str='2018-11-12'):

        df = df.withColumn('Date', to_timestamp(col('Date'), 'yyyy-MM-dd HH:mm:ss'))

        one_day = df.filter(col('Date') == lit(date_str))
        print(f"Count of crimes on {date_str}: {one_day.count()}")

        combined_df = df.union(one_day).orderBy('Date', ascending=False)
        combined_df.show(5)

        combined_df = combined_df.withColumn('year_month', concat_ws('-', year(col('Date')), month(col('Date'))))

        self.save_to_s3(combined_df, "s3a://spark-etl-rvm/Silver/add_and_count_crimes_from_specific_day.parquet",
                        'year_month')
        return combined_df

    @log_decorator
    @timing_decorator
    def group_and_count_crimes_by_type(self, df):
        result_df = df.groupBy('Primary Type').count().orderBy('count', ascending=False)
        result_df.show(10)

        self.save_to_s3(result_df, "s3a://spark-etl-rvm/Silver/group_and_count_crimes_by_type.parquet")
        return result_df

    @log_decorator
    @timing_decorator
    def save_to_s3(self, df: DataFrame, path: str, partition_col=None):
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", Config.AWS_ACCESS_KEY)
        hadoop_conf.set("fs.s3a.secret.key", Config.AWS_SECRET_KEY)
        hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.multipart.size", "104857600")
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

        if partition_col:
            df.write.partitionBy(partition_col).mode('overwrite').parquet(path)
        else:
            df.write.mode('overwrite').parquet(path)
        print(f"DataFrame saved to {path}")

    def stop_spark(self):
        self.spark.catalog.clearCache()
        self.spark.stop()
