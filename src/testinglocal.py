from pyspark.sql import SparkSession


def test_write_local():
    spark = SparkSession.builder \
        .appName("Test Local Write") \
        .getOrCreate()

    # Crear un DataFrame simple
    data = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
    columns = ["Language", "Users"]
    df = spark.createDataFrame(data, columns)

    # Intentar escribir en el sistema local
    df.write.mode('overwrite').parquet("C:\\Users\\RafaelVera-Marañón"
                                       "\\PycharmProjects\\solid_etl_spark\\src\\data\\output")

    spark.stop()


if __name__ == "__main__":
    test_write_local()
