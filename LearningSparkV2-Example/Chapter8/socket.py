from pyspark.sql import SparkSession

spark = SparkSession.builder \
                .master("local") \
                .appName("mysql") \
                .getOrCreate()

lines = (spark
      .readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load())