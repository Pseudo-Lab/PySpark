from pyspark.sql import SparkSession
import pyspark

if __name__ == "__main__":

    spark = SparkSession.builder \
        .config("spark.jars", "./jars/postgresql-42.3.7.jar") \
        .appName("postgresql") \
        .getOrCreate()

    jdbcDF = spark.read.format("jdbc") \
        .options(
        url='jdbc:postgresql://localhost:5432/learningsparkdb',  # jdbc:postgresql://<host>:<port>/<database>
        dbtable='Employee',
        user='postgres',
        password='0000',
        driver='org.postgresql.Driver') \
        .load()

    jdbcDF.printSchema()
    jdbcDF.createOrReplaceTempView("tbl")

    spark.sql('select * from tbl').show()


