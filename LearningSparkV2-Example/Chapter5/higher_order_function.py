from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Higher-Order-Funciton") \
        .getOrCreate()

    schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

    t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
    t_c = spark.createDataFrame(t_list, schema)
    t_c.createOrReplaceTempView("tC")

    # transform(array<T> , function<T, U>) : array<U>
    spark.sql("""
    SELECT celsius, transform(celsius, t -> (((t*9) div 5)+32)) AS fahrenheit
    FROM tC
    """).show()

    # filter(array<T> , function<T, Boolean>) : array<T>
    spark.sql("""
    SELECT celsius, filter(celsius, t -> t > 38) AS high
    FROM tC
    """).show()

    # exists(array<T> , function<T, V,Boolean>) : Boolean
    spark.sql("""
    SELECT celsius, exists(celsius, t -> t = 38) AS threshold
    FROM tC
    """).show()

    # reduce(array<T> , B, function<B,T, B,>, function<B,R>)
    # spark.sql("""
    # SELECT celsius, reduce(celsius,
    #                         0,
    #                         (t,acc) -> t + acc,
    #                         acc -> (acc div size(celsius)* 9 div 5) + 32
    #                         ) AS avgFahrenheit
    # FROM tC
    # """).show()
