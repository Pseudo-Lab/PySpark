from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
schema = StructType([StructField("celsius",ArrayType(IntegerType()))])

t_list = [[35,36,32,30,40,42,38]], [[31,32,34,55,56]]
t_c = spark.createDataFrame(t_list,schema)
t_c.createOrReplaceTempView("tC")

spark.sql("""
                SELECT celsius, reduce(celsius, 
                                        0,
                                        (t,acc) -> t + acc,
                                        acc -> (acc div size(celsius)* 9 div 5) + 32 
                                        ) AS avgFahrenheit
                FROM tC
                """).show()