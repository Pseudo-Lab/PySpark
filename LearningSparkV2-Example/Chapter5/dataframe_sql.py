# 작성자 : 정현석(6기)

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("sparkSQL") \
        .getOrCreate()

    tripdelaysFilePath = "/data/learning-spark-v2/flights/departuredelays.csv"
    airportsnaFilePath = "/data/learning-spark-v2/flights/airport-codes-na.txt"

    airportsna = spark.read \
        .format("csv") \
        .options(header="true", inferSchema="true", sep="\t") \
        .load(airportsnaFilePath)

    airportsna.createOrReplaceTempView("airports_na")

    departureDelays = spark.read \
        .format("csv") \
        .options(header="true") \
        .load(tripdelaysFilePath)

    departureDelays = departureDelays \
        .withColumn("delay", expr("CAST(delay as INT) as delay")) \
        .withColumn("distance", expr("CAST(distance as INT) as distance"))

    departureDelays.createOrReplaceTempView("departureDelays")

    foo = departureDelays \
        .filter(expr("""
        origin == 'SEA' AND destination == 'SFO' and date like '01010%' and delay > 0"""))
    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports_na LIMIT 10").show()

    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

    spark.sql("SELECT * FROM foo").show()

    bar = departureDelays.union(foo)
    bar.createOrReplaceTempView("bar")

    bar.filter(expr("""
    origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '0101%' AND delay > 0""")).show()

    foo.join(
        airportsna,
        airportsna.IATA == foo.origin) \
        .select("City", "State", "date", "delay", "distance", "destination").show()


    # spark.sql("""
    # DROP TABLE IF EXISTS departureDelaysWindow
    # CREATE TABLE departureDelaysWindow AS
    # SELECT origin, destination, SUM(delay) AS TotalDelays
    # FROM departureDelays
    # WHERE origin IN ('SEA', 'SFO', 'JFK')
    # AND destination In ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
    # GROUP BY origin, destination
    #
    # SELECT * FROM departureDelaysWindow""")

    window = spark.sql("""
                SELECT origin, destination, SUM(delay) AS TotalDelays 
                FROM departureDelays 
                WHERE origin IN ('SEA', 'SFO', 'JFK') 
                AND destination In ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') 
                GROUP BY origin, destination""")
    window.createOrReplaceTempView("departureDelaysWindow")
    spark.sql("SELECT * FROM departureDelaysWindow").show()

    spark.sql("""
    SELECT origin, destination, TotalDelays, rank
    FROM (
        SELECT  origin, destination, TotalDelays,
            dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) AS rank
            FROM departureDelaysWindow) t
    WHERE rank <= 3""").show()