import pyspark.sql.SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
import random

def benchmark(name, f):
  start_time = System.nanoTime()
  f()
  end_time = System.nanoTime()
  print(f"Time taken in {name}: {(end_time - start_time).to_double() / 1000000000} seconds")

def main():
  spark = SparkSession.builder.appName("SortMergeJoin").getOrCreate()

  # initialize states and items purchased
  states = {i: f"AZ" for i in range(0, 5)}
  items = {i: f"SKU-{i}" for i in range(0, 5)}

  # create dataframes
  usersDF = [(i, f"user_{i}", f"user_{i}@databricks.com", states[random.randint(0, 4)]) for i in range(0, 100000)]
  ordersDF = [(i, i, random.randint(0, 100000), 10 * i * 0.2, states[random.randint(0, 4)], items[random.randint(0, 4)]) for i in range(0, 100000)]

  # show the DataFrames
  usersDF.show(10)
  ordersDF.show(10)

  # do a Join
  usersOrdersDF = ordersDF.join(usersDF, F.col("users_id") == F.col("uid"))
  usersOrdersDF.show(10, False)
  usersOrdersDF.cache()
  usersOrdersDF.explain()

  # uncomment to view the SparkUI otherwise the program terminates and shutdowsn the UI
  # Thread.sleep(200000000)

if __name__ == "__main__":
  main()