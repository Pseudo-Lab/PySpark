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
  spark = SparkSession.builder.appName("SortMergeJoinBucketed").getOrCreate()

  # initialize states and items purchased
  states = {i: f"AZ" for i in range(0, 5)}
  items = {i: f"SKU-{i}" for i in range(0, 5)}

  # create dataframes
  usersDF = [(i, f"user_{i}", f"user_{i}@databricks.com", states[random.randint(0, 4)]) for i in range(0, 100000)]
  ordersDF = [(i, i, random.randint(0, 100000), 10 * i * 0.2, states[random.randint(0, 4)], items[random.randint(0, 4)]) for i in range(0, 100000)]

  # cache them on Disk only so we can see the difference in size in the storage UI
  usersDF.persist(StorageLevel.DISK_ONLY)
  ordersDF.persist(StorageLevel.DISK_ONLY)

  # let's create five buckets, each DataFrame for their respective columns
  # create bucket and table for uid
  usersDF.orderBy(F.asc("uid")).write.format("parquet").mode(SaveMode.Overwrite).bucketBy(8, "uid").saveAsTable("UsersTbl")
  # create bucket and table for users_id
  ordersDF.orderBy(F.asc("users_id")).write.format("parquet").bucketBy(8, "users_id").mode(SaveMode.Overwrite).saveAsTable("OrdersTbl")

  # cache tables in memory so that we can see the difference in size in the storage UI
  spark.sql("CACHE TABLE UsersTbl")
  spark.sql("CACHE TABLE OrdersTbl")
  spark.sql("SELECT * from OrdersTbl LIMIT 20")

  # read data back in
  usersBucketDF = spark.table("UsersTbl")
  ordersBucketDF = spark.table("OrdersTbl")

  # Now do the join on the bucketed DataFrames
  joinUsersOrdersBucketDF = ordersBucketDF.join(usersBucketDF, F.col("users_id") == F.col("uid"))
  joinUsersOrdersBucketDF.show(false)
  joinUsersOrdersBucketDF.explain()

  # uncomment to view the SparkUI otherwise the program terminates and shutdowsn the UI
  # Thread.sleep(200000000)

if __name__ == "__main__":
  main()