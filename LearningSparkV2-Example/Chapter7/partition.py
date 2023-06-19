import pyspark.sql.SparkSession

def print_configs(session):
  # get conf
  mconf = session.conf.getAll()
  # print them
  for k in mconf.keys():
    print(f"{k} -> {mconf[k]}\n")

def main():
  # create a session
  spark = SparkSession.builder.master("local[*]").appName("Partitions").getOrCreate()

  print_configs(spark)
  numDF = spark.range(1000 * 1000 * 1000).repartition(16)
  print(f"****** Number of Partiions in DataFrame: {numDF.rdd.getNumPartitions}")
  spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
  print("****** Setting Shuffle Partitions to Default Parallelism")
  print_configs(spark)

if __name__ == "__main__":
  main()