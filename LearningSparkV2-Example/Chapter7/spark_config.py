import pyspark
from pyspark.sql import SparkSession

def print_configs(spark: SparkSession) -> None:
  """Prints all the Spark configurations."""
  conf = spark.conf.getAll()
  for k in conf.keys():
    print(f"{k} -> {conf[k]}\n")

def main() -> None:
  """Main function."""
  spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", 5) \
    .config("spark.executor.memory", "2g") \
    .master("local[*]") \
    .appName("SparkConfig") \
    .getOrCreate()

  print_configs(spark)
  print(" ****** Setting Shuffle Partitions to Default Parallelism")
  spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
  print_configs(spark)

if __name__ == "__main__":
  main()