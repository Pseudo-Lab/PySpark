import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)


    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())

    mnm_file = sys.argv[1]

    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))
    mnm_df.show(n=5, truncate=False)


    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .agg(count("Count")
                    .alias("Total"))
                    .orderBy("Total", ascending=False))

    count_mnm_df.explain(True)
    spark.stop()
