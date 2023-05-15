from pyspark.sql import SparkSession
import random
from pyspark.sql.functions import *

if __name__ == "__main__":

        spark = SparkSession.builder \
                .config("spark.jars", "./jar/mysql-connector-j-8.0.32.jar") \
                .master("local") \
                .appName("mysql") \
                .getOrCreate()

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

        states = {0: "AZ", 1: "CO", 2: "CA", 3: "TX", 4: "NY", 5: "MI"}
        items = {0: "SKU-0", 1: "SKU-1", 2: "SKU-2", 3: "SKU-3", 4: "SKU-4",
        5: "SKU-5"}
        rnd = random.Random(42)

        usersDF = spark.createDataFrame([(id, f"user_{id}", f"user_{id}@databricks.com", states[rnd.randint(0, 4)]) for id in range(1000001)], ["uid", "login", "email", "user_state"])
        ordersDF = spark.createDataFrame([(r, r, rnd.randint(0, 9999), 10 * r* 0.2,
        states[rnd.randint(0, 4)], items[rnd.randint(0, 4)]) for r in range(1000001)], ["transaction_id", "quantity", "users_id", "amount", "state", "items"])

        usersOrdersDF = ordersDF.join(usersDF, ordersDF.users_id == usersDF.uid) # Show the joined results
        usersOrdersDF.show()

        usersOrdersDF.explain()

        # (df.write.format("parquet")
        #     .mode("overwrite")
        #     .option("compression", "snappy")
        #     .save("/tmp/data/parquet/df_parquet"))

        usersDF.orderBy(asc("uid")) \
                .write.format("parquet") \
                .bucketBy(8, "uid") \
                .mode("overwrite") \
                .saveAsTable("UsersTbl")

        ordersDF.orderBy(asc("users_id")) \
                .write.format("parquet") \
                .bucketBy(8, "users_id") \
                .mode("overwrite") \
                .saveAsTable("OrdersTbl")


        spark.sql("CACHE TABLE UsersTbl")
        spark.sql("CACHE TABLE OrdersTbl")

        usersBucketDF = spark.table("UsersTbl")
        ordersBucketDF = spark.table("OrdersTbl")


        joinUsersOrdersBucketDF = ordersBucketDF .join(usersBucketDF, ordersBucketDF.users_id == usersBucketDF.uid)
        joinUsersOrdersBucketDF.show()

        joinUsersOrdersBucketDF.explain()