hostAddr = "<ip address>"
keyspaceName = "<keyspace>"
tableName = "<tableName>"

spark.conf.set("spark.cassandra.connection.host", hostAddr)

def writeCountsToCassandra(updatedCountsDF, batchId):
    (updatedCountsDF
     .write
     .format("org.apache.spark.sql.cassandra")
     .mode("append")
     .options(table=tableName, keyspace=keyspaceName)
     .save())

streamingQuery = (counts
                  .writeStream
                  .foreachBatch(writeCountsToCassandra)
                  .outputMode("update")
                  .option("checkpointLocation", checkpointDir)
                  .start())