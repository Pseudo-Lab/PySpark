from pyspark.sql import SparkSession
# import mysql.connector

if __name__ == "__main__":

    # spark 인스턴스 생성
    spark = SparkSession.builder \
        .config("spark.jars", "./jar/mysql-connector-j-8.0.32.jar") \
        .master("local") \
        .appName("mysql") \
        .getOrCreate()

    # 파일 불러오기
    df = spark.read.format("jdbc") \ 
        .options(
        url="jdbc:mysql://localhost:3306/learningsparkdb",
        driver="com.mysql.jdbc.Driver",
        dbtable="Department",
        user="root",
        password="0000").load()

    df.printSchema()
    df.show()
