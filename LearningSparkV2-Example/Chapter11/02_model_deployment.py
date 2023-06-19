import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("App").getOrCreate()

run_id = "ad6d22f420884be7bb7bd00bacbc60b7"
pipelineModel = mlflow.spark.load_model(f"runs:/{run_id}/model")

# 배치
inputDF = spark.read.parquet("../../LearningSparkV2/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet")

predDf = pipelineModel.transform(inputDF)

# 스트리밍
repartitionedPath = "../../LearningSparkV2/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean-100p.parquet"
schema = spark.read.parquet(repartitionedPath).schema

streamingData = (spark
                 .readStream
                 .schema(schema)
                 .option("maxFilesPerTrigger", 1)
                 .parquet(repartitionedPath))

streamPred = pipelineModel.transform(streamingData)
