import mlflow.sklearn
import pandas as pd
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("App").getOrCreate()

def predict(iterator):
    run_id = "ad6d22f420884be7bb7bd00bacbc60b7"
    model_path = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_path)
    for features in iterator:
        yield pd.DataFrame(model.predict(features))

inputDF = spark.read.parquet("../../LearningSparkV2/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet")

inputDF.mapInPandas(predict, "prediction double").show(3)