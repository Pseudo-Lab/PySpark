from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("App").getOrCreate()

filePath = "../../LearningSparkV2/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet"
airbnbDF = spark.read.parquet(filePath)
(trainDF, testDF) = airbnbDF.randomSplit([.8, .2], seed=42)

categoricalCols = [field for (field, dataType) in trainDF.dtypes
                   if dataType == "string"]
indexOutputCols = [x + "Index" for x in categoricalCols]
stringIndexer = StringIndexer(inputCols=categoricalCols,
                              outputCols=indexOutputCols,
                              handleInvalid="skip")

numericCols = [field for (field, dataType) in trainDF.dtypes
               if ((dataType == "double") & (field != "price"))]
assemblerInputs = indexOutputCols + numericCols
vecAssembler = VectorAssembler(inputCols=assemblerInputs,
                               outputCol="features")

rf = RandomForestRegressor(labelCol="price", maxBins=40, maxDepth=5,
                           numTrees=100, seed=42)

pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])

import mlflow
import mlflow.spark
import pandas as pd
from mlflow.tracking import MlflowClient

with mlflow.start_run(run_name="random-forest") as run:
    mlflow.log_param("num_trees", rf.getNumTrees())
    mlflow.log_param("max_depth", rf.getMaxDepth())

    pipelineModel = pipeline.fit(trainDF)
    mlflow.spark.log_model(pipelineModel, "model")

    # Log metrics: RMSE and R2
    predDF = pipelineModel.transform(testDF)
    regressionEvaluator = RegressionEvaluator(predictionCol="prediction",
                                              labelCol="price")
    rmse = regressionEvaluator.setMetricName("rmse").evaluate(predDF)
    r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    mlflow.log_metrics({"rmse": rmse, "r2": r2})

    # Log artifact: Feature Importance Scores
    rfModel = pipelineModel.stages[-1]
    pandasDF = (pd.DataFrame(list(zip(vecAssembler.getInputCols(),
                                      rfModel.featureImportances)),
                             columns=["feature", "importance"])
                .sort_values(by="importance", ascending=False))
    # First write to local filesystem, then tell MLflow where to find that file
    pandasDF.to_csv("/tmp/feature-importance.csv", index=False)
    mlflow.log_artifact("/tmp/feature-importance.csv")

    client = MlflowClient()
    runs = client.search_runs(run.info.experiment_id,
                              order_by=["attributes.start_time desc"],
                              max_results=1)
    run_id = runs[0].info.run_id
    print(runs[0].data.metrics)

# mlflow.run(
#     "https://github.com/databricks/LearningSparkV2/#mlflow-project-example",
#     parameters={"max_depth": 5, "num_trees": 100}
# )
