# 코드 실행 환경 : https://hub.docker.com/r/ingu627/hadoop
# numpy, pandas 설치 필요
# by 가짜연구소 6기 : "Chapter 10. MLlib을 사용한 머신러닝" 스터디

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, RFormula
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.ml.regression import DecisionTreeRegressor, RandomForestRegressor
import pandas as pd

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName('pratice_ml') \
        .config('spark.some.config.option', 'some-value') \
        .getOrCreate()

    filePath = "/root/data/learning-spark-v2/sf-airbnb/sf-airbnb-clean.parquet/"
    airbnbDF = spark.read.parquet(filePath)
    airbnbDF.select("neighbourhood_cleansed", "room_type", "bedrooms",
                    "bathrooms", "number_of_reviews", "price") \
        .show(5)

    trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=42)
    print(
        f"There are {trainDF.count()} rows in the training set, and {testDF.count()} in the test set")

    # 변환기를 사용하여 기능 준비
    vecAssembler = VectorAssembler(
        inputCols=['bedrooms'], outputCol="features")
    vecTrainDF = vecAssembler.transform(trainDF)
    vecTrainDF.select("bedrooms", "features", "price").show(10)

    # 추정기를 사용하여 모델 구축
    lr = LinearRegression(featuresCol="features", labelCol="price")
    lrModel = lr.fit(vecTrainDF)

    m = round(lrModel.coefficients[0], 2)
    b = round(lrModel.intercept, 2)
    print(
        f"The formula for the linear regression line is price = {m}*bedrooms + {b}")

    # 파이프라인 생성
    categoricalCols = [field for (field, dataType)
                       in trainDF.dtypes if dataType == "string"]
    indexOutputCols = [x + "Index" for x in categoricalCols]
    oheOutputCols = [x + "OHE" for x in categoricalCols]

    stringIndexer = StringIndexer(
        inputCols=categoricalCols, outputCols=indexOutputCols, handleInvalid="skip")
    oheEncoder = OneHotEncoder(
        inputCols=indexOutputCols, outputCols=oheOutputCols)

    numericCols = [field for (field, dataType) in trainDF.dtypes if (
        dataType == "double") and (field != "price")]
    assemblerInputs = oheOutputCols + numericCols
    vecAssembler = VectorAssembler(
        inputCols=assemblerInputs, outputCol="features")

    # pipeline = Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])
    # pipelineModel = pipeline.fit(trainDF)

    # predDF = pipelineModel.transform(testDF)
    # predDF.select("bedrooms", "features", "price", "prediction").show(10)

    rFormula = RFormula(formula="price ~ .",
                        featuresCol="features",
                        labelCol="price",
                        handleInvalid="skip")

    lr = LinearRegression(labelCol="price", featuresCol="features")
    pipeline = Pipeline(stages=[stringIndexer, oheEncoder, vecAssembler, lr])
    # RFormula 사용할 때
    # pipeline = Pipeline(stages =[rRormula, lr])

    pipelineModel = pipeline.fit(trainDF)
    predDF = pipelineModel.transform(testDF)
    predDF.select("features", "price", "prediction").show(5)

    # 모델 평가
    regressionEvaluator = RegressionEvaluator(
        predictionCol="prediction",
        labelCol="price",
        metricName="rmse"
    )
    rmse = regressionEvaluator.evaluate(predDF)
    print(f"RMSE is {rmse:.1f}")

    r2 = regressionEvaluator.setMetricName("r2").evaluate(predDF)
    print(f"R2 is {r2}")

    # 모델 저장 및 로드
    # pipelinePath = "/tmp/lr-pipeline-model"
    # pipelineModel.write().overwrite().save(pipelinePath)

    # savedPipelineModel = PipelineModel.load(pipelinePath)

    # 하이퍼파라미터 튜닝
    dt = DecisionTreeRegressor(labelCol="price")
    numericCols = [field for (field, dataType) in trainDF.dtypes
                   if ((dataType == "double") & (field != "price"))]

    assemblerInputs = indexOutputCols + numericCols
    vecAssembler = VectorAssembler(
        inputCols=assemblerInputs, outputCol="features")

    stages = [stringIndexer, vecAssembler, dt]
    pipeline = Pipeline(stages=stages)
    dt.setMaxBins(40)
    pipelineModel = pipeline.fit(trainDF)

    dtModel = pipelineModel.stages[-1]
    print(dtModel.toDebugString)

    # 모델에서 기능 중요도 점수 추출
    featureImp = pd.DataFrame(
        list(zip(vecAssembler.getInputCols(), dtModel.featureImportances)),
        columns=["feature", "importance"]
    )
    print(featureImp.sort_values(by="importance", ascending=False))

    # 랜덤 포레스트
    rf = RandomForestRegressor(labelCol="price", maxBins=40, seed=42)
    pipeline = Pipeline(stages=[stringIndexer, vecAssembler, rf])

    paramGrid = (ParamGridBuilder()
                 .addGrid(rf.maxDepth, [2, 4, 6])
                 .addGrid(rf.numTrees, [10, 100])
                 .build())

    evaluator = RegressionEvaluator(labelCol="price",
                                    predictionCol="prediction",
                                    metricName="rmse")

    cv = CrossValidator(estimator=pipeline,
                        evaluator=evaluator,
                        estimatorParamMaps=paramGrid,
                        numFolds=3,
                        seed=42)
    cvModel = cv.fit(trainDF)

    print(list(zip(cvModel.getEstimatorParamMaps(), cvModel.avgMetrics)))
