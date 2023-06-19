# In Python
from sklearn.utils import parallel_backend
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
import pandas as pd
from joblibspark import register_spark
register_spark() # Register Spark backend

df = pd.read_csv("../../LearningSparkV2/databricks-datasets/learning-spark-v2/sf-airbnb/sf-airbnb-numeric.csv")
X_train, X_test, y_train, y_test = train_test_split(df.drop(["price"], axis=1),
df[["price"]].values.ravel(), random_state=42)
rf = RandomForestRegressor(random_state=42)
param_grid = {"max_depth": [2, 5, 10], "n_estimators": [20, 50, 100]}
gscv = GridSearchCV(rf, param_grid, cv=3)

with parallel_backend("spark", n_jobs=3):
    gscv.fit(X_train, y_train)
print(gscv.cv_results_)