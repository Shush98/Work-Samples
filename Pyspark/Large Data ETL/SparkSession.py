import os
import time
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.regression import GBTRegressor
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, exp



spark = SparkSession.builder \
    .appName('Optimized Cleaning and Training') \
    .config('spark.executor.instances', '3') \
    .config('spark.executor.memory', '12g') \
    .config('spark.executor.cores', '5') \
    .config('spark.driver.memory', '12g') \
    .config('spark.sql.shuffle.partitions', '50') \
    .getOrCreate()


# cleaning_start_time = time.time()
# df = spark.read.parquet('/home/ghosh.shu/EECE/flightData.parquet')
print("Session Started-------------------------------")
spark.stop()