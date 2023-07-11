from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from test.config.ConfigStore import *
from test.udfs.UDFs import *

def revenue_report(spark: SparkSession) -> DataFrame:
    return spark.read.format("parquet").load("dbfs:/Prophecy/kyakkala@prophecy.io/revenue_report")
