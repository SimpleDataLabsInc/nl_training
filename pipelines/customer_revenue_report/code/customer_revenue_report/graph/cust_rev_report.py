from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_revenue_report.config.ConfigStore import *
from customer_revenue_report.udfs.UDFs import *

def cust_rev_report(spark: SparkSession, in0: DataFrame):
    in0.write.format("parquet").mode("overwrite").save("dbfs:/Prophecy/kyakkala@prophecy.io/cust_report")
