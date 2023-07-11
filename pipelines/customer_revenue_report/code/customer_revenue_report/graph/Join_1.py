from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_revenue_report.config.ConfigStore import *
from customer_revenue_report.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.customer_id") == col("in1.customer_id")), "inner")\
        .select(col("in0.customer_id").alias("customer_id"), col("in1.account_lifetime").alias("account_lifetime"), col("in0.total_expenditure").alias("total_expenditure"), col("in0.total_order").alias("total_order"), col("in1.full_name").alias("full_name"))
