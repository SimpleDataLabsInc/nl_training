from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_revenue_report.config.ConfigStore import *
from customer_revenue_report.udfs.UDFs import *

def calc_expenditure(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("customer_id"))

    return df1.agg(sum(col("amount")).alias("total_expenditure"), count(col("order_id")).alias("total_order"))