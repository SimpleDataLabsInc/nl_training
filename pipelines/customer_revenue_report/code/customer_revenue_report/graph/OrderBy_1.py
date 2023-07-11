from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_revenue_report.config.ConfigStore import *
from customer_revenue_report.udfs.UDFs import *

def OrderBy_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("total_expenditure").desc())