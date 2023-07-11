from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_revenue_report.config.ConfigStore import *
from customer_revenue_report.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id"), 
        concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"), 
        datediff(current_date(), col("account_open_date")).alias("account_lifetime")
    )
