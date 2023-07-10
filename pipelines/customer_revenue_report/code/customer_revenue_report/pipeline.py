from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from customer_revenue_report.config.ConfigStore import *
from customer_revenue_report.udfs.UDFs import *
from prophecy.utils import *
from customer_revenue_report.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customer_ds = customer_ds(spark)
    df_orders_ds = orders_ds(spark)
    df_Join_1 = Join_1(spark, df_customer_ds, df_orders_ds)
    df_Reformat_1 = Reformat_1(spark, df_Join_1)
    df_Aggregate_1 = Aggregate_1(spark, df_Reformat_1)
    df_Repartition_1 = Repartition_1(spark, df_Aggregate_1)
    df_OrderBy_1 = OrderBy_1(spark, df_Repartition_1)
    cust_rev_report(spark, df_OrderBy_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/customer_revenue_report")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/customer_revenue_report")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
