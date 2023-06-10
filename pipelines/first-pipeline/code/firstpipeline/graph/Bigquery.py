from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from firstpipeline.config.ConfigStore import *
from firstpipeline.udfs.UDFs import *

def Bigquery(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("text")\
        .schema(StructType([StructField("value", StringType(), True)]))\
        .text("dbfs:/data/targets/customer_info.json/", wholetext = False, lineSep = None)
