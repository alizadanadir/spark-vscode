import pyspark 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder\
    .master('local[4]')\
    .appName('Builtin functions')\
    .getOrCreate()

df = spark.read.json('/workspace/datafiles/date=2022-05-31/')

df_new = df.select('event.datetime', 'event.user','event_type')\
    .withColumn('seconds', F.second(F.col('datetime')))\
    .withColumn('minutes', F.minute(F.col('datetime')))\
    .withColumn('hours', F.hour(F.col('datetime')))

df_ordered = df_new.orderBy(F.col('datetime').desc()).show(10, False)
