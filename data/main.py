import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
events = spark.read.json("datafiles/date=2022-05-31")
events_curr_day = events.withColumn('current_date',F.current_date())

events_diff = events_curr_day\
.withColumn('date', F.lit('2022-05-31'))\
.withColumn('diff',F.datediff(F.col('current_date'),F.col('date'))) 

events_diff.select('event_type', 'current_date', 'date','diff').show(10, False)