import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
                    
events = spark.read.json('/workspace/datafiles/date=2022-05-25/')

events_grouped = events.filter(F.col('event_type') == 'reaction').groupBy(F.col('event.reaction_from')).count()
events_grouped.select(F.max('count').alias('max_count')).show()