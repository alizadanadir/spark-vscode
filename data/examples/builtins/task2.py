import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
                    
events = spark.read.json('/workspace/datafiles/date=2022-05-31/')

res = events.filter(F.col('event.message_to').isNotNull()).count() 
print(res)