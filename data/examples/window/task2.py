import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder\
    .master('local[6]')\
    .appName('Window functions')\
    .getOrCreate()

events = spark.read.json('/workspace/datafiles/date=2022-05-25/')

window = Window().partitionBy('event.message_from').orderBy(F.desc('event.datetime'))

dfWithLag = events.withColumn('lag_7', F.lag('event.message_to', 7).over(window))

dfWithLag.select('event.message_from','lag_7')\
    .filter(F.col('lag_7').isNotNull())\
    .orderBy(F.col('message_from').desc())\
    .show(10, False)