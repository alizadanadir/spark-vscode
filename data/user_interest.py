from datetime import timedelta, datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def input_path(date: str, depth: int = 0) -> list:
    base_path = 'sources/events/date={}'
    file_directories = []
    
    for i in range(depth + 1):
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        new_date_obj = date_obj - timedelta(days=i)
        new_date_str = new_date_obj.strftime("%Y-%m-%d")
        file_directories.append(base_path.format(new_date_str))
    
    return file_directories

def define_top_tags(starting_date, depth):
    spark = SparkSession.builder \
        .appName("UserInterest") \
        .config("spark.executor.memory" , "8g") \
        .config("spark.driver.memory", "4g") \
        .master("local[6]") \
        .getOrCreate()

    directories = input_path('2022-05-31', 6)

    events = spark.read.json(directories[0])
    events = events.select('event.message_from','event.tags', 'event_type', 'event.datetime')
    schema = events.withColumn("tag", F.explode(events.tags)).drop("tags").schema

    all_events = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    for directory in directories:
        events = spark.read.json(directory)
        events = events.filter((F.col('event_type').isNotNull()) & (F.col('event.message_channel_to').isNotNull())) \
            .select('event.message_from', 'event.tags', 'event_type', 'event.datetime')
        events = events.withColumn("tag", F.explode(events.tags)).drop("tags")
        all_events = all_events.union(events)


    events = all_events.groupBy('message_from', 'tag').count()

    window = Window().partitionBy('message_from').orderBy(F.desc('count'), F.asc('tag'))

    dfRowNumb = events.withColumn('rn', F.row_number().over(window))

    df1 = dfRowNumb.filter(F.col('rn') == 1).select(F.col('message_from'), F.col('tag').alias('tag_1'))
    df2 = dfRowNumb.filter(F.col('rn') == 2).select(F.col('message_from'), F.col('tag').alias('tag_2'))
    df3 = dfRowNumb.filter(F.col('rn') == 3).select(F.col('message_from'), F.col('tag').alias('tag_3'))

    users = all_events.select('message_from').drop_duplicates()
    final_df = users \
        .join(df1, on = 'message_from', how = 'left')\
        .join(df2, on = 'message_from', how = 'left')\
        .join(df3, on = 'message_from', how = 'left')

    final_df = final_df.select(F.col('message_from').alias('user'), 'tag_1', 'tag_2', 'tag_3')
    final_df.write.mode('overwrite').format("parquet").save('output/top_tags.parquet')

    final_df.show()

if __name__ == '__main__':
    define_top_tags('2022-05-31', 6)