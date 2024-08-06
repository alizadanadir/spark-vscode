from datetime import timedelta, datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def input_path(date: str, depth: int = 0) -> list:
    base_path = 'sources/events/date={}'
    file_directories = []
    
    for i in range(depth + 1):
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        new_date_obj = date_obj - timedelta(days=i)
        new_date_str = new_date_obj.strftime("%Y-%m-%d")
        file_directories.append(base_path.format(new_date_str))
    
    return file_directories

def define_new_tags(starting_date, depth):
    spark = SparkSession.builder \
        .appName("NewTags") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .master("local[6]") \
        .getOrCreate()

    tags_verified = spark.read.parquet('sources/tags_verified/')

    directories = input_path(starting_date, depth)

    events = spark.read.json(directories[0])
    events = events.select('event.message_from', 'event.message_channel_to', 'event.tags', 'event_type', 'event.datetime')
    schema = events.withColumn("tag", F.explode(events.tags)).drop("tags").schema

    all_events = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

    for directory in directories:
        events = spark.read.json(directory)
        events = events.filter((F.col('event_type') == 'message') & (F.col('event.message_channel_to').isNotNull())) \
            .select('event.message_from', 'event.message_channel_to', 'event.tags', 'event_type', 'event.datetime')
        events = events.withColumn("tag", F.explode(events.tags)).drop("tags")
        all_events = all_events.union(events)
    
    all_events = all_events.join(tags_verified, 'tag', 'leftanti')
    all_events = all_events.groupBy('tag').agg(F.countDistinct('message_from').alias('suggested_count')).where("suggested_count >= 100")

    all_events.write.mode('overwrite').format("parquet").save('output/tags_verified.parquet')

    all_events.show()

if __name__ == '__main__':
    define_new_tags('2022-05-31', 6)