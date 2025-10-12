from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("WeatherDataStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("file_id", LongType(), False),
    StructField("city", StringType(), False),
    StructField("temperature", LongType(), False),
    StructField("humidity", LongType(),False),
    StructField("pressure", LongType(), False),
    StructField("weather_condition", StringType(), False),
    StructField("wind_speed", LongType(), False),
    StructField("timestamp", StringType(), False)
])

streaming_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("/tmp/weather_data")

aggregated_df = streaming_df.groupBy("city").agg(
    avg("temperature").alias("avg_temperature"),
    min("temperature").alias("min_temperature"), 
    max("temperature").alias("max_temperature"),
    avg("humidity").alias("avg_humidity"),
    count("*").alias("measurement_count"),
    collect_set("weather_condition").alias("weather_conditions")
)

query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming started... Waiting for data...")
query.awaitTermination()