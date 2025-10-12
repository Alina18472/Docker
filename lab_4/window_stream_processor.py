from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("WindowedWeatherStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("file_id", LongType(), False),
    StructField("city", StringType(), False),
    StructField("temperature", LongType(), False),
    StructField("humidity", LongType(), False),
    StructField("pressure", LongType(),False),
    StructField("weather_condition", StringType(), False),
    StructField("wind_speed", LongType(), False),
    StructField("timestamp", StringType(), False)
])

streaming_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("/tmp/weather_data")

df_with_timestamp = streaming_df.withColumn(
    "processing_time", current_timestamp()
)

windowed_aggregations = df_with_timestamp \
    .withWatermark("processing_time", "2 minutes") \
    .groupBy(
        window("processing_time", "1 minute", "30 seconds"),
        "city"
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("wind_speed").alias("avg_wind_speed"),
        count("*").alias("measurement_count"),
        collect_set("weather_condition").alias("weather_conditions")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

overall_window_stats = df_with_timestamp \
    .withWatermark("processing_time", "2 minutes") \
    .groupBy(
        window("processing_time", "1 minute", "30 seconds")
    ) \
    .agg(
        count("*").alias("total_measurements"),
        approx_count_distinct("city").alias("cities_count"),  
        avg("temperature").alias("overall_avg_temperature"),
        min("temperature").alias("overall_min_temperature"),
        max("temperature").alias("overall_max_temperature")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

print("Starting windowed streaming processing...")
print("Window size: 1 minute")
print("Slide interval: 30 seconds")
print("Watermark: 2 minutes")

query1 = windowed_aggregations.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .start()

query2 = overall_window_stats.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Waiting for data...")
query1.awaitTermination()