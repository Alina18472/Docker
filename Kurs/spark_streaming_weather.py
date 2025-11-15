from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherDataStreaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/weather_checkpoint") \
        .getOrCreate()

def create_weather_schema():
    return StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("temp", DoubleType(), True),
        StructField("rhum", IntegerType(), True),
        StructField("prcp", DoubleType(), True),
        StructField("wspd", DoubleType(), True),
        StructField("pres", DoubleType(), True),
        StructField("current_timestamp", StringType(), True)
    ])

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Starting Spark Streaming application...")
    
 
    source_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    
   
    json_df = source_df.select(
        from_json(col("value"), create_weather_schema()).alias("data")
    ).select("data.*")
    
    # Добавляем время обработки
    processed_df = json_df.withColumn(
        "processing_time", 
        current_timestamp()
    )
    

    windowed_agg = processed_df \
        .withWatermark("processing_time", "10 seconds") \
        .groupBy(
            window(col("processing_time"), "15 seconds", "10 seconds")
        ) \
        .agg(
            round(avg("temp"), 1).alias("avg_temperature"),
            round(max("temp"), 1).alias("max_temperature"),
            round(min("temp"), 1).alias("min_temperature"),
            round(avg("rhum"), 1).alias("avg_humidity"),
            round(avg("pres"), 1).alias("avg_pressure"),
            round(avg("wspd"), 1).alias("avg_wind_speed"),
            round(sum("prcp"), 2).alias("total_precipitation"),
            count("*").alias("records_count")
        )
    
   
    agg_query = windowed_agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    print("Aggregation query started - waiting for data...")
    
    agg_query.awaitTermination()

if __name__ == "__main__":
    main()