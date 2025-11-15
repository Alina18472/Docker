from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """Создает Spark session с поддержкой Kafka"""
    return SparkSession.builder \
        .appName("KafkaWeatherStreaming") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka_weather_checkpoint") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

def create_weather_schema():
    """Схема для погодных данных"""
    return StructType([
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("temp", DoubleType(), True),
        StructField("temp_source", StringType(), True),
        StructField("rhum", IntegerType(), True),
        StructField("rhum_source", StringType(), True),
        StructField("prcp", DoubleType(), True),
        StructField("prcp_source", StringType(), True),
        StructField("wspd", DoubleType(), True),
        StructField("wspd_source", StringType(), True),
        StructField("pres", DoubleType(), True),
        StructField("pres_source", StringType(), True),
        StructField("coco", IntegerType(), True),
        StructField("coco_source", StringType(), True),
        StructField("kafka_timestamp", StringType(), True)
    ])

def read_from_kafka(spark, kafka_topic='weather-data'):
    """Читает данные из Kafka topic"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

def process_kafka_stream(df):
    """Обрабатывает поток данных из Kafka"""
    
    json_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), create_weather_schema()).alias("data"),
        col("timestamp").alias("kafka_processing_time"),
        col("offset"),
        col("partition")
    ).select(
        "key", "offset", "partition", "kafka_processing_time", "data.*"
    )
    
    processed_df = json_df.withColumn(
        "processing_time", 
        current_timestamp()
    ).withColumn(
        "date_time",
        concat(col("year"), lit("-"), lpad(col("month"), 2, "0"), lit("-"), 
               lpad(col("day"), 2, "0"), lit(" "), lpad(col("hour"), 2, "0"), lit(":00"))
    )
    

    raw_query = processed_df.select(
        col("date_time").alias("historical_time"),
        col("temp").alias("temperature_c"),
        col("rhum").alias("humidity_pct"),
        col("pres").alias("pressure_hpa"),
        col("wspd").alias("wind_speed_kmh"),
        col("kafka_processing_time"),
        col("offset")
    ).writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .start()
    

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
            round(max("wspd"), 1).alias("max_wind_speed"),
            round(sum("prcp"), 2).alias("total_precipitation"),
            count("*").alias("records_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_temperature",
            "max_temperature", 
            "min_temperature",
            "avg_humidity",
            "avg_pressure",
            "avg_wind_speed",
            "max_wind_speed", 
            "total_precipitation",
            "records_count"
        )
    
    return windowed_agg, raw_query

def write_aggregations_to_console(df):
    """Записывает агрегированные результаты в консоль"""
    return df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .start()

def main():
    """Основная функция"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    kafka_topic = "weather-data"
    
    print("Starting Spark Streaming with Kafka...")
    print(f"Reading from Kafka topic: {kafka_topic}")
    print("Configuration:")
    print("  - Window: 15 seconds")
    print("  - Slide: 10 seconds") 
    print("  - Watermark: 10 seconds")
    print("=" * 80)
    
    try:
   
        kafka_df = read_from_kafka(spark, kafka_topic)
        
       
        aggregated_df, raw_query = process_kafka_stream(kafka_df)
   
        agg_query = write_aggregations_to_console(aggregated_df)
        
        print("All streaming queries started successfully!")
        print("Waiting for data from Kafka...")
        
        # Ожидаем завершения
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        for query in spark.streams.active:
            query.stop()
        spark.stop()
        print("All queries stopped.")

if __name__ == "__main__":
    main()