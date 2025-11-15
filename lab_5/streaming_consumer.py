from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import time

def create_spark_session():
    print("Создаем Spark сессию...")
    
    try:
        spark = SparkSession.builder \
            .appName("WeatherStreamingApp") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("Spark сессия создана успешно!")
        return spark
        
    except Exception as e:
        print(f"Ошибка создания Spark сессии: {e}")
        return None

def create_kafka_stream(spark):
    # Создаем Kafka поток для чтения данных
    print("Создаем Kafka поток...")
    
    try:
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "weather_data") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("Подключение к Kafka успешно!")
        return kafka_df
        
    except Exception as e:
        print(f"Ошибка создания Kafka потока: {e}")
        return None

def process_weather_data(kafka_df):
    # Обрабатываем погодные данные из Kafka
    print("Обрабатываем погодные данные...")
    
    # Схема для JSON данных о погоде
    json_schema = StructType([
        StructField("city", StringType()),
        StructField("temperature", IntegerType()),
        StructField("humidity", IntegerType()),
        StructField("pressure", IntegerType()),
        StructField("weather_condition", StringType()),
        StructField("wind_speed", IntegerType()),
        StructField("timestamp", StringType())
    ])
    
    # Парсим JSON сообщения из Kafka
    parsed_df = kafka_df.select(
        col("topic"),
        col("partition"),
        col("value").cast("string"),
        from_json(col("value").cast("string"), json_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("offset")
    ).filter(col("data").isNotNull())
    
    # Извлекаем данные из JSON
    weather_df = parsed_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("data.city"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.pressure"),
        col("data.weather_condition"),
        col("data.wind_speed"),
        col("data.timestamp").alias("weather_timestamp"),
        col("kafka_timestamp")
    )
    
    return weather_df

def add_aggregations(weather_df):
   # Добавляем агрегации к данным
    print("Добавляем агрегации...")
    
    # Агрегация по городам 
    city_stats = weather_df \
        .withWatermark("kafka_timestamp", "5 minutes") \
        .groupBy(
            "city",
            window("kafka_timestamp", "5 minutes", "1 minute")  
        ) \
        .agg(
            count("*").alias("message_count"),
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("wind_speed").alias("avg_wind_speed"),
            approx_count_distinct("weather_condition").alias("unique_conditions")
        ) \
        .select(
            "city",
            "window.start",
            "window.end",
            "message_count",
            round("avg_temperature", 1).alias("avg_temp"),
            "min_temperature",
            "max_temperature",
            round("avg_humidity", 1).alias("avg_humidity"),
            round("avg_wind_speed", 1).alias("avg_wind_speed"),
            "unique_conditions"
        )
    
    return city_stats

def main():
    print("=" * 60)
    print("Weather Streaming Application with Aggregations")
    print("=" * 60)
    
    # Создаем Spark сессию
    spark = create_spark_session()
    if spark is None:
        return
    
    query_raw = None
    query_stats = None
    
    try:
        # Создаем Kafka поток
        kafka_stream = create_kafka_stream(spark)
        
        if kafka_stream is None:
            print("Не удалось подключиться к Kafka")
            return
        
        # Обрабатываем данные
        weather_df = process_weather_data(kafka_stream)
        
        # Добавляем агрегации
        city_stats_df = add_aggregations(weather_df)
        
        print("Запускаем потоковые запросы...")
        
        # Запрос 1: Сырые данные
        query_raw = weather_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 5) \
            .option("checkpointLocation", "/tmp/checkpoints/weather_raw") \
            .queryName("raw_data") \
            .start()
        
        # Запрос 2: Агрегированные данные
        query_stats = city_stats_df \
            .writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 10) \
            .option("checkpointLocation", "/tmp/checkpoints/weather_stats") \
            .queryName("city_statistics") \
            .start()
        
        print("\n" + "=" * 60)
        print("=" * 60)
        print("\nОжидаем данные...\n")
        
     
        query_stats.awaitTermination(120)
        
        print("Время выполнения истекло")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\nЗавершаем работу...")
        if query_raw and query_raw.isActive:
            query_raw.stop()
        if query_stats and query_stats.isActive:
            query_stats.stop()
        spark.stop()
        print("Приложение завершено")

if __name__ == "__main__":
    main()