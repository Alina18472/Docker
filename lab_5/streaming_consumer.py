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
    """Создаем Kafka поток для чтения данных"""
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
    """Обрабатываем погодные данные из Kafka"""
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

def main():
    print("=" * 60)
    print("Weather Streaming Application")
    print("=" * 60)
    
    # Создаем Spark сессию
    spark = create_spark_session()
    if spark is None:
        return
    
    query = None
    
    try:
        # Создаем Kafka поток
        kafka_stream = create_kafka_stream(spark)
        
        if kafka_stream is None:
            print("Не удалось подключиться к Kafka")
            return
        
        # Обрабатываем данные
        weather_df = process_weather_data(kafka_stream)
        
        # Запускаем потоковый запрос
        print("Запускаем потоковый запрос...")
        
        query = weather_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", 10) \
            .option("checkpointLocation", "/tmp/checkpoints/weather") \
            .start()
        
        print("\nСтатус:")
        print("   Поток запущен")
        print("   Ожидаем данные из топика: weather-data")
        print("   Обработка в течение 60 секунд...")
        print("=" * 60)
        
        # Ждем завершения (60 секунд)
        query.awaitTermination(60)
        
        print("Время выполнения истекло")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\nЗавершаем работу...")
        if query and query.isActive:
            query.stop()
        spark.stop()
        print("Приложение завершено")

if __name__ == "__main__":
    main()