from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    # Создаем Spark сессию 
    spark = SparkSession.builder \
        .appName("WeatherAnalytics") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    

    spark.sparkContext.setLogLevel("WARN")
    
    print("Starting simplified Weather Analytics...")
    
    # Схема для погодных данных
    schema = StructType([
        StructField("city", StringType(), False),
        StructField("temperature", IntegerType(), False),
        StructField("humidity", IntegerType(), False),
        StructField("weather_condition", StringType(), False),
        StructField("timestamp", StringType(), False)
    ])
    
    # Читаем из сокета
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    
    # Парсим JSON
    weather_data = lines.select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")
    
    # Добавляем временную метку
    weather_data = weather_data.withColumn(
        "event_time", 
        to_timestamp(col("timestamp"))
    )
    
    print("Processing weather data...")
    
    #  средняя температура по городам за 30 секунд
    city_stats = weather_data \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(
            window(col("event_time"), "30 seconds"),
            col("city")
        ) \
        .agg(
            avg("temperature").alias("avg_temp"),
            count("*").alias("count")
        )
    
    # Запускаем запрос в консоль
    query = city_stats.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    print("Streaming started! Waiting for data...")
    print("Results will appear below:")
    
    query.awaitTermination()

if __name__ == "__main__":
    main()