from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pymongo
from datetime import datetime
import time

def check_mongodb_connection():
    """Проверяет подключение к MongoDB"""
    print("Checking MongoDB connection...")
    try:
      
        client = pymongo.MongoClient(
            "mongodb://admin:password@mongodb:27017/", 
            serverSelectionTimeoutMS=5000
        )
        client.server_info()
        client.close()
        print("MongoDB connection successful")
        return True
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        return False

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaToMongoDB") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka_mongo_checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

def create_weather_schema():
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
        StructField("kafka_timestamp", StringType(), True)
    ])

def save_to_mongodb(batch_df, batch_id):
    """Сохраняет данные в MongoDB"""
    try:
        if batch_df.count() == 0:
            return
        
        client = pymongo.MongoClient("mongodb://admin:password@mongodb:27017/")
        db = client["weather_db"]
        collection = db["weather_data"]
        
        records = []
        for row in batch_df.collect():
            record = {
                "timestamp": datetime.now(),
                "year": row.year,
                "month": row.month,
                "day": row.day,
                "hour": row.hour,
                "temperature": row.temp,
                "humidity": row.rhum,
                "pressure": row.pres,
                "wind_speed": row.wspd,
                "precipitation": row.prcp,
                "batch_id": batch_id,
                "processed_at": datetime.now().isoformat(),
                "metadata": {
                    "temp_source": row.temp_source,
                    "kafka_timestamp": row.kafka_timestamp
                }
            }
            records.append(record)
        
        if records:
            result = collection.insert_many(records)
            print(f"Saved {len(records)} records to MongoDB (Batch {batch_id})")
        
        client.close()
        
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

def main():
    print("Starting: Kafka → MongoDB")
    print("MongoDB: mongodb://admin:password@mongodb:27017/")
    print("Database: weather_db, Collection: weather_data")
    print("=" * 60)
    
    # Проверяем подключение к MongoDB
    if not check_mongodb_connection():
        print("Cannot start: MongoDB is not available")
        return
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
       
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "weather-data") \
            .option("startingOffsets", "latest") \
            .load()
        
     
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), create_weather_schema()).alias("data")
        ).select("data.*")
        
        
        query = parsed_df.writeStream \
            .foreachBatch(save_to_mongodb) \
            .outputMode("update") \
            .start()
        
        print("Stream started - Saving weather data to MongoDB...")
        print("Check data: mongo mongodb://admin:password@mongodb:27017/weather_db --eval 'db.weather_data.find().limit(3).pretty()'")
       
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    except KeyboardInterrupt:
        print("\nStopping stream...")
        spark.stop()
        print("Stream stopped.")

if __name__ == "__main__":
    main()