from kafka import KafkaConsumer
from pymongo import MongoClient
import json
from datetime import datetime

# Настройки Kafka
KAFKA_BROKER = 'kafka:9092'  
TOPIC_NAME = 'weather_data'

# Настройки MongoDB
MONGO_URI = 'mongodb://admin:password@mongodb:27017/'  
DB_NAME = 'weather_db'
COLLECTION_NAME = 'weather_metrics'

# Подключаемся к MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def process_weather_data(weather_data):
    """Обработка данных"""
    temperature = weather_data['temperature']
    
    if temperature < 0:
        temp_category = "freezing"
    elif temperature < 10:
        temp_category = "cold" 
    elif temperature < 20:
        temp_category = "mild"
    else:
        temp_category = "warm"
    
    processed_data = {
        **weather_data,
        "temp_category": temp_category,
        "processed_at": datetime.now().isoformat()
    }
    return processed_data

def start_consumer():
    print("Starting Kafka Weather Consumer with MongoDB...")
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='weather-consumer-group'
    )
    
    try:
        for message in consumer:
            weather_data = message.value
            print(f"Received: {weather_data['city']} - {weather_data['temperature']}°C")
            
            # Обрабатываем и сохраняем в MongoDB
            processed_data = process_weather_data(weather_data)
            result = collection.insert_one(processed_data)
            
            print(f"Saved to MongoDB with ID: {result.inserted_id}")
            print("-" * 50)
            
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        consumer.close()
        client.close()

if __name__ == "__main__":
    start_consumer()