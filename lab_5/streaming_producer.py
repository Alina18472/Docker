
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Producer создан успешно")
        return producer
    except Exception as e:
        print(f" Ошибка создания producer: {e}")
        return None

def generate_weather_data():
    cities = ["Moscow", "London", "New York", "Tokyo", "Berlin", "Paris", "Sydney"]
    weather_conditions = ["Sunny", "Cloudy", "Rainy", "Snowy", "Foggy", "Windy"]
    
    data = {
        "city": random.choice(cities),
        "temperature": random.randint(-20, 35),
        "humidity": random.randint(30, 95),
        "pressure": random.randint(980, 1030),
        "weather_condition": random.choice(weather_conditions),
        "wind_speed": random.randint(0, 25),
        "timestamp": datetime.now().isoformat()
    }
    return data

def main():
    producer = create_producer()
    if not producer:
        return
    
    print("Отправляем данные в Kafka...")
    print("Топик: weather_data")
   
    
    message_count = 0
    
    try:
        while True:
            message_count += 1
            
            # Генерируем данные о погоде
            data = generate_weather_data()
            
            # Отправляем в Kafka
            producer.send("weather_data", value=data)
            
            print(f"Sent message #{message_count}: {data}")
            
            # Пауза между сообщениями
            time.sleep(2)
            
    except KeyboardInterrupt:
        print(f"\n Остановлено. Отправлено сообщений: {message_count}")
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if producer:
            producer.close()
        print(" Producer завершил работу")

if __name__ == "__main__":
    main()