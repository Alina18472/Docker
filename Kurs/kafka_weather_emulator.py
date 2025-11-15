import time
import json
import pandas as pd
import sys
from datetime import datetime
from kafka import KafkaProducer
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_producer(bootstrap_servers='kafka:9092'):  
    """Создает Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers, 
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info(f"Kafka producer created for {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        return None

def send_to_kafka(producer, topic, data):
    """Отправляет данные в Kafka topic"""
    try:
        
        data['kafka_timestamp'] = datetime.now().isoformat()
        
        future = producer.send(topic, value=data)
    
        result = future.get(timeout=10)
        
     
        logger.info(f"Sent to Kafka: {data['year']}-{data['month']:02d}-{data['day']:02d} {data['hour']:02d}:00 | "
                   f"Temp: {data['temp']}C | Humidity: {data['rhum']}% | "
                   f"Wind: {data['wspd']} km/h | Pressure: {data['pres']} hPa | "
                   f"Precipitation: {data['prcp']} mm | Cloud: {data['cldc']}%")
        return True
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")
        return False

def emulate_weather_station(data_file, interval=3, kafka_topic='weather-data'):
    """
    Эмулирует работу метеостанции, отправляя данные в Kafka
    """
    
 
    try:
        df = pd.read_csv(data_file, delimiter=';')
        logger.info(f"Loaded {len(df)} records from {data_file}")
    except Exception as e:
        logger.error(f"Error reading data file: {e}")
        return
    

    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer")
        return
    

    records_sent = 0
    try:
        for index, row in df.iterrows():
      
            record = row.to_dict()
            
        
            for key in record:
                if pd.isna(record[key]):
                    record[key] = None
                elif isinstance(record[key], (int, float)):
                    continue
                else:
                    try:
                        str_val = str(record[key]).strip()
                        if str_val == '' or str_val.lower() == 'nan':
                            record[key] = None
                        elif '.' in str_val:
                            record[key] = float(str_val)
                        else:
                            record[key] = int(str_val)
                    except (ValueError, TypeError):
                        record[key] = str(record[key]).strip() if pd.notna(record[key]) else None
            
    
            if send_to_kafka(producer, kafka_topic, record):
                records_sent += 1
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        logger.info("Emulation stopped by user")
    except Exception as e:
        logger.error(f"Error during emulation: {e}")
    finally:
     
        producer.flush()
        producer.close()
        logger.info(f"Emulation finished. Total records sent to Kafka: {records_sent}")

if __name__ == "__main__":
    data_file = "/opt/spark/data/weather_data.csv"
    interval = 3
    kafka_topic = "weather-data"
    
    if len(sys.argv) > 1:
        data_file = sys.argv[1]
    if len(sys.argv) > 2:
        interval = int(sys.argv[2])
    if len(sys.argv) > 3:
        kafka_topic = sys.argv[3]
    
    print(f"Starting Kafka weather station emulator:")
    print(f"Data file: {data_file}")
    print(f"Kafka topic: {kafka_topic}")
    print(f"Interval: {interval} seconds")
    print("-" * 60)
    
    emulate_weather_station(data_file, interval, kafka_topic)