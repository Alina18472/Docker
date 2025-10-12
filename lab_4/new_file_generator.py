import json
import time
import random
import os
from datetime import datetime

def generate_weather_data(file_id):
    cities = ["Moscow", "London", "New York", "Tokyo", "Berlin", "Paris", "Sydney"]
    weather_conditions = ["Sunny", "Cloudy", "Rainy", "Snowy", "Foggy", "Windy"]
    
    data = {
        "file_id": file_id,
        "city": random.choice(cities),
        "temperature": random.randint(-20, 35),
        "humidity": random.randint(30, 95),
        "pressure": random.randint(980, 1030),
        "weather_condition": random.choice(weather_conditions),
        "wind_speed": random.randint(0, 25),
        "timestamp": datetime.now().isoformat()
    }
    return data

def start_file_generator(output_dir="/tmp/weather_data", interval=5):
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"File generator started! Saving files to: {output_dir}")
    print(f"Generating new file every {interval} seconds...")
    print("Press Ctrl+C to stop\n")
    
    # Найдем максимальный file_id из существующих файлов
    existing_files = [f for f in os.listdir(output_dir) if f.startswith('weather_')]
    if existing_files:
        file_id = len(existing_files) + 1
    else:
        file_id = 1
    
    try:
        while True:
            weather_data = generate_weather_data(file_id)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"weather_{timestamp}_{file_id}.json"
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w') as f:
                json.dump(weather_data, f, separators=(',', ':'))
            
            print(f"Created: {filename}")
            print(f"   Data: {weather_data}")
            print("   " + "-" * 50)
            
            file_id += 1
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\nFile generator stopped. Created {file_id-1} files in {output_dir}")

if __name__ == "__main__":
    start_file_generator(output_dir="/tmp/weather_data", interval=5)