import socket
import time
import json
import random
from datetime import datetime

def generate_weather_data():
    cities = ["Moscow", "London", "New York", "Tokyo", "Berlin", "Paris", "Sydney"]
    weather_conditions = ["Sunny", "Cloudy", "Rainy", "Snowy", "Foggy", "Windy"]
    
    return {
        "city": random.choice(cities),
        "temperature": random.randint(-20, 35),
        "humidity": random.randint(30, 95),
        "pressure": random.randint(980, 1030),
        "weather_condition": random.choice(weather_conditions),
        "wind_speed": random.randint(0, 25),
        "timestamp": datetime.now().isoformat()
    }

def start_weather_stream(host='localhost', port=9999):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Переиспользование порта
    server_socket.bind((host, port))
    server_socket.listen(5)  # Разрешаем несколько подключений
    
    print(f"Weather stream server started on {host}:{port}")
    
    try:
        while True:
            print("Waiting for connection...")
            conn, addr = server_socket.accept()
            print(f"Connected by {addr}")
            
            try:
                while True:
                    weather_data = generate_weather_data()
                    json_data = json.dumps(weather_data)
                    conn.send((json_data + '\n').encode())
                    print(f"Sent: {json_data}")
                    time.sleep(3)
                    
            except (BrokenPipeError, ConnectionResetError):
                print(f"Client {addr} disconnected")
            except Exception as e:
                print(f"Error with client {addr}: {e}")
            finally:
                conn.close()
                
    except KeyboardInterrupt:
        print("\nStopping weather stream...")
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_weather_stream()