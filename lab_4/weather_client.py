import socket
import json

def test_weather_stream(host='localhost', port=9999, count=5):
    """Подключается к потоку погодных данных и получает указанное количество сообщений"""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        print(f"Connecting to weather stream at {host}:{port}...")
        client_socket.connect((host, port))
        print("Connected successfully!")
        
        for i in range(count):
            data = client_socket.recv(1024).decode().strip()
            if data:
                weather_data = json.loads(data)
                print(f"Message {i+1}:")
                print(f"   City: {weather_data['city']}")
                print(f"   Temperature: {weather_data['temperature']}°C")
                print(f"   Condition: {weather_data['weather_condition']}")
                print(f"   Time: {weather_data['timestamp']}")
                print("   " + "-" * 30)
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client_socket.close()
        print("Connection closed")

if __name__ == "__main__":
    test_weather_stream(count=5)  # Получить 5 сообщений