import time
import json
import socket
import pandas as pd
import sys

def create_socket_server(host='localhost', port=9999):
    """Создает socket сервер для отправки данных"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.listen(1)
        print(f"Socket server listening on {host}:{port}")
        return sock
    except Exception as e:
        print(f"Error creating socket server: {e}")
        return None

def send_via_socket(conn, data):
    """Отправляет данные через socket"""
    try:
        message = json.dumps(data) + '\n'
        conn.send(message.encode('utf-8'))
        print(f"Sent: {data['year']}-{data['month']:02d}-{data['day']:02d} {data['hour']:02d}:00 | "
              f"Temp: {data['temp']}C | Humidity: {data['rhum']}% | Wind: {data['wspd']} km/h | Pressure: {data['pres']} hPa")
        return True
    except Exception as e:
        print(f"Error sending via socket: {e}")
        return False

def emulate_weather_station(data_file, interval=5):
    """
    Эмулирует работу метеостанции, отправляя данные с интервалом
    """
 
    try:
        df = pd.read_csv(data_file, delimiter=';')
        print(f"Loaded {len(df)} records from {data_file}")
    except Exception as e:
        print(f"Error reading data file: {e}")
        return
    
 
    sock = create_socket_server()
    if not sock:
        print("Failed to create socket server")
        return
    
    print("Waiting for Spark connection...")
    conn, addr = sock.accept()
    print(f"Connected by {addr}")
    

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
                        if '.' in str(record[key]):
                            record[key] = float(record[key])
                        else:
                            record[key] = int(record[key])
                    except:
                        pass
          
            if send_via_socket(conn, record):
                records_sent += 1
        
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\nEmulation stopped by user")
    except BrokenPipeError:
        print("\nConnection closed by Spark")
    except Exception as e:
        print(f"Error during emulation: {e}")
    finally:
   
        conn.close()
        sock.close()
        print(f"Emulation finished. Total records sent: {records_sent}")

if __name__ == "__main__":
    data_file = "/opt/spark/data/weather_data.csv"
    interval = 3 
    
    if len(sys.argv) > 1:
        data_file = sys.argv[1]
    if len(sys.argv) > 2:
        interval = int(sys.argv[2])
    
    print(f"Starting weather station emulator:")
    print(f"Data file: {data_file}")
    print(f"Interval: {interval} seconds")
    
    emulate_weather_station(data_file, interval)