import sqlite3
import pandas as pd

def check_database():
    conn = sqlite3.connect('weather.db')
    
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print("Таблицы в базе данных:", [table[0] for table in tables])
    
    df = pd.read_sql("SELECT * FROM weather_records LIMIT 5", conn)
    print("\nПервые 5 записей:")
    print(df.to_string(index=False))

    print("\nСтруктура таблицы weather_records:")
    cursor = conn.execute("PRAGMA table_info(weather_records)")
    for column in cursor.fetchall():
        print(f"  {column[1]:<20} {column[2]}")
    
    conn.close()

if __name__ == "__main__":
    check_database()