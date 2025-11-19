import json
import pandas as pd
import sqlite3
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeatherETL:
    def __init__(self, input_file, db_file='weather.db'):
        self.input_file = input_file
        self.db_file = db_file
        self.raw_data = None
        self.transformed_data = None
        
    def extract(self):
        try:
            logger.info(f"Извлечение данных из файла: {self.input_file}")
            with open(self.input_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            self.raw_data = data['weather_data']
            logger.info(f"Успешно извлечено {len(self.raw_data)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении данных: {e}")
            return False
    
    def transform(self):
        try:
            
            df = pd.DataFrame(self.raw_data)


            def get_temperature_category(temp):
                if temp > 5:
                    return 'тепло'
                elif temp > 0:
                    return 'прохладно'
                elif temp > -10:
                    return 'холодно'
                else:
                    return 'очень холодно'
            
            df['temp_category'] = df['temperature'].apply(get_temperature_category)
            

            def get_precipitation_category(precip):
                if precip == 0:
                    return 'без осадков'
                elif precip < 1:
                    return 'слабые осадки'
                elif precip < 3:
                    return 'умеренные осадки'
                else:
                    return 'сильные осадки'
            
            df['precip_category'] = df['precipitation'].apply(get_precipitation_category)
            

            df['created_at'] = datetime.now()
            df['year'] = pd.to_datetime(df['date']).dt.year
            df['month'] = pd.to_datetime(df['date']).dt.month
            df['day'] = pd.to_datetime(df['date']).dt.day
            
            df['record_id'] = df['city'] + '_' + df['date']
            
            initial_count = len(df)
            df = df.drop_duplicates(subset=['record_id'])
            df = df.dropna()
            
            self.transformed_data = df
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при преобразовании данных: {e}")
            return False
    
    def load(self):
        try:
            logger.info(f"Загрузка данных в базу: {self.db_file}")
            
            conn = sqlite3.connect(self.db_file)
            

            create_table_query = """
            CREATE TABLE IF NOT EXISTS weather_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                record_id TEXT UNIQUE,
                city TEXT NOT NULL,
                date TEXT NOT NULL,
                temperature REAL,
                humidity INTEGER,
                pressure INTEGER,
                wind_speed REAL,
                condition TEXT,
                precipitation REAL,
                temp_category TEXT,
                precip_category TEXT,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                created_at TIMESTAMP
            )
            """
            conn.execute(create_table_query)
            
            self.transformed_data.to_sql('weather_records', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
            
            logger.info(f"Успешно загружено {len(self.transformed_data)} записей в базу данных")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            return False
    
    def run_etl(self):

        if not self.extract():
            return False
        
        if not self.transform():
            return False
        
        if not self.load():
            return False
        
        return True
    
    def generate_report(self):

        try:
            conn = sqlite3.connect(self.db_file)
            
            stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT city) as unique_cities,
                COUNT(DISTINCT date) as unique_dates,
                MIN(temperature) as min_temp,
                MAX(temperature) as max_temp,
                AVG(temperature) as avg_temp
            FROM weather_records
            """
            stats = conn.execute(stats_query).fetchone()
            
            city_stats_query = """
            SELECT 
                city,
                COUNT(*) as records_count,
                AVG(temperature) as avg_temp,
                AVG(humidity) as avg_humidity,
                SUM(precipitation) as total_precip
            FROM weather_records
            GROUP BY city
            ORDER BY avg_temp DESC
            """
            city_stats = conn.execute(city_stats_query).fetchall()
            
            conn.close()
            
            print("\n" + "="*50)
            print("ОТЧЕТ ПО ДАННЫМ О ПОГОДЕ")
            print("="*50)
            print(f"Всего записей: {stats[0]}")
            print(f"Количество городов: {stats[1]}")
            print(f"Количество дней: {stats[2]}")
            print(f"Минимальная температура: {stats[3]:.1f}°C")
            print(f"Максимальная температура: {stats[4]:.1f}°C")
            print(f"Средняя температура: {stats[5]:.1f}°C")
            
            print("\nСтатистика по городам:")
            print("-" * 60)
            print(f"{'Город':<15} {'Записей':<8} {'Ср. темп.':<10} {'Ср. влаж.':<10} {'Осадки':<10}")
            print("-" * 60)
            for city in city_stats:
                print(f"{city[0]:<15} {city[1]:<8} {city[2]:<10.1f} {city[3]:<10.1f} {city[4]:<10.1f}")
            
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета: {e}")

def main():

    etl = WeatherETL('weather_data.json')
    
    if etl.run_etl():
        etl.generate_report()
        
        print("\n" + "="*50)
        print("ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ")
        print("="*50)
        
        df = etl.transformed_data
        print(f"Самый теплый город: {df.loc[df['temperature'].idxmax(), 'city']} ({df['temperature'].max():.1f}°C)")
        print(f"Самый холодный город: {df.loc[df['temperature'].idxmin(), 'city']} ({df['temperature'].min():.1f}°C)")
        print(f"Больше всего осадков: {df.loc[df['precipitation'].idxmax(), 'city']} ({df['precipitation'].max():.1f} мм)")
        
        temp_cats = df['temp_category'].value_counts()
        print("\nРаспределение по температурным категориям:")
        for cat, count in temp_cats.items():
            print(f"  {cat}: {count} записей")
            
    else:
        print("ETL процесс завершился с ошибками")

if __name__ == "__main__":
    main()