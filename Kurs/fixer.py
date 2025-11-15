import pandas as pd
import numpy as np

# Загрузка данных
df = pd.read_csv('weather_data_fixed.csv', delimiter=';')

print("Первые строки до исправления:")
print(df[['temp', 'wspd']].head(10))

# Функция для исправления значений
def fix_date_value(value):
    if pd.isna(value):
        return value
    
    value_str = str(value)
    
    # Формат "22.03.2025 0:00" или "06.09.2025 0:00"
    if '.' in value_str and '2025' in value_str:
        try:
            # Берем первое число (день) из даты
            day_part = value_str.split('.')[0]
            return float(day_part)
        except:
            return np.nan
    
    # Формат "2025-09-06 00:00:00" 
    elif '-' in value_str and '2025' in value_str:
        try:
            # Берем день из даты (третья часть после разделения по -)
            day_part = value_str.split('-')[2].split(' ')[0]
            return float(day_part)
        except:
            return np.nan
    
    # Если это нормальное число
    else:
        try:
            return float(value_str)
        except:
            return np.nan

# Применяем исправления
df['temp'] = df['temp'].apply(fix_date_value)
df['wspd'] = df['wspd'].apply(fix_date_value)
df['prcp'] = df['prcp'].apply(fix_date_value)
df['wpgt'] = df['wpgt'].apply(fix_date_value)

print("\nПосле исправления:")
print(df[['temp', 'wspd', 'prcp']].head(20))

# Проверяем результаты
print(f"\nПроверка - уникальные значения в temp: {df['temp'].unique()[:10]}")
print(f"Проверка - уникальные значения в wspd: {df['wspd'].unique()[:10]}")

# Сохраняем
df.to_csv('fixed_data.csv', index=False, sep=';')
print("\nДанные исправлены и сохранены!")