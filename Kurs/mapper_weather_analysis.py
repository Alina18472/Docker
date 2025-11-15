#!/usr/bin/env python3
import sys

def weather_analysis_mapper():
    for line in sys.stdin:
        line = line.strip()
        if not line or line.startswith('year'):
            continue
            
        fields = line.split(';')
        if len(fields) < 20:
            continue
            
        try:
            year = fields[0]
            month = fields[1]
            day = fields[2]
            hour = fields[3]
            temp = float(fields[4])
            rhum = float(fields[6])
            prcp = float(fields[8])
            wspd = float(fields[14])
            pres = float(fields[18])
            
            # Ключ для группировки по дням
            date_key = f"{year}-{month}-{day}"
          
            print(f"{date_key};{temp};{rhum};{prcp};{wspd};{pres};{hour}")
            
        except (ValueError, IndexError):
            continue

if __name__ == "__main__":
    weather_analysis_mapper()