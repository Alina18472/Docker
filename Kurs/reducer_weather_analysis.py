
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def weather_analysis_reducer():
    current_date = None
    weather_data = []
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        parts = line.split(';')
        if len(parts) != 7:
            continue
            
        date_key, temp, rhum, prcp, wspd, pres, hour = parts
        
        try:
            temp = float(temp)
            rhum = float(rhum)
            prcp = float(prcp)
            wspd = float(wspd)
            pres = float(pres)
            
            if current_date != date_key:
                if current_date and weather_data:
                    analyze_weather_conditions(current_date, weather_data)
                
                current_date = date_key
                weather_data = []
            
            weather_data.append({
                'temp': temp,
                'rhum': rhum,
                'prcp': prcp,
                'wspd': wspd,
                'pres': pres,
                'hour': hour
            })
            
        except ValueError:
            continue
    
    if current_date and weather_data:
        analyze_weather_conditions(current_date, weather_data)

def analyze_weather_conditions(date, data):

    temps = [d['temp'] for d in data]
    rhums = [d['rhum'] for d in data]
    prcps = [d['prcp'] for d in data]
    wspds = [d['wspd'] for d in data]
    press = [d['pres'] for d in data]

    max_temp = max(temps)
    min_temp = min(temps)
    max_rhum = max(rhums)
    max_prcp = max(prcps)
    max_wspd = max(wspds)
    pressure_range = max(press) - min(press)

    phenomena = []
    
    if max_temp - min_temp > 15:
        phenomena.append("large_daily_temp_range")
    
    if max_rhum > 95:
        phenomena.append("very_high_humidity")
    
    if max_prcp > 5.0:
        phenomena.append("heavy_precipitation")
    
    if max_wspd > 50.0:
        phenomena.append("strong_wind")
    
    if pressure_range > 15:
        phenomena.append("rapid_pressure_change")
    
   
    if phenomena:
        print(f"WEATHER_PHENOMENA: {date} - Detected: {', '.join(phenomena)}")
        print(f"  Temperature: {min_temp:.1f}C - {max_temp:.1f}C")
        print(f"  Humidity: up to {max_rhum}%")
        print(f"  Precipitation: up to {max_prcp}mm")
        print(f"  Wind: up to {max_wspd} km/h")
        print(f"  Pressure range: {pressure_range:.1f} hPa")
    else:
    
        print(f"WEATHER_STATS: {date}")
        print(f"  Temperature: {min_temp:.1f}C - {max_temp:.1f}C")
        print(f"  Humidity: up to {max_rhum}%")
        print(f"  Precipitation: up to {max_prcp}mm")
        print(f"  Wind: up to {max_wspd} km/h")
        print(f"  Pressure range: {pressure_range:.1f} hPa")

if __name__ == "__main__":
    weather_analysis_reducer()
