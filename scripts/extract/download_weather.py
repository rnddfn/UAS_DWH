# download_weather.py
import pandas as pd
import requests
import time

# 1. Baca CSV daftar kota AS
try:
    df_cities = pd.read_csv('./data/raw/cities_with_coords.csv') # Ganti dengan nama file CSV kotamu
    # Ambil hanya kota unik
    df_locations = df_cities.drop_duplicates(subset=['city'])
except Exception as e:
    print(f"Gagal membaca CSV kota: {e}")
    exit()

master_weather_list = []

print(f"Akan mengunduh data cuaca untuk {len(df_locations)} kota...")

# 2. Loop per KOTA, ambil data 1 TAHUN (2018)
for row in df_locations.itertuples():
    lat = row.lat
    lon = row.lng
    city_name = row.city

    url = (
        "https://archive-api.open-meteo.com/v1/era5?"
        f"latitude={lat}&longitude={lon}"
        "&start_date=2018-01-01&end_date=2018-05-09"
        "&daily=temperature_2m_max,precipitation_sum,windspeed_10m_max"
    )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Proses data 365 hari
        daily_data = data['daily']
        df_city_weather = pd.DataFrame(daily_data)
        df_city_weather['CityName'] = city_name # Tambahkan nama kota

        master_weather_list.append(df_city_weather)

        print(f"BERHASIL: {city_name} (Total Panggilan: {len(master_weather_list)})")

        # Beri jeda agar tidak overload (misal 500 panggilan)
        if len(master_weather_list) % 500 == 0:
            print("--- JEDA 60 DETIK ---")
            time.sleep(60)

    except Exception as e:
        print(f"GAGAL: {city_name}: {e}")

# 3. Simpan hasilnya ke CSV
print("Menggabungkan semua data...")
df_weather_final = pd.concat(master_weather_list)

# 4. SIMPAN SEBAGAI "CACHE" LOKALMU
df_weather_final.to_csv('./data/raw/cuaca_master_2018.csv', index=False)
print("SELESAI. Data cuaca lengkap tersimpan di 'cuaca_master_2018.csv'")