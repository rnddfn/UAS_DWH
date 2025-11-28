# etl.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import requests # <-- DIPASTIKAN ADA

# --- 1. Konfigurasi Logging ---
os.makedirs('logs', exist_ok=True) 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_execution.log'),
        logging.StreamHandler()
    ]
)

# --- 2. Konfigurasi Koneksi Database ---
try:
    db_user = os.environ.get('POSTGRES_USER')
    db_pass = os.environ.get('POSTGRES_PASSWORD')
    db_host = os.environ.get('POSTGRES_HOST')
    db_name = os.environ.get('POSTGRES_DB')
    
    connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
    engine = create_engine(connection_string)
    logging.info("Koneksi ke database Postgres berhasil!")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.commit() 
    logging.info("Schema 'staging' dipastikan ada.")
    
    # Buat schema dan tabel DWH dari scheme.sql
    with open('scheme.sql', 'r') as f:
        schema_sql = f.read()
    
    with engine.connect() as conn:
        # Drop tabel yang mungkin sudah ada
        try:
            conn.execute(text("DROP TABLE IF EXISTS dwh.factsales CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS dwh.dimweather CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS dwh.dimproduct CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS dwh.dimcustomer CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS dwh.dimemployee CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS dwh.dimlocation CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS dwh.dimdate CASCADE;"))
            conn.commit()
        except Exception as e:
            logging.warning(f"No tables to drop: {e}")
        
        # Split SQL statements dan jalankan satu per satu
        statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
        for statement in statements:
            try:
                conn.execute(text(statement))
            except Exception as e:
                logging.warning(f"Skipped statement: {e}")
        conn.commit()
    logging.info("Schema 'dwh' dan tabel-tabel DWH dipastikan ada.")
    
except Exception as e:
    logging.error(f"Koneksi database GAGAL: {e}")
    exit(1)

# Ambil data libur
def load_calendar_and_holidays_to_staging(year=2018, country_code='US'):
    try:
        logging.info(f"Membuat kalender untuk tahun {year}...")
        # 1. Buat kalender dasar (unik)
        df_calendar = pd.DataFrame({"FullDate": pd.date_range(start=f'{year}-01-01', end=f'{year}-12-31')})
        df_calendar['DateID'] = df_calendar['FullDate'].dt.strftime('%Y%m%d').astype(int)
        df_calendar['Day'] = df_calendar['FullDate'].dt.day
        df_calendar['Month'] = df_calendar['FullDate'].dt.month
        df_calendar['MonthName'] = df_calendar['FullDate'].dt.month_name()
        df_calendar['Quarter'] = df_calendar['FullDate'].dt.quarter
        df_calendar['Year'] = df_calendar['FullDate'].dt.year
        df_calendar['DayOfWeek'] = df_calendar['FullDate'].dt.day_name()
        
        # 2. Ambil API Liburan
        logging.info(f"Mengambil data libur untuk {country_code}...")
        response = requests.get(f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}")
        response.raise_for_status() 
        holidays_data = response.json()
        df_holidays = pd.DataFrame(holidays_data)

        # 3. Proses dan Deduplikasi Hari Libur
        df_holidays['FullDate'] = pd.to_datetime(df_holidays['date']).dt.date
        df_holidays_grouped = df_holidays.groupby('FullDate')['name'].apply(
            lambda x: ', '.join(x)
        ).reset_index()
        df_holidays_grouped = df_holidays_grouped.rename(columns={'name': 'HolidayName'})
        df_holidays_grouped['IsHoliday'] = True
        
        # 4. Gabungkan kalender dengan data liburan yang SEKARANG SUDAH UNIK
        df_calendar['FullDate'] = df_calendar['FullDate'].dt.date
        
        # Merge ini sekarang aman (1-ke-1)
        df_final_date = pd.merge(df_calendar, df_holidays_grouped, on='FullDate', how='left')
        
        # Isi nilai default untuk yang bukan hari libur
        df_final_date['IsHoliday'] = df_final_date['IsHoliday'].fillna(False)
        df_final_date['HolidayName'] = df_final_date['HolidayName'].fillna('') # Isi string kosong
        
        # 5. Load ke Staging
        df_final_date.to_sql('dimdate_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        logging.info("Berhasil memuat staging.dimdate_mentah (dengan deduplikasi).")
        
    except Exception as e:
        logging.error(f"Error memuat DimDate: {e}")
        raise e

# --- FUNGSI UTAMA ---
def run_elt():
    try:
        # FASE 1: "Load ke Staging" (Ini adalah proses E-L)
        logging.info("Memulai Extract & Load CSV ke Staging...")

        # Baca CSV, Load ke Staging
        df_products = pd.read_csv('./data/raw/products.csv')
        df_products.to_sql('products_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        del df_products
        logging.info("Berhasil memuat staging.products_mentah.") # <-- PERBAIKAN LOG
        
        df_categories = pd.read_csv('./data/raw/categories.csv')
        df_categories.to_sql('categories_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        del df_categories
        logging.info("Berhasil memuat staging.categories_mentah.")
        
        df_employees = pd.read_csv('./data/raw/employees.csv')
        df_employees.to_sql('employees_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        del df_employees
        logging.info("Berhasil memuat staging.employees_mentah.")

        df_customers = pd.read_csv('./data/raw/customers.csv')
        df_customers.to_sql('customers_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        del df_customers
        logging.info("Berhasil memuat staging.customers_mentah.")

                # KODE BENAR
        df_cities = pd.read_csv('./data/raw/cities_MODIFIED_with_coords.csv')
        df_cities.to_sql(
            'cities_mentah', 
            con=engine,         # <-- Tulis 'con=engine' BUKAN '...'
            schema='staging', 
            if_exists='replace', 
            index=False
        )
        del df_cities
        logging.info("Berhasil memuat staging.cities_mentah.")

        df_countries = pd.read_csv('./data/raw/countries.csv')
        df_countries.to_sql('countries_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        del df_countries
        logging.info("Berhasil memuat staging.countries_mentah.")
        
        df_weather = pd.read_csv('./data/raw/weather_mentah.csv')
        df_weather.to_sql('weather_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        del df_weather
        logging.info("Berhasil memuat staging.weather_mentah.")

        # --- Load Sales (Gunakan chunking jika file besar) ---
        logging.info("Memproses file sales (chunking)...")
        chunk_size = 100000 
        sales_iterator = pd.read_csv('./data/raw/sales.csv', chunksize=chunk_size)
        
        # Hapus tabel lama (jika ada) HANYA SEKALI
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS staging.sales_mentah;"))
            conn.commit()

        for i, chunk in enumerate(sales_iterator):
            logging.info(f"Memuat sales chunk {i+1}...")
            chunk.to_sql(
                'sales_mentah',
                con=engine,
                schema='staging',
                if_exists='append', 
                index=False
            )
            del chunk
        logging.info("Berhasil memuat staging.sales_mentah.")
        
        logging.info("FASE 1 (CSV) SELESAI: Staging area terisi.")

        # --- FASE 1 (API) ---
        logging.info("Memulai Fase 1 (API)...")
        load_calendar_and_holidays_to_staging(year=2018, country_code='US')
        
        logging.info("FASE 1 (API) SELESAI.")
        
        # ==========================================================
        # FASE 2: TRANSFORMASI (T)
        # (TAMBAHKAN SEMUA KODE DI BAWAH INI)
        # ==========================================================
        logging.info("Memulai Fase 2: Transformasi (Staging ke DWH)...")
        
        # Ini adalah satu string SQL besar yang berisi semua logika transformasi
        transform_sql = """
        -- ========= FASE 2.1: KOSONGKAN TABEL DWH (Agar Idempotent) =========
        TRUNCATE TABLE dwh.factsales RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimproduct RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimcustomer RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimemployee RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimlocation RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimdate RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimweather RESTART IDENTITY CASCADE;

        -- ========= FASE 2.2: LOAD TABEL DIMENSI (dari Staging) =========

        -- 2.2.1. Load dwh.dimdate (dari staging.dimdate_mentah)
        INSERT INTO dwh.dimdate (
            dateid, fulldate, day, month, monthname,
            quarter, year, dayofweek, isholiday, holidayname
        )
        SELECT
            "DateID", "FullDate", "Day", "Month", "MonthName",
            "Quarter", "Year", "DayOfWeek", "IsHoliday", "HolidayName"
        FROM staging.dimdate_mentah;

        -- 2.2.2. Load dwh.dimlocation (dari staging.cities_mentah)
        INSERT INTO dwh.dimlocation (
            locationid, cityid_oltp, cityname, countryname
        )
        SELECT
            "CityID",
            "CityID",
            "CityName",
            'United States'
        FROM staging.cities_mentah;

        -- 2.2.3. Load dwh.dimproduct (gabungan produk + kategori)
        INSERT INTO dwh.dimproduct (
            productid_oltp, productname, price,
            categoryname, class, isallergic
        )
        SELECT
            p."ProductID",
            p."ProductName",
            p."Price",
            c."CategoryName",
            p."Class",
            p."IsAllergic"
        FROM staging.products_mentah p
        LEFT JOIN staging.categories_mentah c ON p."CategoryID" = c."CategoryID";

        -- 2.2.4. Load dwh.dimcustomer
        INSERT INTO dwh.dimcustomer (
            customerid_oltp, customername, address,
            customercityname, customercountryname
        )
        SELECT
            c."CustomerID",
            c."FirstName",
            c."Address",
            ci."CityName",
            co."CountryName"
        FROM staging.customers_mentah c
        LEFT JOIN staging.cities_mentah ci ON c."CityID" = ci."CityID"
        LEFT JOIN staging.countries_mentah co ON ci."CountryID" = co."CountryID";

        -- 2.2.5. Load dwh.dimemployee
        INSERT INTO dwh.dimemployee (
            employeeid_oltp, employeename, gender, hiredate
        )
        SELECT
            "EmployeeID",
            "FirstName",
            "Gender",
            "HireDate"::DATE
        FROM staging.employees_mentah;

        -- 2.2.6. Load dwh.dimweather (dari staging.weather_mentah)
        INSERT INTO dwh.dimweather (
            condition, temperature_c, feelslike_c, wind_kph, precip_mm, isday,
            dateid, locationid
        )
        SELECT
            NULL AS condition,
            w."temperature_2m_max"        AS temperature_c,
            NULL                         AS feelslike_c,
            w."windspeed_10m_max"        AS wind_kph,
            w."precipitation_sum"        AS precip_mm,
            NULL                         AS isday,
            d.dateid,
            l.locationid
        FROM staging.weather_mentah w
        LEFT JOIN dwh.dimdate d
            ON w."time"::DATE = d.fulldate
        LEFT JOIN dwh.dimlocation l
            ON w."CityName" = l.cityname;

        -- ========= FASE 2.3: LOAD TABEL FAKTA (Gabungan dari semua) =========
        INSERT INTO dwh.factsales (
            dateid, weatherid, productid, customerid,
            employeeid, locationid,
            quantity, totalprice, discount
        )
        SELECT
            d.dateid,
            w.weatherid,
            p.productid,
            c.customerid,
            e.employeeid,
            l.locationid,
            s."Quantity"::INT,
            s."TotalPrice"::DECIMAL(10, 2),
            s."Discount"::DECIMAL(10, 2)
        FROM staging.sales_mentah s
        LEFT JOIN dwh.dimdate d
            ON s."SalesDate"::DATE = d.fulldate
        LEFT JOIN dwh.dimproduct p
            ON s."ProductID" = p.productid_oltp
        LEFT JOIN dwh.dimcustomer c
            ON s."CustomerID" = c.customerid_oltp
        LEFT JOIN dwh.dimemployee e
            ON s."SalesPersonID" = e.employeeid_oltp
        LEFT JOIN dwh.dimlocation l
            ON c.customercityname = l.cityname
        LEFT JOIN dwh.dimweather w
            ON d.dateid = w.dateid AND l.locationid = w.locationid;
        """
        
        # Jalankan satu kueri besar ini di database
        with engine.begin() as conn:  
            conn.execute(text(transform_sql))
            
        logging.info("FASE 2 (Transformasi) SELESAI.")

    except Exception as e:
        logging.error(f"Error selama proses ELT: {e}")
        raise e

# --- PEMANGGIL FUNGSI ---
if __name__ == "__main__":
    try:
        logging.info("=== MEMULAI SKRIP ETL ===")
        run_elt() # Memanggil fungsi yang kamu definisikan di atas
        logging.info("=== SKRIP ETL SELESAI ===")
    except Exception as e:
        # Menangkap error dari run_elt()
        logging.error("=== SKRIP ETL GAGAL ===")
        exit(1) # Pastikan Docker tahu skrip ini gagal