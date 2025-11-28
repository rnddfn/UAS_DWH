# etl.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import requests

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

# --- 2. Fungsi Koneksi (Adaptasi ke PostgreSQL & Docker) ---
def get_db_engine():
    """Membuat koneksi engine SQLAlchemy ke PostgreSQL."""
    try:
        db_user = os.environ.get('POSTGRES_USER')
        db_pass = os.environ.get('POSTGRES_PASSWORD')
        db_host = os.environ.get('POSTGRES_HOST')
        db_name = os.environ.get('POSTGRES_DB')
        
        connection_string = f"postgresql://{db_user}:{db_pass}@{db_host}:5432/{db_name}"
        engine = create_engine(connection_string)
        
        # Memastikan skema 'staging' ada
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
            conn.commit() 
        logging.info("Koneksi DB (engine) berhasil dibuat dan schema 'staging' dipastikan ada.")
        return engine
        
    except Exception as e:
        logging.error(f"Koneksi database GAGAL: {e}")
        exit(1)

# --- 3. FASE 1: EXTRACT & LOAD (E-L) ---
def extract_and_load_to_staging(engine):
    """Mengekstrak data dari CSV & API, lalu memuat ke schema 'staging'."""
    logging.info("--- Memulai Fase 1: Extract & Load ---")
    
    try:
        # --- 3.1. Load CSVs ---
        logging.info("Memuat CSVs ke Staging...")

        # Daftar file CSV yang akan dimuat
        csv_files_to_load = {
            'products.csv': 'products_mentah',
            'categories.csv': 'categories_mentah',
            'employees.csv': 'employees_mentah',
            'customers.csv': 'customers_mentah',
            'cities_MODIFIED_with_coords.csv': 'cities_mentah', # File hasil 'map_real_cities.py'
            'countries.csv': 'countries_mentah',
            'cuaca_master_2018.csv': 'weather_mentah' # File hasil 'download_weather.py'
        }
        
        for file_name, table_name in csv_files_to_load.items():
            path = f'./data/raw/{file_name}'
            df = pd.read_csv(path)
            df.to_sql(table_name, con=engine, schema='staging', if_exists='replace', index=False)
            logging.info(f"Berhasil memuat staging.{table_name}.")
            del df # Hemat memori

        # --- 3.2. Load Sales (Chunking) ---
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
        
        # --- 3.3. Load API Data (Hari Libur) ---
        logging.info("Memuat data dari API (Hari Libur)...")
        load_calendar_to_staging(engine, year=2018, country_code='US')
        
        logging.info("--- Fase 1 (E-L) Selesai ---")

    except Exception as e:
        logging.error(f"Error selama Fase 1 (Extract-Load): {e}")
        raise e

def load_calendar_to_staging(engine, year=2018, country_code='US'):
    """Helper: Membuat kalender & mengambil API libur, lalu load ke staging."""
    try:
        logging.info(f"Membuat kalender untuk tahun {year}...")
        df_calendar = pd.DataFrame({"FullDate": pd.date_range(start=f'{year}-01-01', end=f'{year}-12-31')})
        df_calendar['DateID'] = df_calendar['FullDate'].dt.strftime('%Y%m%d').astype(int)
        df_calendar['Day'] = df_calendar['FullDate'].dt.day
        df_calendar['Month'] = df_calendar['FullDate'].dt.month
        df_calendar['MonthName'] = df_calendar['FullDate'].dt.month_name()
        df_calendar['Quarter'] = df_calendar['FullDate'].dt.quarter
        df_calendar['Year'] = df_calendar['FullDate'].dt.year
        df_calendar['DayOfWeek'] = df_calendar['FullDate'].dt.day_name()
        
        logging.info(f"Mengambil data libur untuk {country_code}...")
        response = requests.get(f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}")
        response.raise_for_status()
        
        holidays_data = response.json()
        df_holidays = pd.DataFrame(holidays_data)
        
        df_holidays['FullDate'] = pd.to_datetime(df_holidays['date']).dt.date
        df_holidays_grouped = df_holidays.groupby('FullDate')['name'].apply(
            lambda x: ', '.join(x)
        ).reset_index()
        df_holidays_grouped = df_holidays_grouped.rename(columns={'name': 'HolidayName'})
        df_holidays_grouped['IsHoliday'] = True
        
        df_calendar['FullDate'] = df_calendar['FullDate'].dt.date
        df_final_date = pd.merge(df_calendar, df_holidays_grouped, on='FullDate', how='left')
        df_final_date['IsHoliday'] = df_final_date['IsHoliday'].fillna(False)
        df_final_date['HolidayName'] = df_final_date['HolidayName'].fillna('')
        
        df_final_date.to_sql('dimdate_mentah', con=engine, schema='staging', if_exists='replace', index=False)
        logging.info("Berhasil memuat staging.dimdate_mentah.")
    except Exception as e:
        logging.error(f"Error memuat DimDate: {e}")
        raise e

# --- 4. FASE 2: TRANSFORMASI (T) - Load Dimensi ---
def transform_and_load_dimensions(engine):
    """Mengubah data staging dan memuat ke tabel Dimensi di DWH."""
    logging.info("--- Memulai Fase 2a: Transformasi Dimensi ---")
    
    dim_sql = """
    -- 1. Kosongkan Dimensi (TRUNCATE)
    TRUNCATE TABLE dwh.dimproduct RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dwh.dimcustomer RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dwh.dimemployee RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dwh.dimlocation RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dwh.dimdate RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dwh.dimweather RESTART IDENTITY CASCADE;

    -- 2. Load dwh.dimdate (dari staging.dimdate_mentah)
    INSERT INTO dwh.dimdate (
        dateid, fulldate, day, month, monthname,
        quarter, year, dayofweek, isholiday, holidayname
    )
    SELECT
        "DateID", "FullDate", "Day", "Month", "MonthName",
        "Quarter", "Year", "DayOfWeek", "IsHoliday", "HolidayName"
    FROM staging.dimdate_mentah;

    -- 3. Load dwh.dimlocation (dari staging.cities_mentah)
    INSERT INTO dwh.dimlocation (
        locationid, cityid_oltp, cityname, countryname
    )
    SELECT
        "CityID", "CityID", "CityName", 'United States'
    FROM staging.cities_mentah;

    -- 4. Load dwh.dimproduct (gabungan produk + kategori)
    INSERT INTO dwh.dimproduct (
        productid_oltp, productname, price,
        categoryname, class, isallergic
    )
    SELECT
        p."ProductID", p."ProductName", p."Price",
        c."CategoryName", p."Class", p."IsAllergic"
    FROM staging.products_mentah p
    LEFT JOIN staging.categories_mentah c ON p."CategoryID" = c."CategoryID";

    -- 5. Load dwh.dimcustomer
    INSERT INTO dwh.dimcustomer (
        customerid_oltp, customername, address,
        customercityname, customercountryname
    )
    SELECT
        c."CustomerID", c."FirstName", c."Address",
        ci."CityName", co."CountryName"
    FROM staging.customers_mentah c
    LEFT JOIN staging.cities_mentah ci ON c."CityID" = ci."CityID"
    LEFT JOIN staging.countries_mentah co ON ci."CountryID" = co."CountryID";

    -- 6. Load dwh.dimemployee
    INSERT INTO dwh.dimemployee (
        employeeid_oltp, employeename, gender, hiredate
    )
    SELECT
        "EmployeeID", "FirstName", "Gender", "HireDate"::DATE
    FROM staging.employees_mentah;

    -- 7. Load dwh.dimweather (dari staging.weather_mentah)
    INSERT INTO dwh.dimweather (
        condition, temperature_c, feelslike_c, wind_kph, precip_mm, isday,
        dateid, locationid
    )
    SELECT
        NULL AS condition,
        w."temperature_2m_max" AS temperature_c,
        NULL AS feelslike_c,
        w."windspeed_10m_max" AS wind_kph,
        w."precipitation_sum" AS precip_mm,
        NULL AS isday,
        d.dateid,
        l.locationid
    FROM staging.weather_mentah w
    LEFT JOIN dwh.dimdate d ON w."time"::DATE = d.fulldate
    LEFT JOIN dwh.dimlocation l ON w."CityName" = l.cityname;
    """
    
    try:
        # engine.begin() otomatis commit (jika sukses) atau rollback (jika error)
        with engine.begin() as conn:
            conn.execute(text(dim_sql))
        logging.info("FASE 2a (Transformasi Dimensi) SELESAI.")
    except Exception as e:
        logging.error(f"Error selama Fase 2a (Transformasi Dimensi): {e}")
        raise e

# --- 5. FASE 2: TRANSFORMASI (T) - Load Fakta ---
def load_fact_tables(engine):
    """Mengubah data staging dan memuat ke tabel Fakta di DWH."""
    logging.info("--- Memulai Fase 2b: Transformasi Fakta ---")
    
    fact_sql = """
    -- 1. Kosongkan Fakta (TRUNCATE)
    TRUNCATE TABLE dwh.factsales RESTART IDENTITY CASCADE;

    -- 2. Load dwh.factsales (Gabungan dari semua)
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
        ON s."SalesPersonID" = e.employeeid_oltp -- Pastikan nama kolom 'SalesPersonID' benar
    LEFT JOIN dwh.dimlocation l
        ON c.customercityname = l.cityname
    LEFT JOIN dwh.dimweather w
        ON d.dateid = w.dateid AND l.locationid = w.locationid;
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(fact_sql))
        logging.info("FASE 2b (Transformasi Fakta) SELESAI.")
    except Exception as e:
        logging.error(f"Error selama Fase 2b (Transformasi Fakta): {e}")
        raise e

# --- 6. PEMANGGIL FUNGSI (PIPELINE) ---
if __name__ == "__main__":
    try:
        logging.info("=== MEMULAI PIPELINE ELT LENGKAP ===")
        
        db_engine = get_db_engine()
        
        # JALANKAN FASE E-L
        extract_and_load_to_staging(db_engine)
        
        # JALANKAN FASE T (DIMENSI)
        transform_and_load_dimensions(db_engine)

        # JALANKAN FASE T (FAKTA)
        load_fact_tables(db_engine)
        
        logging.info("=== PIPELINE ELT LENGKAP SELESAI ===")
    
    except Exception as e:
        logging.error(f"=== PIPELINE ELT GAGAL: {e} ===")
        exit(1)