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
        # Reset schema staging agar bersih dari tabel mentah sisa eksekusi lama
        conn.execute(text("DROP SCHEMA IF EXISTS staging CASCADE;"))
        conn.execute(text("CREATE SCHEMA staging;"))
        
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS datalake;")) # <-- Tambahan Data Lake
        conn.commit() 
    logging.info("Schema 'staging' (bersih) dan 'datalake' dipastikan ada.")
    
    # Buat schema dan tabel DWH dari scheme.sql
    with open('scheme.sql', 'r') as f:
        schema_sql = f.read()
    
    with engine.connect() as conn:
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
        
        # Load Calendar ke Data Lake (Tanpa Merge)
        df_calendar['FullDate'] = df_calendar['FullDate'].dt.date
        df_calendar.to_sql('calendar_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        logging.info("Berhasil memuat datalake.calendar_mentah.")

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
        
        # Load Holidays ke Data Lake (Tabel Terpisah)
        df_holidays_grouped.to_sql('holidays_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        logging.info("Berhasil memuat datalake.holidays_mentah.")
        
    except Exception as e:
        logging.error(f"Error memuat DimDate: {e}")
        raise e

def validate_dwh_counts():
    """
    Memvalidasi apakah data berhasil masuk ke tabel DWH.
    """
    logging.info("Validating DWH row counts...")
    tables = [
        'dwh.factsales', 'dwh.dimproduct', 'dwh.dimcustomer', 
        'dwh.dimemployee', 'dwh.dimlocation', 'dwh.dimdate', 'dwh.dimweather'
    ]
    
    with engine.connect() as conn:
        for table in tables:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                logging.info(f"Table {table} has {count} rows.")
                if count == 0:
                    logging.warning(f"⚠️ Table {table} is empty!")
            except Exception as e:
                logging.error(f"Could not validate table {table}: {e}")

# --- FUNGSI UTAMA ---
def run_elt():
    try:
        # FASE 1: "Load ke Data Lake" (Ini adalah proses E-L)
        logging.info("Memulai Extract & Load CSV ke Data Lake...")

        # Baca CSV, Load ke Data Lake
        df_products = pd.read_csv('./data/raw/products.csv')
        df_products.to_sql('products_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        del df_products
        logging.info("Berhasil memuat datalake.products_mentah.") 
        
        df_categories = pd.read_csv('./data/raw/categories.csv')
        df_categories.to_sql('categories_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        del df_categories
        logging.info("Berhasil memuat datalake.categories_mentah.")
        
        df_employees = pd.read_csv('./data/raw/employees.csv')
        df_employees.to_sql('employees_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        del df_employees
        logging.info("Berhasil memuat datalake.employees_mentah.")

        df_customers = pd.read_csv('./data/raw/customers.csv')
        df_customers.to_sql('customers_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        del df_customers
        logging.info("Berhasil memuat datalake.customers_mentah.")

        df_cities = pd.read_csv('./data/raw/cities_MODIFIED_with_coords.csv')
        df_cities.to_sql(
            'cities_mentah', 
            con=engine,         
            schema='datalake', 
            if_exists='replace', 
            index=False
        )
        del df_cities
        logging.info("Berhasil memuat datalake.cities_mentah.")

        df_countries = pd.read_csv('./data/raw/countries.csv')
        df_countries.to_sql('countries_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        del df_countries
        logging.info("Berhasil memuat datalake.countries_mentah.")
        
        df_weather = pd.read_csv('./data/raw/weather_mentah.csv')
        df_weather.to_sql('weather_mentah', con=engine, schema='datalake', if_exists='replace', index=False)
        del df_weather
        logging.info("Berhasil memuat datalake.weather_mentah.")

        # --- Load Sales (Gunakan chunking jika file besar) ---
        logging.info("Memproses file sales (chunking)...")
        chunk_size = 100000 
        sales_iterator = pd.read_csv('./data/raw/sales.csv', chunksize=chunk_size)
        
        # Hapus tabel lama (jika ada) HANYA SEKALI
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS datalake.sales_mentah;"))
            conn.commit()

        for i, chunk in enumerate(sales_iterator):
            logging.info(f"Memuat sales chunk {i+1}...")
            chunk.to_sql(
                'sales_mentah',
                con=engine,
                schema='datalake',
                if_exists='append', 
                index=False
            )
            del chunk
        logging.info("Berhasil memuat datalake.sales_mentah.")
        
        logging.info("FASE 1 (CSV) SELESAI: Data Lake terisi.")

        # --- FASE 1 (API) ---
        logging.info("Memulai Fase 1 (API)...")
        load_calendar_and_holidays_to_staging(year=2018, country_code='US')
        
        logging.info("FASE 1 (API) SELESAI.")
        
        # ==========================================================
        # FASE 2: TRANSFORMASI (T)
        # (TAMBAHKAN SEMUA KODE DI BAWAH INI)
        # ==========================================================
        logging.info("Memulai Fase 2: Transformasi (Data Lake ke Staging)...")
        
        # Ini adalah satu string SQL besar yang berisi semua logika transformasi
        transform_sql = """
        -- ========= FASE 2.1: BERSIHKAN STAGING (Agar Idempotent) =========
        DROP TABLE IF EXISTS staging.factsales CASCADE;
        DROP TABLE IF EXISTS staging.dimproduct CASCADE;
        DROP TABLE IF EXISTS staging.dimcustomer CASCADE;
        DROP TABLE IF EXISTS staging.dimemployee CASCADE;
        DROP TABLE IF EXISTS staging.dimlocation CASCADE;
        DROP TABLE IF EXISTS staging.dimdate CASCADE;
        DROP TABLE IF EXISTS staging.dimweather CASCADE;

        -- ========= FASE 2.2: TRANSFORMASI KE STAGING (dari Data Lake) =========

        -- 2.2.1. Transform ke staging.dimdate
        CREATE TABLE staging.dimdate AS
        SELECT
            c."DateID" as dateid,
            c."FullDate" as fulldate,
            c."Day" as day,
            c."Month" as month,
            c."MonthName" as monthname,
            c."Quarter" as quarter,
            c."Year" as year,
            c."DayOfWeek" as dayofweek,
            CASE WHEN h."HolidayName" IS NOT NULL THEN TRUE ELSE FALSE END as isholiday,
            COALESCE(h."HolidayName", '') as holidayname
        FROM datalake.calendar_mentah c
        LEFT JOIN datalake.holidays_mentah h ON c."FullDate" = h."FullDate";

        -- 2.2.2. Transform ke staging.dimlocation
        CREATE TABLE staging.dimlocation AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY "CityID") as locationid,
            "CityID" as cityid_oltp,
            TRIM("CityName") as cityname,
            'United States' as countryname
        FROM (SELECT DISTINCT * FROM datalake.cities_mentah) c;

        -- 2.2.3. Transform ke staging.dimproduct
        CREATE TABLE staging.dimproduct AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY p."ProductID") as productid,
            p."ProductID" as productid_oltp,
            TRIM(p."ProductName") as productname,
            p."Price" as price,
            TRIM(c."CategoryName") as categoryname,
            TRIM(p."Class") as class,
            TRIM(p."IsAllergic") as isallergic
        FROM (SELECT DISTINCT * FROM datalake.products_mentah) p
        LEFT JOIN (SELECT DISTINCT * FROM datalake.categories_mentah) c ON p."CategoryID" = c."CategoryID";

        -- 2.2.4. Transform ke staging.dimcustomer
        CREATE TABLE staging.dimcustomer AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY c."CustomerID") as customerid,
            c."CustomerID" as customerid_oltp,
            TRIM(c."FirstName") as customername,
            TRIM(c."Address") as address,
            TRIM(ci."CityName") as customercityname,
            TRIM(co."CountryName") as customercountryname
        FROM (SELECT DISTINCT * FROM datalake.customers_mentah) c
        LEFT JOIN (SELECT DISTINCT * FROM datalake.cities_mentah) ci ON c."CityID" = ci."CityID"
        LEFT JOIN (SELECT DISTINCT * FROM datalake.countries_mentah) co ON ci."CountryID" = co."CountryID";

        -- 2.2.5. Transform ke staging.dimemployee
        CREATE TABLE staging.dimemployee AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY "EmployeeID") as employeeid,
            "EmployeeID" as employeeid_oltp,
            TRIM("FirstName") as employeename,
            TRIM("Gender") as gender,
            "HireDate"::DATE as hiredate
        FROM (SELECT DISTINCT * FROM datalake.employees_mentah) e;

        -- 2.2.6. Transform ke staging.dimweather
        CREATE TABLE staging.dimweather AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY w."time", w."CityName") as weatherid,
            NULL::text AS condition,
            w."temperature_2m_max"        AS temperature_c,
            NULL::float                   AS feelslike_c,
            w."windspeed_10m_max"        AS wind_kph,
            w."precipitation_sum"        AS precip_mm,
            NULL::int                    AS isday,
            d.dateid,
            l.locationid
        FROM (SELECT DISTINCT * FROM datalake.weather_mentah) w
        LEFT JOIN staging.dimdate d
            ON w."time"::DATE = d.fulldate
        LEFT JOIN staging.dimlocation l
            ON TRIM(w."CityName") = l.cityname;

        -- ========= FASE 2.3: TRANSFORMASI FAKTA KE STAGING =========
        CREATE TABLE staging.factsales AS
        SELECT
            d.dateid,
            w.weatherid,
            p.productid,
            c.customerid,
            e.employeeid,
            l.locationid,
            s."Quantity"::INT as quantity,
            -- Hitung TotalPrice karena di CSV nilainya 0
            (s."Quantity"::INT * p.price * (1 - COALESCE(s."Discount"::DECIMAL(10, 2), 0)))::DECIMAL(10, 2) as totalprice,
            s."Discount"::DECIMAL(10, 2) as discount
        FROM (SELECT DISTINCT * FROM datalake.sales_mentah) s
        LEFT JOIN staging.dimdate d
            ON s."SalesDate"::DATE = d.fulldate
        LEFT JOIN staging.dimproduct p
            ON s."ProductID" = p.productid_oltp
        LEFT JOIN staging.dimcustomer c
            ON s."CustomerID" = c.customerid_oltp
        LEFT JOIN staging.dimemployee e
            ON s."SalesPersonID" = e.employeeid_oltp
        LEFT JOIN staging.dimlocation l
            ON c.customercityname = l.cityname
        LEFT JOIN staging.dimweather w
            ON d.dateid = w.dateid AND l.locationid = w.locationid;
        """
        
        # Jalankan satu kueri besar ini di database
        with engine.begin() as conn:  
            conn.execute(text(transform_sql))
            
        logging.info("FASE 2: Transformasi SELESAI.")

        # ========= FASE 2.5: DATA QUALITY CHECKS (GOVERNANCE) =========
        logging.info("=== Memulai Data Quality Checks (Governance) ===")
        dq_checks = [
            {
                "name": "Negative Quantity Check",
                "query": "SELECT COUNT(*) FROM staging.factsales WHERE quantity < 0",
                "threshold": 0
            },
            {
                "name": "Negative Price Check",
                "query": "SELECT COUNT(*) FROM staging.factsales WHERE totalprice < 0",
                "threshold": 0
            },
            {
                "name": "Null Product ID in Fact",
                "query": "SELECT COUNT(*) FROM staging.factsales WHERE productid IS NULL",
                "threshold": 0
            },
             {
                "name": "Null Customer ID in Fact",
                "query": "SELECT COUNT(*) FROM staging.factsales WHERE customerid IS NULL",
                "threshold": 0
            }
        ]
        
        with engine.connect() as conn:
            dq_failed = False
            for check in dq_checks:
                result = conn.execute(text(check['query'])).scalar()
                if result > check['threshold']:
                    logging.error(f"DQ FAILED: {check['name']} found {result} bad rows.")
                    dq_failed = True
                else:
                    logging.info(f"DQ PASSED: {check['name']}")
            
            if dq_failed:
                raise ValueError("Data Quality Checks Failed! Pipeline dihentikan sebelum Load ke DWH.")

        logging.info("=== Data Quality Checks Selesai (PASSED) ===")

        # ========= FASE 3: LOAD KE DWH (Final) =========
        load_sql = """
        TRUNCATE TABLE dwh.factsales RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimproduct RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimcustomer RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimemployee RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimlocation RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimdate RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dwh.dimweather RESTART IDENTITY CASCADE;

        INSERT INTO dwh.dimdate SELECT * FROM staging.dimdate;
        INSERT INTO dwh.dimlocation (locationid, cityid_oltp, cityname, countryname) SELECT locationid, cityid_oltp, cityname, countryname FROM staging.dimlocation;
        INSERT INTO dwh.dimproduct (productid, productid_oltp, productname, price, categoryname, class, isallergic) SELECT productid, productid_oltp, productname, price, categoryname, class, isallergic FROM staging.dimproduct;
        INSERT INTO dwh.dimcustomer (customerid, customerid_oltp, customername, address, customercityname, customercountryname) SELECT customerid, customerid_oltp, customername, address, customercityname, customercountryname FROM staging.dimcustomer;
        INSERT INTO dwh.dimemployee (employeeid, employeeid_oltp, employeename, gender, hiredate) SELECT employeeid, employeeid_oltp, employeename, gender, hiredate FROM staging.dimemployee;
        INSERT INTO dwh.dimweather (weatherid, condition, temperature_c, feelslike_c, wind_kph, precip_mm, isday, dateid, locationid) SELECT weatherid, condition, temperature_c, feelslike_c, wind_kph, precip_mm, isday, dateid, locationid FROM staging.dimweather;
        
        INSERT INTO dwh.factsales (dateid, weatherid, productid, customerid, employeeid, locationid, quantity, totalprice, discount) 
        SELECT dateid, weatherid, productid, customerid, employeeid, locationid, quantity, totalprice, discount FROM staging.factsales;
        """
        
        with engine.begin() as conn:
            conn.execute(text(load_sql))

        logging.info("FASE 3: Load ke DWH SELESAI.")
        validate_dwh_counts()

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