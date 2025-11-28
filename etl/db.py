# etl/db.py
import os
import logging
from sqlalchemy import create_engine, text

# --- 1. Konfigurasi Logging (Dipindah ke sini) ---
os.makedirs('logs', exist_ok=True) 

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_execution.log'),
        logging.StreamHandler()
    ]
)

engine = None

def get_engine():
    """Membuat atau mengembalikan engine SQLAlchemy yang sudah ada."""
    global engine
    if engine:
        return engine

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