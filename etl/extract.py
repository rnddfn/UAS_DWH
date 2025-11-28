#!/usr/bin/env python3
"""
extract.py
- Baca CSV dari folder data/raw
- Load ke schema staging di Postgres
- Fast-ish inserts (method='multi') dan chunking untuk sales
- Option: --clear-staging untuk truncate semua staging tables before load
"""

import os
import logging
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# env / connection
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_NAME = os.environ.get("POSTGRES_DB")

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    logging.error("Environment variables POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_HOST/POSTGRES_DB harus diset.")
    raise SystemExit(1)

CONN_STR = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
engine = create_engine(CONN_STR)

RAW_DIR = "./data/raw"

# helper untuk cek file
def ensure_file(path):
    if not os.path.exists(path):
        logging.error("File tidak ditemukan: %s", path)
        raise FileNotFoundError(path)

def clear_staging_tables(conn):
    logging.info("Clearing all tables in schema 'staging' (truncate)...")
    conn.execute(text("""
    DO $$
    DECLARE r RECORD;
    BEGIN
      FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'staging' LOOP
        EXECUTE 'TRUNCATE TABLE staging.' || quote_ident(r.tablename) || ' CASCADE';
      END LOOP;
    END $$;
    """))
    logging.info("Staging truncated.")

def load_table_from_csv(df, table_name, replace=True):
    """Load pandas df to staging.table_name. replace -> if_exists 'replace' else 'append'."""
    if_exists_mode = "replace" if replace else "append"
    # method='multi' speeds up inserts; adjust chunksize if needed
    df.to_sql(table_name, con=engine, schema="staging", if_exists=if_exists_mode, index=False, method="multi", chunksize=1000)
    logging.info("Wrote %d rows to staging.%s (mode=%s)", len(df), table_name, if_exists_mode)

def load_sales_in_chunks(csv_path, chunk_size=100_000):
    ensure_file(csv_path)
    # drop old table first to avoid duplicates
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS staging.sales_mentah;"))
    it = pd.read_csv(csv_path, chunksize=chunk_size)
    for i, chunk in enumerate(it, start=1):
        # convert types lightly if needed here (optional)
        chunk.to_sql("sales_mentah", con=engine, schema="staging", if_exists="append", index=False, method="multi", chunksize=1000)
        logging.info("Loaded sales chunk %d, rows=%d", i, len(chunk))

def run_extract(clear_staging_flag: bool = False):
    logging.info("Starting Extract phase: load CSVs into staging...")
    # ensure staging schema present
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        logging.info("Schema 'staging' ensured.")
        if clear_staging_flag:
            clear_staging_tables(conn)

    # non-sales files -> load entirely and replace so re-run is idempotent
    files_to_replace = {
        "products_mentah": "products.csv",
        "categories_mentah": "categories.csv",
        "employees_mentah": "employees.csv",
        "customers_mentah": "customers.csv",
        "cities_mentah": "cities_MODIFIED_with_coords.csv",  # choose correct file name
        "countries_mentah": "countries.csv",
        "weather_mentah": "weather_mentah.csv",
        "dimdate_mentah": None,  # produced via API function if you have one
    }

    for table, fname in files_to_replace.items():
        if fname is None:
            continue  # skip if file not present; dimdate handled elsewhere
        path = os.path.join(RAW_DIR, fname)
        ensure_file(path)
        logging.info("Reading %s -> %s", path, table)
        df = pd.read_csv(path)
        load_table_from_csv(df, table, replace=True)
        del df

    # sales -> chunking append after dropping previous table
    sales_path = os.path.join(RAW_DIR, "sales.csv")
    load_sales_in_chunks(sales_path, chunk_size=100_000)

    logging.info("Extract finished: staging area populated.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear-staging", action="store_true", help="Truncate staging before load")
    args = parser.parse_args()
    try:
        run_extract(clear_staging_flag=args.clear_staging)
    except Exception as e:
        logging.exception("Extract failed: %s", e)
        raise