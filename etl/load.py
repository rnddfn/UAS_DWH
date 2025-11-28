#!/usr/bin/env python3
"""
load.py

- Baca tabel dari schema staging_clean
- Upsert ke dwh.dim* menggunakan ON CONFLICT (asumsi unique constraints ada)
- Insert ke dwh.factsales (opsional: truncate facts sebelum insert)
- Semua dijalankan di 1 transaksi
"""

import os
import logging
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_NAME = os.environ.get("POSTGRES_DB")

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    logging.error("Set POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_HOST/POSTGRES_DB in env")
    raise SystemExit(1)

CONN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
engine = create_engine(CONN)

DWH = "dwh"
STAGING_CLEAN = "staging_clean"

def upsert_dim(conn, tmp_table, target_table, target_cols, conflict_cols):
    """
    Generic upsert from staging_clean.tmp_table -> dwh.target_table
    target_cols: list of columns in target order (and present in tmp_table)
    conflict_cols: list of columns that form unique key in target_table
    """
    cols = ", ".join(target_cols)
    select_cols = ", ".join([f"t.{c}" for c in target_cols])
    updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in target_cols if c not in conflict_cols])
    sql = f"""
    INSERT INTO {DWH}.{target_table} ({cols})
    SELECT {select_cols} FROM {STAGING_CLEAN}.{tmp_table} t
    ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE
      SET {updates};
    """
    logging.info("Upserting into %s from %s", target_table, tmp_table)
    conn.execute(text(sql))

def load_dimensions(conn):
    # dimdate: expects staging_clean.dimdate_clean with DateID, FullDate, ...
    upsert_dim(conn,
        tmp_table="dimdate_clean",
        target_table="dimdate",
        target_cols=["dateid","fulldate","day","month","monthname","quarter","year","dayofweek","isholiday","holidayname"],
        conflict_cols=["dateid"]
    )

    # dimlocation (cities)
    # We assume staging_clean.cities_clean has CityID, CityName, CountryName(optional)
    upsert_dim(conn,
        tmp_table="cities_clean",
        target_table="dimlocation",
        target_cols=["cityid_oltp","locationid","cityname","countryname"],
        conflict_cols=["cityid_oltp"]
    )

    # dimproduct
    upsert_dim(conn,
        tmp_table="products_clean",
        target_table="dimproduct",
        target_cols=["productid_oltp","productname","price","categoryname","class","isallergic"],
        conflict_cols=["productid_oltp"]
    )

    # dimcustomer
    upsert_dim(conn,
        tmp_table="customers_clean",
        target_table="dimcustomer",
        target_cols=["customerid_oltp","customername","address","customercityname","customercountryname"],
        conflict_cols=["customerid_oltp"]
    )

    # dimemployee
    upsert_dim(conn,
        tmp_table="employees_clean",
        target_table="dimemployee",
        target_cols=["employeeid_oltp","employeename","gender","hiredate"],
        conflict_cols=["employeeid_oltp"]
    )

    # dimweather - assume tmp has dateid and locationid mapped already (or temperature fields)
    # We'll map columns conservatively; ensure staging_clean.weather_clean has dateid and locationid or CityID
    upsert_dim(conn,
        tmp_table="weather_clean",
        target_table="dimweather",
        target_cols=["dateid","locationid","temperature_c","feelslike_c","wind_kph","precip_mm","isday"],
        conflict_cols=["dateid","locationid"]
    )

def load_facts(conn, truncate_facts=False):
    if truncate_facts:
        logging.info("Truncating dwh.factsales (truncate_facts=True)")
        conn.execute(text(f"TRUNCATE TABLE {DWH}.factsales RESTART IDENTITY CASCADE;"))

    # Insert facts: join tmp_sales to dims for FK lookup
    # Note: adjust column names if different in your staging_clean.tmp_sales
    sql = f"""
    INSERT INTO {DWH}.factsales (
        dateid, weatherid, productid, customerid,
        employeeid, locationid, quantity, totalprice, discount
    )
    SELECT
        d.dateid,
        w.weatherid,
        p.productid,
        c.customerid,
        e.employeeid,
        l.locationid,
        s.quantity::INT,
        s.totalprice::DECIMAL(10,2),
        s.discount::DECIMAL(10,2)
    FROM {STAGING_CLEAN}.sales_clean s
    LEFT JOIN {DWH}.dimdate d ON s.salesdate::DATE = d.fulldate
    LEFT JOIN {DWH}.dimproduct p ON s.productid = p.productid_oltp
    LEFT JOIN {DWH}.dimcustomer c ON s.customerid = c.customerid_oltp
    LEFT JOIN {DWH}.dimemployee e ON s.salespersonid = e.employeeid_oltp
    LEFT JOIN {DWH}.dimlocation l ON c.customercityname = l.cityname
    LEFT JOIN {DWH}.dimweather w ON d.dateid = w.dateid AND l.locationid = w.locationid
    ;
    """
    logging.info("Inserting factsales from staging_clean.sales_clean")
    conn.execute(text(sql))


def run_load(truncate_facts=False):
    try:
        with engine.begin() as conn:
            logging.info("Starting LOAD transaction (dimensions -> facts)...")
            load_dimensions(conn)
            load_facts(conn, truncate_facts=truncate_facts)
            logging.info("LOAD completed (transaction will be committed).")
    except SQLAlchemyError as e:
        logging.exception("DB error during load; transaction rolled back.")
        raise
    except Exception as e:
        logging.exception("Unexpected error during load.")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load staging_clean -> dwh")
    parser.add_argument("--truncate-facts", action="store_true", help="Truncate facts table before inserting")
    args = parser.parse_args()
    run_load(truncate_facts=args.truncate_facts)