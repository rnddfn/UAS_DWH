#!/usr/bin/env python3
"""
transform.py (revisi)
- Membaca tabel dari schema staging (hasil extract)
- Cleaning, normalisasi, mapping kolom ke format yang dipakai oleh load.py
- Menulis hasil ke schema staging_clean dengan nama kolom lowercase sesuai contract
"""

import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----- DB connection (dari env) -----
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")
DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
DB_NAME = os.environ.get("POSTGRES_DB")

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    logging.error("Environment variables POSTGRES_USER/POSTGRES_PASSWORD/POSTGRES_HOST/POSTGRES_DB harus diset.")
    raise SystemExit(1)

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}", future=True)

# Pastikan schema staging_clean ada
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging_clean;"))

# Helper: tulis ke staging_clean
def write_clean(df: pd.DataFrame, table_name: str):
    # pastikan kolom order tidak penting; to_sql akan replace table
    df.to_sql(table_name, con=engine, schema="staging_clean", if_exists="replace", index=False)
    logging.info("[CLEAN OK] staging_clean.%s rows=%d", table_name, len(df))

# Helper kecil
def df_from_staging(table: str) -> pd.DataFrame:
    sql = f'SELECT * FROM staging."{table}"' if table.lower() != table else f"SELECT * FROM staging.{table}"
    # Some staging tables have quoted mixed-case column names; pandas.read_sql handles them fine.
    return pd.read_sql(sql, con=engine)

# ========== TRANSFORM DIMDATE ==========
def clean_dimdate():
    # read source (dimdate_mentah expected to have "DateID","FullDate",...)
    try:
        df = pd.read_sql('SELECT * FROM staging.dimdate_mentah', con=engine)
    except Exception:
        logging.warning("staging.dimdate_mentah not found or empty -> creating from scratch aborted")
        df = pd.DataFrame()

    if df.empty:
        logging.warning("dimdate_mentah is empty; creating empty dimdate_clean")
        df_clean = pd.DataFrame(columns=["dateid","fulldate","day","month","monthname","quarter","year","dayofweek","isholiday","holidayname"])
        write_clean(df_clean, "dimdate_clean")
        return

    # normalize types
    # FullDate may be datetime or string; convert to date
    if "FullDate" in df.columns:
        df["FullDate"] = pd.to_datetime(df["FullDate"], errors="coerce").dt.date

    df_clean = pd.DataFrame({
        "dateid": df["DateID"].astype("Int64") if "DateID" in df.columns else df["FullDate"].apply(lambda d: int(d.strftime("%Y%m%d")) if pd.notna(d) else pd.NA),
        "fulldate": df["FullDate"],
        "day": df["Day"] if "Day" in df.columns else df["FullDate"].apply(lambda d: d.day if pd.notna(d) else pd.NA),
        "month": df["Month"] if "Month" in df.columns else df["FullDate"].apply(lambda d: d.month if pd.notna(d) else pd.NA),
        "monthname": df.get("MonthName"),
        "quarter": df.get("Quarter"),
        "year": df.get("Year"),
        "dayofweek": df.get("DayOfWeek"),
        "isholiday": df.get("IsHoliday").fillna(False) if "IsHoliday" in df.columns else False,
        "holidayname": df.get("HolidayName").fillna("") if "HolidayName" in df.columns else ""
    })

    # make sure dateid is int (no decimals)
    df_clean["dateid"] = df_clean["dateid"].astype("Int64")
    write_clean(df_clean, "dimdate_clean")

# ========== TRANSFORM CITIES -> dimlocation ==========
def clean_cities():
    # load cities and countries to enrich countryname
    try:
        df_cities = pd.read_sql('SELECT * FROM staging.cities_mentah', con=engine)
    except Exception:
        df_cities = pd.DataFrame()

    try:
        df_countries = pd.read_sql('SELECT * FROM staging.countries_mentah', con=engine)
    except Exception:
        df_countries = pd.DataFrame()

    if df_cities.empty:
        logging.warning("staging.cities_mentah empty -> creating empty cities_clean")
        write_clean(pd.DataFrame(columns=["cityid_oltp","locationid","cityname","countryname"]), "cities_clean")
        return

    # normalize CityName
    if "CityName" in df_cities.columns:
        df_cities["CityName"] = df_cities["CityName"].astype(str).str.strip().str.title()

    # map CountryName via CountryID if possible
    if "CountryID" in df_cities.columns and not df_countries.empty and "CountryID" in df_countries.columns and "CountryName" in df_countries.columns:
        df_merged = df_cities.merge(df_countries[["CountryID","CountryName"]], on="CountryID", how="left")
        df_merged["CountryName"] = df_merged["CountryName"].fillna("")

    else:
        df_merged = df_cities.copy()
        df_merged["CountryName"] = df_merged.get("CountryName", "")

    df_clean = pd.DataFrame({
        "cityid_oltp": df_merged["CityID"],
        "locationid": df_merged["CityID"],   # using CityID as locationid surrogate
        "cityname": df_merged["CityName"],
        "countryname": df_merged["CountryName"]
    })

    write_clean(df_clean, "cities_clean")

# ========== TRANSFORM PRODUCTS ==========
def clean_products():
    df = pd.read_sql('SELECT * FROM staging.products_mentah', con=engine)

    if df.empty:
        write_clean(pd.DataFrame(columns=["productid_oltp","productname","price","categoryname","class","isallergic"]), "products_clean")
        return

    # join categories to get CategoryName if needed
    try:
        df_cat = pd.read_sql('SELECT * FROM staging.categories_mentah', con=engine)
    except Exception:
        df_cat = pd.DataFrame()

    if not df_cat.empty and "CategoryID" in df.columns and "CategoryID" in df_cat.columns:
        df = df.merge(df_cat[["CategoryID","CategoryName"]], on="CategoryID", how="left")

    # normalize
    df["ProductName"] = df["ProductName"].astype(str).str.strip()
    df["Price"] = pd.to_numeric(df.get("Price", pd.Series()), errors="coerce").fillna(0.0)
    df["Class"] = df.get("Class")
    df["IsAllergic"] = df.get("IsAllergic", False)

    df_clean = pd.DataFrame({
        "productid_oltp": df["ProductID"],
        "productname": df["ProductName"],
        "price": df["Price"],
        "categoryname": df.get("CategoryName"),
        "class": df.get("Class"),
        "isallergic": df.get("IsAllergic")
    })

    write_clean(df_clean, "products_clean")

# ========== TRANSFORM CUSTOMERS ==========
def clean_customers():
    df = pd.read_sql('SELECT * FROM staging.customers_mentah', con=engine)

    if df.empty:
        write_clean(pd.DataFrame(columns=["customerid_oltp","customername","address","customercityname","customercountryname"]), "customers_clean")
        return

    # join cities -> to get CityName and CountryName
    try:
        df_cities = pd.read_sql('SELECT * FROM staging.cities_mentah', con=engine)
        df_countries = pd.read_sql('SELECT * FROM staging.countries_mentah', con=engine)
    except Exception:
        df_cities = pd.DataFrame()
        df_countries = pd.DataFrame()

    df_join = df.copy()
    if "CityID" in df_join.columns and not df_cities.empty:
        df_join = df_join.merge(df_cities[["CityID","CityName","CountryID"]], on="CityID", how="left")
        if not df_countries.empty and "CountryID" in df_countries.columns:
            df_join = df_join.merge(df_countries[["CountryID","CountryName"]], on="CountryID", how="left")
            df_join["CountryName"] = df_join["CountryName"].fillna("")
        else:
            df_join["CountryName"] = ""
    else:
        df_join["CityName"] = ""
        df_join["CountryName"] = ""

    # customername: prefer FirstName + ' ' + LastName if available
    if "FirstName" in df_join.columns and "LastName" in df_join.columns:
        df_join["CustomerName"] = (df_join["FirstName"].fillna("").astype(str).str.strip() + " " + df_join["LastName"].fillna("").astype(str).str.strip()).str.strip()
    elif "FirstName" in df_join.columns:
        df_join["CustomerName"] = df_join["FirstName"].astype(str).str.strip()
    else:
        df_join["CustomerName"] = ""

    df_clean = pd.DataFrame({
        "customerid_oltp": df_join["CustomerID"],
        "customername": df_join["CustomerName"],
        "address": df_join.get("Address"),
        "customercityname": df_join.get("CityName"),
        "customercountryname": df_join.get("CountryName")
    })

    write_clean(df_clean, "customers_clean")

# ========== TRANSFORM EMPLOYEES ==========
def clean_employees():
    df = pd.read_sql('SELECT * FROM staging.employees_mentah', con=engine)

    if df.empty:
        write_clean(pd.DataFrame(columns=["employeeid_oltp","employeename","gender","hiredate"]), "employees_clean")
        return

    df["FirstName"] = df.get("FirstName").astype(str).str.strip()
    df["LastName"] = df.get("LastName", "").astype(str).str.strip()
    df["EmployeeName"] = (df["FirstName"].fillna("") + " " + df["LastName"].fillna("")).str.strip()
    df["Gender"] = df.get("Gender").astype(str).str.upper()
    df["HireDate"] = pd.to_datetime(df.get("HireDate"), errors="coerce").dt.date

    df_clean = pd.DataFrame({
        "employeeid_oltp": df["EmployeeID"],
        "employeename": df["EmployeeName"],
        "gender": df["Gender"],
        "hiredate": df["HireDate"]
    })

    write_clean(df_clean, "employees_clean")

# ========== TRANSFORM WEATHER ==========
def clean_weather():
    df = pd.read_sql('SELECT * FROM staging.weather_mentah', con=engine)

    if df.empty:
        write_clean(pd.DataFrame(columns=["dateid","locationid","temperature_c","feelslike_c","wind_kph","precip_mm","isday"]), "weather_clean")
        return

    # normalize cityname and date/time
    df["CityName"] = df.get("CityName").astype(str).str.strip().str.title()
    # convert 'time' to date (if column name is 'time' or 'Time' try both)
    time_col = None
    for c in ["time","Time","date","Date"]:
        if c in df.columns:
            time_col = c
            break
    if time_col:
        df["__date"] = pd.to_datetime(df[time_col], errors="coerce").dt.date
    else:
        df["__date"] = pd.NaT

    # derive dateid as YYYYMMDD int
    df["dateid"] = df["__date"].apply(lambda d: int(d.strftime("%Y%m%d")) if pd.notna(d) else pd.NA)

    # find locationid via cities table
    try:
        df_cities = pd.read_sql('SELECT * FROM staging.cities_mentah', con=engine)
    except Exception:
        df_cities = pd.DataFrame()
    if not df_cities.empty:
        # CityName join
        df_cities["CityName"] = df_cities["CityName"].astype(str).str.strip().str.title()
        df = df.merge(df_cities[["CityID","CityName"]].rename(columns={"CityID":"locationid"}), left_on="CityName", right_on="CityName", how="left")
    else:
        df["locationid"] = pd.NA

    df_clean = pd.DataFrame({
        "dateid": df["dateid"].astype("Int64"),
        "locationid": df["locationid"].astype("Int64"),
        "temperature_c": pd.to_numeric(df.get("temperature_2m_max"), errors="coerce"),
        "feelslike_c": pd.to_numeric(df.get("feelslike_c"), errors="coerce") if "feelslike_c" in df.columns else pd.NA,
        "wind_kph": pd.to_numeric(df.get("windspeed_10m_max"), errors="coerce"),
        "precip_mm": pd.to_numeric(df.get("precipitation_sum"), errors="coerce"),
        "isday": pd.NA
    })

    write_clean(df_clean, "weather_clean")

# ========== TRANSFORM SALES ==========
def clean_sales():
    df = pd.read_sql('SELECT * FROM staging.sales_mentah', con=engine)

    if df.empty:
        write_clean(pd.DataFrame(columns=["salesdate","productid","customerid","salespersonid","quantity","totalprice","discount"]), "sales_clean")
        return

    # normalize column names to lower-case for load
    # Many staging CSVs use mixed-case column headers; map them to expected lowercase
    mapping = {}
    for col in df.columns:
        lc = col.lower()
        if lc == "salesdate":
            mapping[col] = "salesdate"
        elif lc in ("productid","product_id"):
            mapping[col] = "productid"
        elif lc in ("customerid","customer_id"):
            mapping[col] = "customerid"
        elif lc in ("salespersonid","salesperson_id","employeeid"):
            mapping[col] = "salespersonid"
        elif lc == "quantity":
            mapping[col] = "quantity"
        elif lc in ("totalprice","total_price","price"):
            mapping[col] = "totalprice"
        elif lc == "discount":
            mapping[col] = "discount"

    df = df.rename(columns=mapping)

    # ensure columns exist
    for c in ["salesdate","productid","customerid","salespersonid","quantity","totalprice","discount"]:
        if c not in df.columns:
            df[c] = pd.NA

    df["salesdate"] = pd.to_datetime(df["salesdate"], errors="coerce").dt.date
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype("Int64")
    df["totalprice"] = pd.to_numeric(df["totalprice"], errors="coerce").fillna(0.0)
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0.0)

    # final ensure lowercase columns
    df_clean = df[["salesdate","productid","customerid","salespersonid","quantity","totalprice","discount"]].copy()

    write_clean(df_clean, "sales_clean")

# ========== RUN ALL ==========
def run_transform():
    logging.info("Starting TRANSFORM (cleaning) -> staging_clean ...")
    clean_dimdate()
    clean_cities()
    clean_products()
    clean_customers()
    clean_employees()
    clean_weather()
    clean_sales()
    logging.info("TRANSFORM complete. staging_clean schema populated.")

if __name__ == "__main__":
    run_transform()