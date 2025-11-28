# utils/db.py
import os
from sqlalchemy import create_engine
import pandas as pd
 
def get_engine():
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    db = os.getenv("POSTGRES_DB")

    conn_str = f"postgresql://{user}:{pwd}@{host}:5432/{db}"
    return create_engine(conn_str)

def read_query(query):
    engine = get_engine()
    return pd.read_sql(query, engine)