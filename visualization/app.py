# app.py
import os
import streamlit as st
import streamlit as st
from utils.db import read_query
from utils.charts import bar_chart, line_chart


st.write("POSTGRES_HOST:", os.getenv("POSTGRES_HOST"))
st.write("POSTGRES_USER:", os.getenv("POSTGRES_USER"))
st.set_page_config(page_title="Sales Dashboard", layout="wide")

st.title("📊 Sales Analytics Dashboard (DWH Version)")

# ================================
# SIDEBAR FILTER
# ================================
st.sidebar.header("Filters")

selected_city = st.sidebar.text_input("Filter City (optional)", "")

month_query = """
SELECT d.monthname AS month, SUM(f.totalprice) AS total_sales
FROM dwh.factsales f
JOIN dwh.dimdate d ON f.dateid = d.dateid
GROUP BY d.monthname, d.month
ORDER BY d.month;
"""

category_query = """
SELECT p.categoryname, SUM(f.totalprice) AS total_sales
FROM dwh.factsales f
JOIN dwh.dimproduct p ON f.productid = p.productid
GROUP BY p.categoryname
ORDER BY total_sales DESC;
"""

location_query = f"""
SELECT l.cityname, SUM(f.totalprice) AS total_sales
FROM dwh.factsales f
JOIN dwh.dimlocation l ON f.locationid = l.locationid
WHERE l.cityname ILIKE '%{selected_city}%'
GROUP BY l.cityname
ORDER BY total_sales DESC;
"""

weather_query = """
SELECT w.temperature_c, SUM(f.totalprice) AS total_sales
FROM dwh.factsales f
JOIN dwh.dimweather w ON f.weatherid = w.weatherid
GROUP BY w.temperature_c
ORDER BY w.temperature_c;
"""

# ================================
# LOAD DATA
# ================================
df_month = read_query(month_query)
df_category = read_query(category_query)
df_location = read_query(location_query)
df_weather = read_query(weather_query)

# ================================
# VISUAL SECTION
# ================================
col1, col2 = st.columns(2)

with col1:
    st.subheader("Total Sales per Month")
    st.plotly_chart(line_chart(df_month, "month", "total_sales", "Sales per Month"), use_container_width=True)

with col2:
    st.subheader("Sales by Product Category")
    st.plotly_chart(bar_chart(df_category, "categoryname", "total_sales", "Sales by Category"), use_container_width=True)

st.subheader(f"Sales by City {'('+selected_city+')' if selected_city else ''}")
st.plotly_chart(bar_chart(df_location, "cityname", "total_sales", "Sales by City"), use_container_width=True)

st.subheader("Weather Impact on Sales")
st.plotly_chart(line_chart(df_weather, "temperature_c", "total_sales", "Temperature vs Sales"), use_container_width=True)