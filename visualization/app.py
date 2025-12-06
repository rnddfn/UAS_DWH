import streamlit as st
import pandas as pd
import plotly.express as px
from utils.db import read_query
import datetime
import os
import numpy as np
from sklearn.linear_model import LinearRegression

st.set_page_config(page_title="Warehouse Executive Dashboard", layout="wide", page_icon="📈")

# --- CSS Styling ---
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 2em;
        font-weight: bold;
        color: #0e1117;
    }
    .metric-label {
        font-size: 1em;
        color: #555;
    }
</style>
""", unsafe_allow_html=True)

# --- NAVIGATION ---
page = st.sidebar.radio("Go to", ["Dashboard", "Prediction"])

if page == "Prediction":
    st.title("🔮 Sales Prediction (Machine Learning)")
    st.markdown("Simple Linear Regression to forecast future sales based on historical data.")

    # 1. Get Historical Data
    hist_query = """
    SELECT d.fulldate, SUM(f.totalprice) as revenue
    FROM dwh.factsales f
    JOIN dwh.dimdate d ON f.dateid = d.dateid
    GROUP BY d.fulldate
    ORDER BY d.fulldate
    """
    try:
        df_hist = read_query(hist_query)
        if not df_hist.empty:
            df_hist['revenue'] = df_hist['revenue'].astype(float)
            df_hist['fulldate'] = pd.to_datetime(df_hist['fulldate'])
            
            # 2. Prepare Data for ML
            df_hist['date_ordinal'] = df_hist['fulldate'].map(datetime.datetime.toordinal)
            
            X = df_hist[['date_ordinal']]
            y = df_hist['revenue']
            
            # 3. Train Model
            model = LinearRegression()
            model.fit(X, y)
            
            # 4. Predict Future (Next 30 Days)
            last_date = df_hist['fulldate'].max()
            future_dates = [last_date + datetime.timedelta(days=x) for x in range(1, 31)]
            future_ordinals = [[d.toordinal()] for d in future_dates]
            
            future_pred = model.predict(future_ordinals)
            
            df_future = pd.DataFrame({
                'fulldate': future_dates,
                'revenue': future_pred,
                'type': 'Forecast'
            })
            
            df_hist['type'] = 'Historical'
            
            # Combine for plotting
            df_combined = pd.concat([df_hist[['fulldate', 'revenue', 'type']], df_future])
            
            # 5. Visualize
            st.subheader("Sales Forecast (Next 30 Days)")
            fig_pred = px.line(df_combined, x='fulldate', y='revenue', color='type', 
                               color_discrete_map={'Historical': 'blue', 'Forecast': 'red'},
                               template='plotly_white')
            st.plotly_chart(fig_pred, use_container_width=True)
            
            # Show metrics
            st.write(f"**Model Coefficient (Slope):** ${model.coef_[0]:.2f} / day")
            st.write(f"**Projected Revenue (Next 30 Days):** ${future_pred.sum():,.2f}")
            
        else:
            st.warning("Not enough data to train model.")
    except Exception as e:
        st.error(f"Prediction Error: {e}")
        
    st.stop() # Stop execution here so Dashboard code doesn't run

st.title("🏭 Enterprise Data Warehouse Dashboard")
st.markdown("Monitoring Sales, Inventory, and Performance Metrics")

# --- SIDEBAR FILTERS ---
st.sidebar.header("🔍 Filter Data")

# 1. Date Filter
# Fallback dates if DB is empty or connection fails
default_min = datetime.date(2018, 1, 1)
default_max = datetime.date(2018, 12, 31)

try:
    min_date_query = "SELECT MIN(fulldate) FROM dwh.dimdate"
    max_date_query = "SELECT MAX(fulldate) FROM dwh.dimdate"
    min_date_df = read_query(min_date_query)
    max_date_df = read_query(max_date_query)
    
    if not min_date_df.empty and min_date_df.iloc[0,0]:
        min_date = pd.to_datetime(min_date_df.iloc[0,0]).date()
    else:
        min_date = default_min
        
    if not max_date_df.empty and max_date_df.iloc[0,0]:
        max_date = pd.to_datetime(max_date_df.iloc[0,0]).date()
    else:
        max_date = default_max
except Exception as e:
    st.sidebar.error(f"DB Connection Error: {e}")
    min_date = default_min
    max_date = default_max

start_date = st.sidebar.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

# 2. Category Filter
try:
    cat_query = "SELECT DISTINCT categoryname FROM dwh.dimproduct ORDER BY categoryname"
    categories = read_query(cat_query)['categoryname'].tolist()
except:
    categories = []

if categories:
    selected_categories = st.sidebar.multiselect("Select Categories", categories, default=categories)
else:
    st.sidebar.warning("No categories found.")
    selected_categories = []

if not selected_categories and categories:
    st.warning("Please select at least one category.")
    st.stop()

# --- DATA LOADING HELPER ---
def get_filtered_data(start, end, cats):
    if not cats:
        return f"WHERE d.fulldate BETWEEN '{start}' AND '{end}'"
        
    cats_tuple = tuple(cats)
    if len(cats) == 1:
        cats_tuple = f"('{cats[0]}')"
    
    # Base Condition
    where_clause = f"""
    WHERE d.fulldate BETWEEN '{start}' AND '{end}'
    AND p.categoryname IN {cats_tuple}
    """
    
    return where_clause

where_clause = get_filtered_data(start_date, end_date, selected_categories)

# --- KPI SECTION ---
kpi_query = f"""
SELECT 
    SUM(f.totalprice) as total_revenue,
    SUM(f.quantity) as total_units,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT f.customerid) as total_customers
FROM dwh.factsales f
JOIN dwh.dimdate d ON f.dateid = d.dateid
JOIN dwh.dimproduct p ON f.productid = p.productid
{where_clause}
"""

try:
    kpi_data = read_query(kpi_query)
except:
    kpi_data = pd.DataFrame()

col1, col2, col3, col4 = st.columns(4)

def display_metric(col, label, value, prefix="", suffix=""):
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{prefix}{value:,.0f}{suffix}</div>
        </div>
        """, unsafe_allow_html=True)

if not kpi_data.empty and kpi_data.iloc[0,0] is not None:
    display_metric(col1, "Total Revenue", kpi_data['total_revenue'].iloc[0] or 0, "$")
    display_metric(col2, "Total Units Sold", kpi_data['total_units'].iloc[0] or 0)
    display_metric(col3, "Transactions", kpi_data['total_transactions'].iloc[0] or 0)
    display_metric(col4, "Total Customers", kpi_data['total_customers'].iloc[0] or 0)
else:
    st.info("No data available for the selected filters.")

st.markdown("---")

# --- CHARTS ROW 1 ---
c1, c2 = st.columns(2)

# Chart 1: Sales Trend
with c1:
    st.subheader("📈 Sales Trend Over Time")
    trend_query = f"""
    SELECT d.fulldate, SUM(f.totalprice) as revenue
    FROM dwh.factsales f
    JOIN dwh.dimdate d ON f.dateid = d.dateid
    JOIN dwh.dimproduct p ON f.productid = p.productid
    {where_clause}
    GROUP BY d.fulldate
    ORDER BY d.fulldate
    """
    try:
        df_trend = read_query(trend_query)
        if not df_trend.empty:
            # Ensure revenue is float for Plotly
            df_trend['revenue'] = df_trend['revenue'].astype(float)
            fig_trend = px.line(df_trend, x='fulldate', y='revenue', template='plotly_white')
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.write("No data.")
    except Exception as e:
        st.error(f"Error: {e}")

# Chart 2: Sales by Category
with c2:
    st.subheader("📦 Sales by Category")
    cat_sales_query = f"""
    SELECT p.categoryname, SUM(f.totalprice) as revenue
    FROM dwh.factsales f
    JOIN dwh.dimdate d ON f.dateid = d.dateid
    JOIN dwh.dimproduct p ON f.productid = p.productid
    {where_clause}
    GROUP BY p.categoryname
    ORDER BY revenue DESC
    """
    try:
        df_cat = read_query(cat_sales_query)
        if not df_cat.empty:
            # Ensure revenue is float
            df_cat['revenue'] = df_cat['revenue'].astype(float)
            fig_cat = px.pie(df_cat, names='categoryname', values='revenue', hole=0.4, template='plotly_white')
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.write("No data.")
    except:
        st.write("No data.")

# --- CHARTS ROW 2 ---
c3, c4 = st.columns(2)

# Chart 3: Top 10 Products
with c3:
    st.subheader("🏆 Top 10 Products")
    prod_query = f"""
    SELECT p.productname, SUM(f.totalprice) as revenue
    FROM dwh.factsales f
    JOIN dwh.dimdate d ON f.dateid = d.dateid
    JOIN dwh.dimproduct p ON f.productid = p.productid
    {where_clause}
    GROUP BY p.productname
    ORDER BY revenue DESC
    LIMIT 10
    """
    try:
        df_prod = read_query(prod_query)
        if not df_prod.empty:
            # Ensure revenue is float
            df_prod['revenue'] = df_prod['revenue'].astype(float)
            fig_prod = px.bar(df_prod, x='revenue', y='productname', orientation='h', template='plotly_white')
            fig_prod.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_prod, use_container_width=True)
        else:
            st.write("No data.")
    except:
        st.write("No data.")

# Chart 4: Top 10 Cities
with c4:
    st.subheader("🏙️ Top 10 Cities")
    city_query = f"""
    SELECT l.cityname, SUM(f.totalprice) as revenue
    FROM dwh.factsales f
    JOIN dwh.dimdate d ON f.dateid = d.dateid
    JOIN dwh.dimproduct p ON f.productid = p.productid
    JOIN dwh.dimlocation l ON f.locationid = l.locationid
    {where_clause}
    GROUP BY l.cityname
    ORDER BY revenue DESC
    LIMIT 10
    """
    try:
        df_city = read_query(city_query)
        if not df_city.empty:
            # Ensure revenue is float
            df_city['revenue'] = df_city['revenue'].astype(float)
            fig_city = px.bar(df_city, x='cityname', y='revenue', template='plotly_white', color='revenue')
            st.plotly_chart(fig_city, use_container_width=True)
        else:
            st.write("No data.")
    except:
        st.write("No data.")

# --- CHARTS ROW 3 ---
c5, c6 = st.columns(2)

# Chart 5: Employee Performance
with c5:
    st.subheader("👔 Top Employees")
    emp_query = f"""
    SELECT e.employeename, SUM(f.totalprice) as revenue
    FROM dwh.factsales f
    JOIN dwh.dimdate d ON f.dateid = d.dateid
    JOIN dwh.dimproduct p ON f.productid = p.productid
    JOIN dwh.dimemployee e ON f.employeeid = e.employeeid
    {where_clause}
    GROUP BY e.employeename
    ORDER BY revenue DESC
    LIMIT 10
    """
    try:
        df_emp = read_query(emp_query)
        if not df_emp.empty:
            # Ensure revenue is float
            df_emp['revenue'] = df_emp['revenue'].astype(float)
            fig_emp = px.bar(df_emp, x='employeename', y='revenue', template='plotly_white')
            st.plotly_chart(fig_emp, use_container_width=True)
        else:
            st.write("No data.")
    except:
        st.write("No data.")

# Chart 6: Holiday Impact
with c6:
    st.subheader("🎉 Holiday vs Non-Holiday Sales")
    hol_query = f"""
    SELECT 
        CASE WHEN d.isholiday THEN 'Holiday' ELSE 'Regular Day' END as day_type,
        AVG(f.totalprice) as avg_daily_revenue
    FROM dwh.factsales f
    JOIN dwh.dimdate d ON f.dateid = d.dateid
    JOIN dwh.dimproduct p ON f.productid = p.productid
    {where_clause}
    GROUP BY d.isholiday
    """
    try:
        df_hol = read_query(hol_query)
        if not df_hol.empty:
            # Ensure avg_daily_revenue is float
            df_hol['avg_daily_revenue'] = df_hol['avg_daily_revenue'].astype(float)
            fig_hol = px.bar(df_hol, x='day_type', y='avg_daily_revenue', template='plotly_white', title="Average Daily Revenue")
            st.plotly_chart(fig_hol, use_container_width=True)
        else:
            st.write("No data.")
    except:
        st.write("No data.")
