#!/bin/bash
set -e

# Kalau perintah = "streamlit", jalankan dashboard
if [ "$1" = "streamlit" ]; then
    exec streamlit run visualization/app.py --server.address=0.0.0.0
fi

# Kalau perintah = "etl", jalankan ETL
if [ "$1" = "etl" ]; then
    exec python etl.py
fi

# Default
exec "$@"