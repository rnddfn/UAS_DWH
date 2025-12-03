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

# Jika di Windows (Git Bash/Cygwin) bisa juga gunakan dos2unix bila tersedia:
dos2unix entrypoint.sh || true

# 1.2 - buat executable
chmod +x entrypoint.sh

# Default
exec "$@"