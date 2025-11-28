# Dockerfile

# 1. Mulai dari image Python yang resmi (ringan/slim)
FROM python:3.10-slim

# 2. Tentukan folder kerja di dalam container
WORKDIR /usr/src/app

# 3. Salin file requirements.txt
#    Ini dilakukan terpisah agar Docker bisa "cache" langkah ini
COPY requirements.txt .

# 4. Instal dependensi Python yang ada di requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 5. Salin semua sisa file proyek (termasuk etl.py dan folder 'data' CSV)
COPY . .

# 6. Perintah yang akan dijalankan saat container dimulai
CMD [ "python", "etl.py" ]

# Supaya container bisa menjalankan apa saja (ETL, Streamlit)
ENTRYPOINT ["bash", "entrypoint.sh"]