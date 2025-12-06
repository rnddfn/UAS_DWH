# Panduan Perintah (Cheatsheet) - UAS Warehouse Project

Berikut adalah daftar perintah yang dapat Anda gunakan untuk menjalankan, mengelola, dan memantau project ini.

## 1. Menjalankan Infrastruktur (Docker)

Sebelum menjalankan apa pun, pastikan seluruh infrastruktur (Database, Airflow, Dashboard) menyala.

```bash
# Menjalankan semua service di background
docker-compose up -d --build

# Melihat status container yang berjalan
docker-compose ps

# Mematikan semua service
docker-compose down
```

---

## 2. Menjalankan ETL Pipeline

Ada dua cara untuk menjalankan proses ETL (`etl.py`):

### Cara A: Manual (via Docker One-off Container)

Gunakan perintah ini jika Anda ingin menjalankan ETL secara langsung dari terminal tanpa menunggu jadwal Airflow.

```bash
# PowerShell (Windows)
docker run --rm --network airflow-net --env-file .env -v ${PWD}:/usr/src/app -w /usr/src/app python:3.10-slim bash -c "pip install pandas sqlalchemy psycopg2-binary requests && python etl.py"
```

### Cara B: Otomatis (via Airflow)

1.  Buka browser ke **[http://localhost:8080](http://localhost:8080)**.
2.  Login dengan user: `admin` dan password: `admin`.
3.  Cari DAG bernama `etl_docker_demo`.
4.  Klik tombol **Play** (▶️) di sebelah kanan untuk men-trigger DAG secara manual.
5.  Atau biarkan berjalan sesuai jadwal (`@weekly`).

---

## 3. Mengakses Dashboard (Streamlit)

Setelah container berjalan, dashboard visualisasi dapat diakses langsung.

- **URL**: **[http://localhost:8501](http://localhost:8501)**
- Jika dashboard error atau kosong, pastikan ETL sudah dijalankan minimal satu kali (Cara A atau B di atas).

---

## 4. Maintenance & Debugging

### Cek Logs

```bash
# Cek logs ETL (jika dijalankan manual)
Get-Content logs/etl_execution.log -Wait

# Cek logs container spesifik (misal: postgres atau streamlit)
docker logs -f postgres_db
docker logs -f uas_warehouse-streamlit-1
```

### Akses Database Langsung

Jika ingin mengecek data di dalam database menggunakan SQL via terminal:

```bash
# Masuk ke container Postgres
docker exec -it postgres_db psql -U admin -d db_penjualan

# Contoh Query di dalam psql:
# \dt dwh.*          (List tabel di schema dwh)
# SELECT count(*) FROM dwh.factsales;
# \q                 (Keluar)
```

### Reset Total (Hapus Data)

**PERINGATAN**: Perintah ini akan menghapus seluruh data di database (termasuk volume). Gunakan hanya jika ingin mengulang dari nol.

```bash
docker-compose down -v
docker-compose up -d
```