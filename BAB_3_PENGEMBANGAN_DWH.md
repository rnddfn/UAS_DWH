# BAB 3: PENGEMBANGAN DATA WAREHOUSE

## 3.1 Pendahuluan

Bab ini membahas proses implementasi dan pengembangan Data Warehouse (DWH) pada proyek ini, mulai dari pemilihan metodologi, tahapan ETL, hingga penerapan data governance dan visualisasi.

## 3.2 Metodologi Pengembangan

Pengembangan DWH menggunakan pendekatan **Kimball (Dimensional Modeling)** dengan model **Star Schema**. Metode ini dipilih karena sangat sesuai untuk kebutuhan analitik, integrasi multi-sumber, dan kemudahan ekspansi di masa depan.

### 3.2.1 Data Lake dan Data Ingestion

Pada tahap awal pengembangan Data Warehouse, seluruh data dari berbagai sumber dikumpulkan dan disimpan di Data Lake. Data Lake berfungsi sebagai area landing untuk data mentah, baik dari file CSV maupun API eksternal. Proses ini disebut sebagai data ingestion.

- **Sumber Data CSV**: Data penjualan, produk, pelanggan, karyawan, dan lokasi diambil dari file CSV yang disimpan di folder `data/raw/`. Setiap file CSV di-load ke dalam tabel mentah di schema `datalake` tanpa modifikasi, sehingga data tetap "as-is" dari sumber.
- **Sumber Data API**: Data eksternal seperti cuaca harian dan hari libur nasional diambil melalui proses fetch API. Hasil response API juga disimpan sebagai tabel mentah di Data Lake, misal `weather_mentah` dan `holidays_mentah`.
- **Tujuan Data Lake**: Menyediakan backup data mentah, audit trail, dan sumber utama untuk proses ETL selanjutnya. Dengan adanya Data Lake, pipeline ETL dapat diulang kapan saja tanpa kehilangan data sumber, serta memudahkan troubleshooting jika terjadi error.

Proses ingestion ini memastikan seluruh data sumber terintegrasi dan terdokumentasi dengan baik sebelum dilakukan transformasi dan pembersihan di tahap berikutnya.

## 3.3 Implementasi ETL (Extract, Transform, Load)

Implementasi ETL pada proyek ini dirancang agar robust, modular, dan mudah di-maintain. Proses ETL dibagi menjadi beberapa tahap utama sebagai berikut:

### 3.3.1 Extract

Pada tahap extract, data diambil dari berbagai sumber:

- **File CSV**: Data penjualan, produk, pelanggan, karyawan, lokasi, dan lain-lain diambil dari file CSV yang disimpan di folder `data/raw/`.
- **API Eksternal**: Data cuaca harian diambil dari API cuaca (misal: WeatherAPI), sedangkan data hari libur nasional diambil dari API khusus libur (misal: Calendarific). Data hasil fetch API ini juga disimpan dalam bentuk tabel mentah di Data Lake.
- **Tujuan**: Menjamin seluruh data sumber dapat di-audit dan diulang prosesnya jika terjadi kegagalan.

### 3.3.2 Load ke Data Lake

Setelah data diekstrak, seluruh data mentah dimuat ke schema `datalake` di PostgreSQL. Proses ini dilakukan tanpa pembersihan atau transformasi, sehingga data tetap "as-is" dari sumber. Setiap file atau hasil API akan menjadi satu tabel mentah, misal: `sales_mentah`, `products_mentah`, `weather_mentah`, dsb.

- **Manfaat**: Data Lake berfungsi sebagai backup, audit trail, dan sumber utama untuk reprocessing jika pipeline gagal.

### 3.3.3 Transform

Transformasi data dilakukan di dua tempat:

- **Python (Pre-SQL)**: Untuk operasi chunking file besar (misal: sales.csv), parsing tanggal, dan validasi awal.
- **SQL (Staging Area)**: Transformasi utama dilakukan di database, meliputi:
  - Pembersihan data (TRIM, DISTINCT, filter NULL/negatif)
  - Join antar tabel mentah (misal: join produk dengan kategori, join sales dengan customer dan produk)
  - Enrichment data (join data cuaca dan hari libur ke dimensi waktu)
  - Pembuatan tabel dimensi (`dimproduct`, `dimcustomer`, `dimdate`, dsb) dan tabel fakta (`factsales`) di schema `staging`.
- **Quality Gate**: Setelah transformasi, dilakukan pengecekan kualitas data secara otomatis (misal: tidak boleh ada quantity negatif, foreign key null, dsb). Jika gagal, proses ETL dihentikan.

### 3.3.4 Load ke Data Warehouse (DWH)

Data yang sudah bersih dan terstruktur di schema `staging` kemudian dimuat ke schema `dwh` dengan model Star Schema:

- Tabel dimensi dan fakta di DWH diisi ulang (TRUNCATE + INSERT) setiap pipeline dijalankan, sehingga data selalu konsisten dan bebas duplikasi.
- Proses load dilakukan secara batch dan atomic (menggunakan transaction block) untuk mencegah data setengah jadi jika terjadi error.

### 3.3.5 Monitoring & Logging

- Seluruh proses ETL dicatat ke file log (`logs/etl_execution.log`) dengan level detail (INFO, ERROR).
- Jika terjadi error, log dapat digunakan untuk troubleshooting dan audit.

### 3.3.6 Orkestrasi (Opsional)

- Pipeline ETL dapat dijalankan manual (via Docker) atau otomatis menggunakan Apache Airflow, sehingga mendukung penjadwalan dan monitoring workflow secara enterprise.

Dengan desain ini, pipeline ETL pada proyek ini mampu menangani data multi-sumber, menjamin kualitas dan keamanan data, serta mudah diulang jika terjadi kegagalan tanpa kehilangan data historis.

## 3.4 Implementasi Data Governance

### a. Data Quality Gate

Pipeline ETL dilengkapi pengecekan otomatis (circuit breaker) untuk mencegah data buruk (misal: nilai negatif, foreign key null) masuk ke DWH. Jika ditemukan anomali, proses ETL akan berhenti dan error dicatat di log.

### b. Audit Logging

Seluruh proses ETL dicatat ke file log (`logs/etl_execution.log`) untuk keperluan audit, debugging, dan monitoring.

### c. Security

Kredensial database dikelola menggunakan environment variable (.env) dan seluruh service berjalan di jaringan Docker internal untuk keamanan.

## 3.5 Lingkungan Pengembangan

Pengembangan dilakukan menggunakan Python 3.10, PostgreSQL 18, Docker, dan Apache Airflow. Semua dependensi dikelola dengan Docker Compose untuk memastikan konsistensi environment.

## 3.6 Implementasi Visualisasi

Dashboard dibangun menggunakan Streamlit dan terhubung langsung ke DWH. Fitur dashboard meliputi KPI, analisis tren, analisis produk, analisis geografis, korelasi cuaca, dan prediksi penjualan menggunakan model machine learning sederhana.

## 3.7 Penutup

Dengan pendekatan ini, Data Warehouse yang dibangun mampu menyediakan data yang terintegrasi, bersih, dan siap pakai untuk analisis bisnis, serta mudah dikembangkan dan dipelihara di masa depan.
