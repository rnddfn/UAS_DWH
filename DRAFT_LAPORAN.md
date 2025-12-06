# DRAFT LAPORAN: PERANCANGAN DATA WAREHOUSE PENJUALAN

Berikut adalah draf untuk bagian **Rumusan Masalah**, **Tujuan**, dan **Penjelasan Perancangan** yang disesuaikan dengan kondisi teknis project Anda saat ini (Kimball, ETL, Data Governance, & Docker).

---

## 1. Rumusan Masalah

Berdasarkan latar belakang bisnis retail yang menangani volume transaksi harian yang besar, ditemukan beberapa permasalahan utama dalam pengelolaan dan analisis data:

1.  **Fragmentasi Data**: Data transaksi penjualan, data pelanggan, dan data produk tersebar dalam berbagai file mentah (CSV) yang terpisah, menyulitkan manajemen untuk mendapatkan gambaran bisnis secara menyeluruh (_holistic view_).
2.  **Keterbatasan Analisis Faktor Eksternal**: Sistem operasional saat ini hanya mencatat transaksi internal, namun belum mengintegrasikan data eksternal seperti kondisi cuaca dan hari libur nasional. Padahal, faktor-faktor ini berpotensi signifikan mempengaruhi tren penjualan.
3.  **Kualitas dan Konsistensi Data**: Sering ditemukan ketidakkonsistenan data (seperti duplikasi kota, format tanggal yang berbeda) dan anomali data (nilai negatif atau _null_) yang mengurangi akurasi pelaporan.
4.  **Proses Pelaporan Manual**: Pembuatan laporan analitik masih dilakukan secara manual, sehingga memakan waktu lama dan rentan terhadap _human error_.

## 2. Tujuan Penelitian/Project

Tujuan dari perancangan dan pembangunan Data Warehouse ini adalah:

1.  **Membangun Data Warehouse Terpusat**: Merancang penyimpanan data terpusat menggunakan metodologi **Kimball (Dimensional Modeling)** untuk menyatukan data dari berbagai sumber CSV dan API eksternal.
2.  **Implementasi Otomatisasi ETL**: Mengembangkan pipeline **ETL (Extract, Transform, Load)** yang otomatis dan tangguh (_robust_) untuk memproses data dari _Data Lake_ menuju _Data Warehouse_ dengan intervensi manual yang minimal.
3.  **Peningkatan Kualitas Data (Data Governance)**: Menerapkan mekanisme _Data Quality Gates_ otomatis untuk menjamin data yang masuk ke sistem analitik bersih, valid, dan dapat dipercaya.
4.  **Analisis Multidimensi & Visualisasi**: Menyediakan dashboard interaktif yang memungkinkan pemangku kepentingan menganalisis performa penjualan berdasarkan dimensi waktu, lokasi, produk, serta korelasinya dengan kondisi cuaca.

---

## 3. Penjelasan Perancangan Sistem

Perancangan sistem Data Warehouse ini mengacu pada standar industri modern dengan fokus pada performa, skalabilitas, dan tata kelola data (_governance_).

### A. Metodologi Perancangan

Project ini mengadopsi pendekatan **Kimball (Bottom-Up)**. Pendekatan ini dipilih karena berfokus pada pengiriman nilai bisnis secara cepat melalui pembentukan **Dimensional Model (Star Schema)**. Struktur ini mengoptimalkan performa _query_ untuk kebutuhan _Business Intelligence_ (BI) dibandingkan struktur 3NF yang biasa digunakan pada database operasional.

### B. Arsitektur Data (Technical Architecture)

Sistem menggunakan arsitektur **ETL Modern** yang terbagi menjadi tiga layer logis untuk menjamin integritas data:

1.  **Ingestion Layer (Data Lake / Landing Area)**:
    - Berfungsi sebagai tempat pendaratan data mentah (_Raw Data_) dari sumber CSV dan API (Weather & Holiday).
    - Data disimpan "as-is" (apa adanya) dalam skema `datalake` untuk keperluan audit dan _recovery_.
2.  **Transformation Layer (Staging Area)**:
    - Area kerja sementara (skema `staging`) di mana data mentah dibersihkan, dideduplikasi, dan distandarisasi.
    - Proses transformasi meliputi penggabungan data cuaca dengan tanggal transaksi serta perhitungan metrik bisnis (misal: `TotalPrice`).
3.  **Presentation Layer (Data Warehouse)**:
    - Penyimpanan final (skema `dwh`) yang menggunakan **Star Schema**.
    - Data di layer ini sudah bersih, tervalidasi, dan siap dikonsumsi oleh tools visualisasi (Streamlit).

### C. Perancangan Skema (Star Schema)

Model data terdiri dari satu tabel fakta pusat yang dikelilingi oleh tabel dimensi:

- **Fact Table (`FactSales`)**: Menyimpan metrik kuantitatif seperti `Quantity`, `TotalPrice`, dan `Discount`. Tabel ini memiliki _Foreign Keys_ yang terhubung ke semua dimensi.
- **Dimension Tables**:
  - `DimDate`: Dimensi waktu yang diperkaya dengan informasi hari libur (_Holiday API_).
  - `DimWeather`: Dimensi unik yang menyimpan kondisi cuaca historis (Suhu, Curah Hujan) untuk setiap tanggal dan lokasi.
  - `DimProduct`, `DimCustomer`, `DimLocation`, `DimEmployee`: Dimensi standar untuk analisis operasional.
- **Surrogate Keys**: Seluruh tabel dimensi menggunakan _Surrogate Key_ (Integer Auto-increment) untuk menjaga independensi dari perubahan _Primary Key_ pada sistem sumber operasional.

### D. Data Governance & Security

Perancangan ini tidak hanya berfokus pada fungsionalitas tetapi juga keamanan dan kualitas:

- **Automated Quality Gates**: Implementasi _Circuit Breaker_ pada pipeline ETL yang otomatis menghentikan proses jika terdeteksi anomali fatal (misal: nilai transaksi negatif).
- **Security**: Penggunaan _Environment Variables_ untuk manajemen kredensial database dan isolasi jaringan menggunakan Docker Network.

### E. Teknologi Pengembang

- **Bahasa Pemrograman**: Python (Pandas, SQLAlchemy) untuk logika ETL.
- **Database**: PostgreSQL 18 sebagai mesin penyimpanan Data Warehouse.
- **Orkestrasi**: Apache Airflow (opsional) dan Docker Container untuk manajemen _workflow_.
- **Visualisasi**: Streamlit untuk antarmuka dashboard pengguna.
