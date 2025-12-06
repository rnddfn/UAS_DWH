# BAB 2: DESAIN DAN RANCANGAN DATA WAREHOUSE

## 2.1 Analisis Kebutuhan

Sistem Data Warehouse ini dirancang untuk memenuhi kebutuhan analisis bisnis penjualan, integrasi data multi-sumber (CSV, API cuaca, API hari libur), serta penyajian data yang konsisten dan mudah diakses untuk pelaporan dan dashboard.

### Kebutuhan Utama:

- Integrasi data transaksi, produk, pelanggan, karyawan, lokasi, cuaca, dan hari libur.
- Penyimpanan historis dan konsolidasi data untuk analisis tren dan performa bisnis.
- Penyajian data yang siap pakai untuk visualisasi dan pelaporan.

## 2.2 Desain Konseptual

### a. Diagram Arsitektur Sistem

Sistem terdiri dari beberapa komponen utama:

- **Sumber Data**: File CSV (penjualan, produk, pelanggan, dsb) dan API eksternal (cuaca, hari libur).
- **Data Lake (Landing Area)**: Menampung data mentah hasil ingest/fetch.
- **Staging Area**: Area kerja untuk transformasi dan pembersihan data.
- **Data Warehouse (Presentation Area)**: Menyimpan data akhir dalam bentuk Star Schema.
- **Dashboard**: Visualisasi data menggunakan Streamlit.

(Diagram arsitektur dapat dilampirkan di bagian ini)

### b. Entitas dan Relasi Utama

- **Fakta**: Transaksi penjualan (FactSales)
- **Dimensi**: Produk, Pelanggan, Karyawan, Lokasi, Waktu, Cuaca
- **Relasi**: Setiap transaksi penjualan terhubung ke seluruh dimensi melalui foreign key.

## 2.3 Desain Logis

### a. Star Schema

- **Fact Table**: FactSales (menyimpan metrik penjualan dan foreign key ke dimensi)
- **Dimension Tables**: DimProduct, DimCustomer, DimEmployee, DimLocation, DimDate, DimWeather

### b. Struktur Tabel (Logis)

- Penjelasan atribut utama, kunci primer, kunci asing, dan tipe data (lihat Data Dictionary untuk detail).

## 2.4 Desain Fisik (Opsional)

- Penentuan skema database: `datalake`, `staging`, `dwh`.
- Penggunaan Surrogate Key (integer auto-increment) pada seluruh dimensi.
- Pengaturan partisi atau indeks jika diperlukan (bisa dijelaskan singkat).

## 2.5 Diagram Alur Data (DFD)

- Gambarkan alur data dari sumber (CSV/API) ke Data Lake, Staging, DWH, hingga dashboard.
- Penjelasan singkat setiap proses utama (Extract, Transform, Load, Visualisasi).

## 2.6 Ringkasan Desain

Desain ini memastikan data dari berbagai sumber dapat diintegrasikan, dibersihkan, dan disajikan dalam bentuk yang optimal untuk analisis bisnis, dengan struktur yang mudah dikembangkan dan dipelihara.

## 2.7 Use Case Data Warehouse

Data Warehouse yang dirancang pada project ini dapat digunakan untuk berbagai kebutuhan analisis dan pelaporan bisnis, di antaranya:

### 1. Analisis Penjualan Multi Dimensi

- Melihat total penjualan berdasarkan waktu (harian, bulanan, tahunan).
- Menganalisis tren penjualan musiman dan pengaruh hari libur nasional.
- Membandingkan performa penjualan antar produk, kategori, atau lokasi.

### 2. Analisis Perilaku Pelanggan

- Mengidentifikasi pelanggan paling aktif dan loyal.
- Segmentasi pelanggan berdasarkan lokasi, frekuensi pembelian, atau nilai transaksi.

### 3. Analisis Kinerja Karyawan

- Mengevaluasi performa sales berdasarkan jumlah transaksi dan total penjualan yang dihasilkan.

### 4. Analisis Pengaruh Faktor Eksternal

- Mengkaji dampak kondisi cuaca (suhu, hujan) terhadap volume penjualan.
- Menemukan pola penjualan yang dipengaruhi oleh faktor eksternal (cuaca ekstrem, hari libur).

### 5. Dashboard Eksekutif

- Menyediakan visualisasi interaktif (KPI, grafik tren, peta penjualan) untuk pengambilan keputusan manajemen secara cepat dan berbasis data.

### 6. Prediksi Penjualan (Machine Learning)

- Menggunakan data historis untuk memprediksi penjualan masa depan dengan model regresi sederhana.

Use case di atas dapat dikembangkan lebih lanjut sesuai kebutuhan bisnis dan ketersediaan data tambahan di masa depan.
