# Data Dictionary - UAS Warehouse Project

Dokumen ini berisi definisi detail mengenai struktur data, tabel, dan kolom yang digunakan dalam Data Warehouse (DWH) proyek ini.

## 1. Metodologi & Arsitektur Data

Proyek ini mengadopsi metodologi **Kimball (Dimensional Modeling)** yang berfokus pada proses bisnis (_Sales_) dan performa query analitik. Struktur data disusun dalam bentuk **Star Schema**, yang sangat tepat untuk kebutuhan Business Intelligence dan Dashboarding karena meminimalkan operasi _join_ yang kompleks saat pembacaan data.

Arsitektur teknis menggunakan pola **ETL (Extract, Transform, Load)** yang dimulai setelah data terkumpul di Data Lake. Alur data dibagi menjadi dua tahap utama:

1.  **Ingestion & Fetching**: Proses pengambilan data mentah dari sumber (CSV & API) untuk ditampung ke dalam **Data Lake**.
2.  **Core ETL**: Proses utama di mana data diekstrak dari Data Lake, ditransformasi di Staging, dan dimuat (_Load_) ke DWH.

Tiga layer data yang digunakan adalah:

| Layer                 | Schema     | Konsep Kimball            | Deskripsi                                                                                                                   |
| --------------------- | ---------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **Landing Area**      | `datalake` | _Source / Landing Zone_   | Tempat penyimpanan data mentah (as-is) dari sumber (CSV & API) sebelum diproses.                                            |
| **Staging Area**      | `staging`  | _Data Staging Area (DSA)_ | Area kerja (_Work Area_) untuk transformasi. Data dibersihkan, digabungkan, dan disiapkan. Bersifat sementara (_volatile_). |
| **Presentation Area** | `dwh`      | _Dimensional Model_       | Data Warehouse final dengan skema **Star Schema** (Fact & Dimensions) yang siap diakses oleh _Business Intelligence_.       |

---

## 2. Data Warehouse (Schema: `dwh`)

Sesuai prinsip Kimball, skema ini terdiri dari satu **Fact Table** pusat yang dikelilingi oleh **Dimension Tables** yang terdenormalisasi.

### A. Fact Table

#### `FactSales`

Tabel fakta utama yang menyimpan transaksi penjualan harian.

| Kolom        | Tipe Data | Deskripsi                                                                              | Contoh Data |
| ------------ | --------- | -------------------------------------------------------------------------------------- | ----------- |
| `SalesID`    | INT (PK)  | Primary Key unik untuk setiap transaksi penjualan.                                     | `1001`      |
| `DateID`     | INT (FK)  | Foreign Key ke `DimDate`. Menunjukkan kapan transaksi terjadi.                         | `20180101`  |
| `WeatherID`  | INT (FK)  | Foreign Key ke `DimWeather`. Kondisi cuaca saat transaksi terjadi di lokasi pelanggan. | `542`       |
| `ProductID`  | INT (FK)  | Foreign Key ke `DimProduct`. Produk yang terjual.                                      | `12`        |
| `CustomerID` | INT (FK)  | Foreign Key ke `DimCustomer`. Pelanggan yang membeli.                                  | `88`        |
| `EmployeeID` | INT (FK)  | Foreign Key ke `DimEmployee`. Karyawan yang melayani penjualan.                        | `5`         |
| `LocationID` | INT (FK)  | Foreign Key ke `DimLocation`. Lokasi kota pelanggan.                                   | `3`         |
| `Quantity`   | INT       | Jumlah barang yang dibeli dalam satu transaksi.                                        | `5`         |
| `TotalPrice` | DECIMAL   | Total pendapatan dari transaksi (Quantity _ Price _ (1 - Discount)).                   | `150.00`    |
| `Discount`   | DECIMAL   | Diskon yang diberikan (dalam desimal, misal 0.10 untuk 10%).                           | `0.10`      |

---

### B. Dimension Tables

#### `DimDate`

Dimensi waktu untuk analisis temporal (harian, bulanan, tahunan).

| Kolom         | Tipe Data | Deskripsi                                                   |
| ------------- | --------- | ----------------------------------------------------------- |
| `DateID`      | INT (PK)  | Format YYYYMMDD (misal: 20180101).                          |
| `FullDate`    | DATE      | Tanggal lengkap.                                            |
| `Day`         | INT       | Tanggal (1-31).                                             |
| `Month`       | INT       | Bulan (1-12).                                               |
| `MonthName`   | VARCHAR   | Nama bulan (January, February, dst).                        |
| `Quarter`     | INT       | Kuartal tahun (1, 2, 3, 4).                                 |
| `Year`        | INT       | Tahun (misal: 2018).                                        |
| `DayOfWeek`   | VARCHAR   | Nama hari (Monday, Tuesday, dst).                           |
| `IsHoliday`   | BOOLEAN   | Indikator apakah tanggal tersebut hari libur nasional (US). |
| `HolidayName` | VARCHAR   | Nama hari libur (jika ada).                                 |

#### `DimProduct`

Informasi detail mengenai produk yang dijual.

| Kolom            | Tipe Data | Deskripsi                                          |
| ---------------- | --------- | -------------------------------------------------- |
| `ProductID`      | INT (PK)  | Surrogate Key untuk produk di DWH.                 |
| `ProductID_OLTP` | INT       | ID asli produk dari sistem sumber (CSV).           |
| `ProductName`    | VARCHAR   | Nama produk.                                       |
| `Price`          | DECIMAL   | Harga satuan produk.                               |
| `CategoryName`   | VARCHAR   | Kategori produk (misal: Beverages, Condiments).    |
| `Class`          | VARCHAR   | Klasifikasi produk (misal: Agriculture, Chemical). |
| `IsAllergic`     | VARCHAR   | Informasi alergen (Y/N atau detail).               |

#### `DimCustomer`

Informasi mengenai pelanggan.

| Kolom                 | Tipe Data | Deskripsi                             |
| --------------------- | --------- | ------------------------------------- |
| `CustomerID`          | INT (PK)  | Surrogate Key untuk pelanggan di DWH. |
| `CustomerID_OLTP`     | INT       | ID asli pelanggan dari sistem sumber. |
| `CustomerName`        | VARCHAR   | Nama lengkap pelanggan.               |
| `Address`             | VARCHAR   | Alamat pelanggan.                     |
| `CustomerCityName`    | VARCHAR   | Kota domisili pelanggan.              |
| `CustomerCountryName` | VARCHAR   | Negara domisili pelanggan.            |

#### `DimEmployee`

Informasi mengenai karyawan sales.

| Kolom             | Tipe Data | Deskripsi                            |
| ----------------- | --------- | ------------------------------------ |
| `EmployeeID`      | INT (PK)  | Surrogate Key untuk karyawan di DWH. |
| `EmployeeID_OLTP` | INT       | ID asli karyawan dari sistem sumber. |
| `EmployeeName`    | VARCHAR   | Nama lengkap karyawan.               |
| `Gender`          | VARCHAR   | Jenis kelamin karyawan.              |
| `HireDate`        | DATE      | Tanggal mulai bekerja.               |

#### `DimLocation`

Dimensi geografis berdasarkan kota pelanggan.

| Kolom         | Tipe Data | Deskripsi                       |
| ------------- | --------- | ------------------------------- |
| `LocationID`  | INT (PK)  | Surrogate Key untuk lokasi.     |
| `CityID_OLTP` | INT       | ID kota dari sumber (jika ada). |
| `CityName`    | VARCHAR   | Nama kota.                      |
| `CountryName` | VARCHAR   | Nama negara.                    |

#### `DimWeather`

Dimensi cuaca historis yang dikaitkan dengan tanggal dan lokasi penjualan.

| Kolom           | Tipe Data | Deskripsi                                     |
| --------------- | --------- | --------------------------------------------- |
| `WeatherID`     | INT (PK)  | Surrogate Key untuk data cuaca.               |
| `Condition`     | VARCHAR   | Kondisi cuaca (misal: Sunny, Rainy, Cloudy).  |
| `Temperature_C` | INT       | Suhu rata-rata dalam Celcius.                 |
| `FeelsLike_C`   | INT       | Suhu yang dirasakan (Real Feel).              |
| `Wind_kph`      | DECIMAL   | Kecepatan angin (km/h).                       |
| `Precip_mm`     | DECIMAL   | Curah hujan (mm).                             |
| `IsDay`         | INT       | Indikator siang/malam (1 = Siang, 0 = Malam). |
| `DateID`        | INT       | Referensi ke tanggal cuaca.                   |
| `LocationID`    | INT       | Referensi ke lokasi cuaca.                    |

---

## Penjelasan Tabel Fakta dan Dimensi

### FactSales

Tabel fakta utama yang merekam setiap transaksi penjualan yang terjadi di sistem. Setiap baris pada tabel ini mewakili satu transaksi, berisi informasi kuantitatif seperti jumlah barang yang terjual (Quantity), total pendapatan (TotalPrice), dan diskon yang diberikan. Tabel ini juga menghubungkan seluruh dimensi melalui foreign key, sehingga memungkinkan analisis penjualan berdasarkan waktu, produk, pelanggan, lokasi, karyawan, dan kondisi cuaca. FactSales menjadi pusat analisis performa bisnis dan sumber utama untuk perhitungan KPI.

### DimProduct

Tabel dimensi produk berisi data detail tentang setiap produk yang dijual, seperti nama produk, kategori, harga, kelas, dan informasi alergen. Dimensi ini memungkinkan analisis penjualan berdasarkan jenis produk, kategori, dan karakteristik lain yang relevan untuk strategi pemasaran dan pengelolaan inventori.

### DimCustomer

Tabel dimensi pelanggan menyimpan informasi identitas dan lokasi pelanggan, seperti nama, alamat, kota, dan negara. Dengan dimensi ini, perusahaan dapat melakukan segmentasi pelanggan, analisis perilaku pembelian, serta mengidentifikasi pelanggan paling aktif atau loyal.

### DimEmployee

Tabel dimensi karyawan berisi data tentang staf penjualan, termasuk nama, gender, dan tanggal mulai bekerja. Dimensi ini digunakan untuk mengevaluasi kinerja karyawan, membandingkan performa antar sales, dan mendukung program insentif atau pelatihan.

### DimLocation

Tabel dimensi lokasi menyimpan data geografis berupa nama kota dan negara. Dimensi ini penting untuk analisis penjualan berdasarkan wilayah, identifikasi pasar potensial, dan pengambilan keputusan ekspansi bisnis.

### DimDate

Tabel dimensi waktu menyediakan atribut tanggal, hari, bulan, tahun, kuartal, hari dalam minggu, serta penanda hari libur dan nama libur. Dimensi ini sangat krusial untuk analisis tren penjualan, identifikasi pola musiman, dan perencanaan promosi.

### DimWeather

Tabel dimensi cuaca berisi data historis kondisi cuaca (suhu, curah hujan, kondisi langit, dll) yang dihubungkan ke tanggal dan lokasi penjualan. Dimensi ini memungkinkan analisis dampak faktor eksternal terhadap penjualan, seperti pengaruh cuaca ekstrem atau musim tertentu terhadap volume transaksi.

---

## 3. Sumber Data (Data Lineage)

| Tabel DWH     | Sumber Utama                      | Keterangan Transformasi                                                  |
| ------------- | --------------------------------- | ------------------------------------------------------------------------ |
| `DimDate`     | Python Logic + `holidays` API     | Dibuat secara prosedural di Python, digabung dengan data libur dari API. |
| `DimProduct`  | `products.csv` + `categories.csv` | Join antara produk dan kategori.                                         |
| `DimCustomer` | `customers.csv`                   | Data pelanggan langsung.                                                 |
| `DimEmployee` | `employees.csv`                   | Data karyawan langsung.                                                  |
| `DimLocation` | `cities.csv` + `countries.csv`    | Join kota dan negara.                                                    |
| `DimWeather`  | `weather_mentah.csv`              | Data cuaca historis, di-join dengan Date dan Location.                   |
| `FactSales`   | `sales.csv`                       | Transaksi penjualan, dihitung `TotalPrice` = `Qty * Price * (1-Disc)`.   |

---

## 4. Implementasi Data Governance

Proyek ini menerapkan standar Data Governance untuk menjamin keamanan, kualitas, dan keterlacakan data.

### A. Data Security (Keamanan)

- **Credential Management**: Password dan user database tidak disimpan dalam kode (`hardcoded`), melainkan menggunakan **Environment Variables** (`.env` file) yang tidak di-upload ke repository.
- **Access Control**: Akses database dibatasi hanya untuk container dalam jaringan Docker internal (`airflow-net`).

### B. Data Quality (Kualitas Data)

Sistem memiliki mekanisme **Automated Quality Gates (Circuit Breaker)** yang berjalan sebelum data dimuat ke DWH. Pipeline akan otomatis berhenti jika mendeteksi anomali berikut:

1.  **Negative Value Check**: Memastikan tidak ada nilai `Quantity` atau `TotalPrice` yang negatif.
2.  **Referential Integrity Check**: Memastikan setiap transaksi penjualan memiliki `ProductID` dan `CustomerID` yang valid (tidak NULL).
3.  **Schema Validation**: Memastikan struktur tabel staging sesuai dengan ekspektasi sebelum transformasi.

### C. Data Architecture & Lineage

- **Layered Architecture**: Pemisahan tegas antara data mentah (`datalake`), data proses (`staging`), dan data final (`dwh`) mencegah kontaminasi data.
- **Audit Logging**: Setiap langkah eksekusi ETL dicatat dalam file log (`logs/etl_execution.log`) untuk keperluan audit dan debugging.

---

## 5. Data Recovery Plan & Failure Handling

Strategi pemulihan data (Data Recovery) dalam proyek ini mengandalkan prinsip **Idempotency** dan **Re-processing**. Karena sumber data utama adalah file statis (CSV) dan API, pemulihan dilakukan dengan menjalankan ulang pipeline (Re-run).

### A. Skenario Kegagalan & Prosedur Pemulihan (SOP)

| Skenario Kegagalan                   | Dampak                                        | Prosedur Pemulihan (Recovery Steps)                                                                                                                                                                  |
| ------------------------------------ | --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Pipeline Error** (Code/Data Issue) | Proses berhenti, data DWH tidak terupdate.    | 1. Cek `logs/etl_execution.log` untuk error detail.<br>2. Perbaiki bug kode atau data sumber.<br>3. Jalankan ulang `etl.py`. Script akan otomatis membersihkan schema `staging` dan memproses ulang. |
| **Data Corruption di DWH**           | Data di dashboard salah atau tidak konsisten. | 1. Jalankan ulang `etl.py`.<br>2. Script melakukan `TRUNCATE` pada tabel DWH sebelum insert, sehingga data korup akan tertimpa data baru yang bersih.                                                |
| **Docker Container Crash**           | Database tidak bisa diakses.                  | 1. Restart container: `docker-compose restart db_postgres`.<br>2. Data aman karena tersimpan di Docker Volume (`postgres_data`).                                                                     |
| **Volume Terhapus (Catastrophic)**   | Seluruh data database hilang.                 | 1. Deploy ulang container.<br>2. Jalankan `etl.py`.<br>3. Pipeline akan membangun ulang seluruh database (`datalake` -> `staging` -> `dwh`) dari nol menggunakan file CSV sumber.                    |

### B. Callback & Alerting Mechanism

Sistem menangani kegagalan secara programatik menggunakan mekanisme berikut:

1.  **Exception Handling (Try-Catch)**:

    - Seluruh proses ETL dibungkus dalam blok `try-except`.
    - Jika terjadi error (misal: koneksi putus, DQ check gagal), script menangkap error tersebut.

2.  **Logging Callback**:

    - **On Failure**: Error dicatat ke `logs/etl_execution.log` dengan level `ERROR`.
    - **On Success**: Status keberhasilan dan jumlah baris data dicatat dengan level `INFO`.

3.  **Process Termination**:
    - Jika gagal, script keluar dengan `exit(1)`. Ini memberi sinyal kepada orkestrator (Docker/Airflow) bahwa _task_ gagal, sehingga tidak dianggap sukses secara palsu (False Positive).

---

## 6. Lingkungan Pengembangan & Implementasi Visualisasi

### A. Lingkungan Pengembangan (Development Environment)

Proyek ini dikembangkan dan dijalankan dalam lingkungan yang terisolasi menggunakan teknologi kontainerisasi untuk menjamin konsistensi antar _environment_.

| Komponen                | Teknologi / Versi            | Deskripsi                                                 |
| :---------------------- | :--------------------------- | :-------------------------------------------------------- |
| **Sistem Operasi Host** | Windows / Linux / MacOS      | Sistem operasi tempat Docker berjalan.                    |
| **Container Engine**    | Docker & Docker Compose      | Mengelola _lifecycle_ service (Database, App, Scheduler). |
| **Database Engine**     | PostgreSQL 18                | RDBMS untuk menyimpan Data Lake, Staging, dan DWH.        |
| **Bahasa Pemrograman**  | Python 3.10 (Slim)           | Digunakan untuk script ETL dan aplikasi Dashboard.        |
| **ETL Libraries**       | Pandas, SQLAlchemy, Psycopg2 | Pustaka utama untuk manipulasi data dan koneksi database. |
| **Orchestrator**        | Apache Airflow 2.8.0         | (Opsional) Penjadwalan dan monitoring workflow ETL.       |

### B. Implementasi Visualisasi (Dashboard)

Visualisasi data diimplementasikan menggunakan **Streamlit**, sebuah framework Python untuk membangun aplikasi data interaktif berbasis web.

#### 1. Teknologi Visualisasi

- **Framework**: Streamlit
- **Charting Library**: Plotly Express (untuk grafik interaktif) & Altair.
- **Data Source**: Langsung terhubung ke schema `dwh` di PostgreSQL.

#### 2. Fitur Dashboard

Dashboard dirancang untuk memberikan wawasan bisnis (_Business Insights_) melalui metrik dan grafik berikut:

- **KPI Utama (Scorecards)**:
  - Total Penjualan (_Total Sales_)
  - Total Transaksi (_Total Orders_)
  - Total Pelanggan (_Total Customers_)
- **Analisis Tren**: Grafik garis (_Line Chart_) penjualan harian/bulanan.
- **Analisis Produk**: Grafik batang (_Bar Chart_) untuk produk terlaris (_Top Selling Products_).
- **Analisis Geografis**: Peta sebaran penjualan berdasarkan kota/negara.
- **Analisis Korelasi Cuaca**: Visualisasi hubungan antara kondisi cuaca (Suhu/Hujan) dengan volume penjualan.
- **Fitur Prediksi (ML)**: Implementasi regresi linear sederhana untuk memprediksi penjualan masa depan.
