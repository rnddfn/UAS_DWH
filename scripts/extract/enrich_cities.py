# enrich_cities.py (REVISI - Strategi Mapping Kota)
import pandas as pd
import os

DATA_DIR = './data/raw/'

try:
    print("Membaca file 'cities.csv' (simulasi)...")
    # Kita tidak peduli 'Zipcode' lagi, jadi kita baca apa adanya
    df_my_cities = pd.read_csv(os.path.join(DATA_DIR, 'cities.csv'))
    
    print("Membaca file 'us_cities.csv' (data nyata)...")
    df_us_cities = pd.read_csv(
        os.path.join(DATA_DIR, 'uscities.csv'),
        dtype={'zips': str}
    )
    
    # --- INI ADALAH LANGKAH KUNCI ---
    print("Membuat peta 1-ke-1 (Satu Kota Nyata per Nama Kota)...")

    df_us_cities = df_us_cities.sort_values(by='population', ascending=False)
    
    # 2. Hapus duplikat berdasarkan 'city', simpan yang PERTAMA (terbesar)
    #    Ini adalah inti dari idemu "mengambil sembarang satu saja"
    df_real_map = df_us_cities.drop_duplicates(subset=['city'], keep='first')
    
    # 3. Pilih hanya kolom yang kita perlukan untuk digabung
    df_real_map = df_real_map[['city', 'state_name', 'zips', 'lat', 'lng']]
    # ------------------------------------

    print("Menggabungkan file simulasi dengan data kota nyata (berdasarkan Nama Kota)...")
    df_merged = pd.merge(
        df_my_cities,
        df_real_map,
        left_on='CityName',  # Nama kota di file simulasimu
        right_on='city',     # Nama kota di file us_cities
        how='left'           # Jaga semua kota simulasimu
    )
    
    # Bersihkan kolom duplikat
    df_merged = df_merged.drop(columns=['city']) # Hapus 'city' (karena sudah ada 'CityName')
    
    # 4. Simpan hasilnya
    output_file = os.path.join(DATA_DIR, 'cities_MODIFIED_with_coords.csv')
    df_merged.to_csv(output_file, index=False)
    
    # Cek apakah ada kota simulasi yang tidak ditemukan di file us_cities
    gagal_merge = df_merged[df_merged['lat'].isnull()]
    if not gagal_merge.empty:
        print("\nPERINGATAN: Kota simulasi berikut tidak ditemukan di us_cities.csv:")
        print(gagal_merge['CityName'])
    
    print(f"\nBERHASIL! 🚀 File baru telah dibuat di: {output_file}")
    print("File ini sekarang memiliki data Lat/Lng NYATA yang 'dipetakan'.")

except Exception as e:
    print(f"\nERROR: {e}")