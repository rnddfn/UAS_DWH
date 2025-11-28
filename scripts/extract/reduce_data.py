# reduce_data.py
import pandas as pd
import os

# --- Konfigurasi ---
DATA_DIR = './data/raw/' # Sesuaikan jika beda
FILE_BESAR = 'sales.csv'
FILE_BACKUP = 'sales_BACKUP.csv'
SAMPLE_SIZE = 100000 # Ambil 2.000.000 baris saja

file_path_ori = os.path.join(DATA_DIR, FILE_BESAR)
file_path_backup = os.path.join(DATA_DIR, FILE_BACKUP)

# ----------------------

try:
    # 1. Backup file asli (jika belum ada backup)
    if not os.path.exists(file_path_backup):
        print(f"Mem-backup {file_path_ori} -> {file_path_backup}...")
        os.rename(file_path_ori, file_path_backup)
        print("Backup selesai.")
    else:
        print("File backup sudah ada, lanjut.")

    # 2. Baca SAMPLE_SIZE baris dari file BACKUP (file besar)
    print(f"Membaca {SAMPLE_SIZE} baris dari {file_path_backup}...")
    # 'nrows' adalah kuncinya, ini sangat hemat memori
    df_sample = pd.read_csv(file_path_backup, nrows=SAMPLE_SIZE)
    print("Pembacaan sample selesai.")

    # 3. Simpan sample kembali ke nama file ASLI
    print(f"Menyimpan sample ke {file_path_ori}...")
    df_sample.to_csv(file_path_ori, index=False)
    
    print("\nBERHASIL! 🚀")
    print(f"File '{file_path_ori}' sekarang hanya berisi {SAMPLE_SIZE} baris.")
    print(f"File aslimu aman di '{file_path_backup}'.")

except Exception as e:
    print(f"\nERROR: {e}")
    print("Pastikan file 'sales.csv' ada di folder './data/raw/'.")