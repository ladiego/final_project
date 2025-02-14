import os
import logging
import time
import pandas as pd
from pandas_gbq import to_gbq
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from google.cloud import bigquery
from datetime import datetime

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path absolut untuk menyimpan CSV
CSV_PATH = "/tmp/scraped_data.csv"

# Fungsi untuk membuat tabel di BigQuery jika belum ada
def create_table_if_not_exists(client, dataset_id, table_id):
    schema = [
        bigquery.SchemaField("Jumlah Akumulasi Peminjam - orang", "INT64"),
        bigquery.SchemaField("Jumlah Peminjam Aktif-orang", "INT64"),
        bigquery.SchemaField("Jumlah Peminjam Perorangan - Tahun Berjalan", "INT64"),
        bigquery.SchemaField("Total Pinjaman Yang Sudah Disalurkan", "INT64"),
        bigquery.SchemaField("Akumulasi Pinjaman Tahun ini", "INT64"),
        bigquery.SchemaField("Akumulasi Pinjaman Posisi Akhir Bulan", "INT64"),
        bigquery.SchemaField("Total Outstanding Pinjaman", "INT64"),
        bigquery.SchemaField("Jumlah Akumulasi Pemberi Dana", "INT64"),
        bigquery.SchemaField("Jumlah Akumulasi Pemberi Dana Tahun Berjalan", "INT64"),
        bigquery.SchemaField("Jumlah Akumulasi Pemberi Dana Posisi Akhir Bulan", "INT64"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    try:
        client.create_table(table)
        logging.info(f"Tabel {table.table_id} berhasil dibuat di BigQuery.")
    except Exception as e:
        logging.warning(f"Tabel {table_id} sudah ada: {e}")

# Fungsi untuk konversi angka ke integer
def convert_value(value):
    value = value.replace(" ", "")
    if "Ribu" in value:
        return int(float(value.replace("Ribu", "")) * 1000)
    elif "M" in value:
        return int(float(value.replace("M", "")) * 1000000)
    else:
        return int(value)

# Fungsi untuk scraping dan menyimpan ke CSV
def save_to_csv():
    try:
        logging.info("Memulai scraping dengan Selenium...")

        # Konfigurasi Chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Inisialisasi WebDriver
        service = ChromeService(ChromeDriverManager().install())
        browser = webdriver.Chrome(service=service, options=chrome_options)

        # Akses URL
        url = 'http://www.adapundi.com/'
        browser.get(url)

        # Tunggu elemen agar benar-benar muncul
        WebDriverWait(browser, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'col-md-3')]"))
        )

        # Ambil ulang HTML setelah JavaScript selesai dieksekusi
        html = browser.execute_script("return document.body.innerHTML;")
        soup = BeautifulSoup(html, "html.parser")

        # Ambil data
        parent = soup.find_all("div", class_="col-md-3 col-12")

        # Dictionary untuk menyimpan data dalam format BigQuery
        data_dict = {
            "Jumlah Akumulasi Peminjam - orang": None,
            "Jumlah Peminjam Aktif-orang": None,
            "Jumlah Peminjam Perorangan - Tahun Berjalan": None,
            "Total Pinjaman Yang Sudah Disalurkan": None,
            "Akumulasi Pinjaman Tahun ini": None,
            "Akumulasi Pinjaman Posisi Akhir Bulan": None,
            "Total Outstanding Pinjaman": None,
            "Jumlah Akumulasi Pemberi Dana": None,
            "Jumlah Akumulasi Pemberi Dana Tahun Berjalan": None,
            "Jumlah Akumulasi Pemberi Dana Posisi Akhir Bulan": None,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        for div in parent:
            title_element = div.find("p")
            value_element = div.find("h5")

            if title_element and value_element:
                title = title_element.text.strip()
                value = value_element.text.strip()
                converted_value = convert_value(value)
                if title in data_dict:
                    data_dict[title] = converted_value

        # Simpan hasil scraping ke CSV
        df = pd.DataFrame([data_dict])
        df.to_csv(CSV_PATH, index=False)
        logging.info(f"Data berhasil disimpan ke {CSV_PATH}")

        # Tutup browser
        browser.quit()
    except Exception as e:
        logging.error(f"Error saat scraping: {e}")
        raise e

# Fungsi untuk memuat data ke BigQuery
def load_to_bq():
    logging.info("Mulai upload data ke BigQuery...")
    
    # Pastikan kredensial Google Cloud tersedia
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/keys/gcp_keys.json"
    
    client = bigquery.Client()
    dataset_id = 'purwadika.diego_library_final_project'
    table_id = f"{dataset_id}.adapundi"

    # Periksa apakah file CSV ada sebelum memproses
    if not os.path.exists(CSV_PATH):
        logging.error(f"File {CSV_PATH} tidak ditemukan! Tidak ada data untuk diupload.")
        return

    # Pastikan tabel ada di BigQuery sebelum mengupload data
    create_table_if_not_exists(client, dataset_id, table_id)

    try:
        # Baca CSV
        df = pd.read_csv(CSV_PATH)
        
        # Upload data ke BigQuery tanpa menggantikan data lama
        to_gbq(df, table_id, if_exists='append', progress_bar=False)
        logging.info("Data baru berhasil ditambahkan ke BigQuery.")

        # Hapus file CSV setelah berhasil diupload
        os.remove(CSV_PATH)
        logging.info(f"File CSV {CSV_PATH} berhasil dihapus setelah upload.")
    except Exception as e:
        logging.error(f"Error saat upload ke BigQuery: {e}")

# **Cara Menjalankan Kode Secara Manual**
if __name__ == "__main__":
    save_to_csv()  # Jalankan scraping
    load_to_bq()   # Upload ke BigQuery
