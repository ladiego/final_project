**FINAL PROJECT \- JCDEOL \- PURWADHIKA**

**BAGIAN I**

**Data Pipeline dengan Apache Airflow, PostgreSQL, BigQuery, dan DBT**  
**1\. Pendahuluan**

Proyek ini mensimulasikan workflow seorang Data Engineer dalam membangun pipeline data dari sumber data (PostgreSQL) ke data warehouse (BigQuery) dengan otomatisasi menggunakan Apache Airflow dan transformasi data menggunakan DBT.

Tujuan Proyek

* **Membangun ETL Pipeline**  
  * **Extract:** Mengambil data dari PostgreSQL.  
  * **Transform:** Melakukan pemrosesan data dan menyimpannya sementara dalam format CSV.  
  * **Load:** Memasukkan data ke BigQuery dalam tabel staging dan tabel final.  
* **Otomatisasi Proses Data** menggunakan Apache Airflow.  
* **Integrasi dengan BigQuery** untuk memastikan data valid dan terstruktur.  
* **Notifikasi & Monitoring** melalui Discord.  
* **Penggunaan Data Dummy** menggunakan library **`Faker`**.  
* **Penerapan Best Practices** seperti partisi tabel, staging table, dan error handling.

---

**2\. Arsitektur & Workflow**

Pipeline ini terdiri dari beberapa komponen utama:

1. **PostgreSQL** sebagai database sumber.  
2. **Apache Airflow** untuk orkestrasi ETL.  
3. **BigQuery** sebagai data warehouse tujuan.  
4. **DBT** untuk transformasi data setelah dimuat ke BigQuery.  
5. **Notifikasi ke Discord** untuk monitoring tugas.

**Diagram Arsitektur:**  
![A711CAB0-2457-4579-AF63-1F1D2A12C309](https://github.com/user-attachments/assets/41671f61-faf2-4e7f-bf2f-4e24b5284033)

---

**3\. Teknologi yang Digunakan**

* **Python**: Untuk skrip ETL.  
* **PostgreSQL**: Database relasional untuk penyimpanan awal.  
* **BigQuery**: Data warehouse untuk analisis.  
* **Apache Airflow**: Orkestrasi pipeline data.  
* **DBT (Data Build Tool)**: Transformasi data di warehouse.  
* **Docker & Docker Compose**: Deployment dan isolasi lingkungan.  
* **Faker**: Pembuatan data dummy.  
* **Discord Webhook**: Notifikasi status pipeline.

---

**4\. Persiapan dan Instalasi**

4.1. Prasyarat

Sebelum menjalankan proyek, pastikan telah menginstal:

* **Docker & Docker Compose**  
* **Python 3.8+**  
* **Google Cloud SDK** (untuk autentikasi BigQuery)

4.2. Clone Repository & Instalasi Dependensi
   `git clone https://github.com/ladiego/final_project.git`  
   `Cd final-project`

4.3. Instalasi Dependensi Python  
`pip install -r requirements.txt`

4.4. Pastikan Docker dan Docker Compose terinstal:

	`docker --version`
	`docker-compose --version`

4.5. Konfigurasi Kredensial

1. **PostgreSQL:** Sesuaikan konfigurasi database dalam `docker-compose.yml`.  
2. **BigQuery:** Simpan file kredensial Google Cloud dalam direktori proyek dan tambahkan variabel lingkungan:  
   `export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"`  
3. **Discord Webhook:** Tambahkan URL webhook dalam `notify.py`.

---

**5\. Menjalankan Proyek**

5.1. Menjalankan Docker Compose

Proyek ini memiliki 4 layanan utama dalam Docker Compose:

* **Database (`docker-compose.yml`)**  
* **Airflow (`docker-compose.yml`)**  
* **Production Airflow (`docker-compose.yml`)**  
* **DBT (`docker-compose.yml`)**

**Langkah-langkah menjalankan layanan:**

`docker-compose -f postgres_db/docker-compose.yml up -d`  
`docker-compose -f postgres_airflow/docker-compose.yml up -d`  
`docker-compose -f production_airflow/docker-compose.yml up -d`  
`docker-compose -f dbt/docker-compose.yml up -d`

Cek apakah layanan berjalan dengan:

`docker ps`

5.2. Mengakses Apache Airflow

1. Buka browser dan masuk ke:

	`http://localhost:8080`

2. Gunakan kredensial default Airflow (`airflow.cfg`) untuk login.
  * username : `airflow`
  * password : `airflow`

---

**6\. Proses ETL dalam Apache Airflow**
![4C60E197-116F-4F60-9D43-D42BA2AE7E92_1_105_c](https://github.com/user-attachments/assets/abb83f3f-11d7-464a-b281-eab2035f428a)

6.1. DAG 1: Ingest Data ke PostgreSQL
![AD087C27-E321-4E04-B154-9CAF19F70189_1_105_c](https://github.com/user-attachments/assets/a9d826eb-72de-4794-9697-39a7d96575e8)

* **Tugas**: Membuat tabel dan memasukkan data dummy ke PostgreSQL.  
* **File DAG**: `ingest_sql.py`  
* **Komponen Utama**:  
  * `create_schema_and_tables()`  
  * `generate_data()`  
  * `insert_data()`

6.2. DAG 2: Ekstraksi dan Load ke BigQuery
![92C1082E-6123-4C8C-B148-F60645BE666E](https://github.com/user-attachments/assets/118d7d41-f287-40ea-add7-ae82b0cf5009)

* **Tugas**: Mengekstrak data dari PostgreSQL, menyimpannya ke CSV, lalu mengunggahnya ke BigQuery.  
* **File DAG**: `sql_to_bigquery.py`  
* **Komponen Utama**:  
  * `extract_table_data()`  
  * `save_to_csv()`  
  * `load_data_to_staging()`  
  * `load_data_to_final()`

6.3. DAG 3: dbt
![F9C545D1-EE52-4D98-99CB-08EB2F10ADB9](https://github.com/user-attachments/assets/41170b6d-888b-4a2f-a178-c755c0746665)

* **Tugas**: membersihkan, menyiapkan, dan membuat data untuk tabel books, rents, users yang selanjutnya digunakan untuk analisis.  
* **File DAG**: `dbt_running.py`  
* **Komponen Utama**:  
  * `extract_table_data()`  
  * `save_to_csv()`  
  * `load_data_to_staging()`  
  * `load_data_to_final()`

6.4. Notifikasi ke Discord
![E6F539F0-468F-4A75-955C-563061E893CD](https://github.com/user-attachments/assets/e9a5c25c-c750-4fa2-ae86-be7f53f30f3e)

Setiap tugas dalam DAG akan mengirimkan notifikasi ke Discord:

* **Sukses** → `notify_on_success()`  
* **Gagal** → `notify_on_error()`  
* **Retry** → `notify_on_retry()`

---

**7\. Transformasi Data dengan DBT**

7.1. Konfigurasi DBT

* File konfigurasi **`profiles.yml`** untuk koneksi ke BigQuery.  
* File **`dbt_project.yml`** untuk mengatur proyek DBT.

7.2. Menjalankan DBT  
 * Masuk ke folder DBT:
	`cd dbt`
 * Verifikasi koneksi ke BigQuery:
	`dbt debug`
 * Jalankan transformasi data dengan DBT:
	`dbt run`

7.3. Model Transformasi

1. **Preparation Layer** (**`diego_finpro_preparation`**)  
   * Membersihkan dan mempersiapkan data (users, books, rents).  
2. **Dimensi & Fakta (`diego_finpro_dimfact`)**  
   * Membuat tabel **dim\_users**, **dim\_books**, dan **fact\_rents**.  
3. **Datamart (`diego_finpro_marts`)**  
   * Menggabungkan data dari dimensi dan fakta untuk analisis.
![46D6CDD5-5E31-43B4-B169-BC41AE59E427](https://github.com/user-attachments/assets/723064dc-b870-4358-b381-fd4ba7ea9d98)
  
---

**8\. Monitoring & Debugging**

8.1. Monitoring di Airflow

* Cek status DAG di UI Airflow.  
* Gunakan **task logs** untuk debugging jika terjadi error.

8.2. Log di Docker  
`docker logs airflow_scheduler`  
`docker logs airflow_webserver`

8.3. Validasi Data di BigQuery

Gunakan SQL di BigQuery Console:

`` SELECT * FROM `project_id.dataset.table` ``  
`Contoh :`   
`` SELECT * FROM `purwadika.diego_library_finpro.books` ``  
---

**9\. Kesimpulan**

Proyek ini membangun pipeline data otomatis dengan PostgreSQL, Apache Airflow, BigQuery, dan DBT. Dengan implementasi ini:

* **ETL dapat berjalan otomatis** tanpa campur tangan manual.  
* **Transformasi data lebih terstruktur** menggunakan DBT.  
* **Monitoring lebih mudah** dengan notifikasi Discord.  
* **Pipeline dapat diperluas** untuk proyek-proyek lainnya.

**BAGIAN II**

**1\. Web Scraping dalam Pipeline**  
Selain data dari PostgreSQL, proyek ini juga melakukan web scraping dari situs [Adapundi](http://www.adapundi.com) untuk mendapatkan data tambahan yang akan dimuat ke BigQuery.  
![FA5C3F2F-F6C0-46E6-AD2F-AC7847C55567_1_105_c](https://github.com/user-attachments/assets/a5ae41e5-c4a6-4c5b-87ad-c7c6c373f5cb)
  
1.1. Teknologi yang Digunakan

* Selenium & BeautifulSoup: Untuk mengakses dan mengekstrak data dari web.  
* Pandas: Untuk memproses dan menyimpan data sementara dalam CSV.  
* Google BigQuery: Sebagai tujuan akhir penyimpanan data.  
* Apache Airflow: Untuk mengotomatisasi scraping dan pemuatan data ke BigQuery.

1.2. Workflow Web Scraping

1. Scraping Data  
   * Menggunakan Selenium untuk mengambil halaman web.  
   * BeautifulSoup digunakan untuk mengekstrak data numerik dari elemen HTML.  
   * Data yang berhasil diambil disimpan dalam CSV.  
2. Menyimpan ke BigQuery  
   * Membuat tabel jika belum ada.  
   * Memuat data dari CSV ke BigQuery.  
3. Otomatisasi dengan Airflow  
   * DAG scraping\_webadapundi menjalankan scraping setiap hari pukul 03:00 AM.  
   * Jika berhasil, data diunggah ke BigQuery.  
   * Jika gagal, sistem mencoba kembali dan mengirim notifikasi ke Discord.

---

**2\. Menjalankan DAG Web Scraping**
![56AAA49A-4ED9-4BA7-A3EB-9535387E47D2](https://github.com/user-attachments/assets/bdfd727e-edc0-4f27-b7e0-715c4f72d23f)
  
Untuk menjalankan DAG ini, pastikan layanan Airflow sudah berjalan, lalu aktifkan DAG di UI Airflow:  
2.1 Pastikan layanan Airflow aktif

	`docker-compose -f postgres_db/docker-compose.yml up -d`  
	`docker-compose -f postgres_airflow/docker-compose.yml up -d`  
	`docker-compose -f production_airflow/docker-compose.yml up -d`

2.2 Buka UI Apache Airflow

	[http://localhost:8080](http://localhost:8080)

2.3 Cari DAG bernama scraping\_webadapundi, lalu aktifkan DAG.  
2.4 Jalankan DAG secara manual (opsional) untuk menguji scraping.  
---

**3\. Validasi & Debugging Web Scraping**  
3.1. Cek Log Airflow  
Jika scraping mengalami kegagalan, cek log di Airflow:

`docker logs airflow_scheduler`  
`docker logs airflow_webserver`

3.2. Cek Data di BigQuery  
Gunakan query SQL berikut untuk melihat hasil scraping:

``SELECT * FROM `purwadika.diego_library_finpro.adapundi` ORDER BY created_at DESC;``

![B10474C6-A916-4F7D-B5A2-7DF0E479053C_1_105_c](https://github.com/user-attachments/assets/a5c21a2a-db23-4d9b-9dfc-c28741abb0fd)
  
---

**4\. Kesimpulan**  
Penambahan web scraping dalam pipeline ini meningkatkan cakupan data yang tersedia untuk analisis. Dengan integrasi Apache Airflow, Selenium, dan BigQuery, proses scraping menjadi lebih otomatis dan dapat di-monitoring dengan notifikasi ke Discord.  

