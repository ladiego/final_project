**FINAL PROJECT \- JCDEOL \- PURWADHIKA**

Final Project ini dirancang untuk mensimulasikan alur kerja (workflow) seorang Data Engineer dalam membangun pipeline data dari sumber data (PostgreSQL) ke tujuan data (BigQuery) dan pengaplikasian DBT. Secara spesifik, tujuan project ini adalah:

* Membangun ETL Pipeline:  
  * Extract: Mengekstrak data dari database PostgreSQL.  
  * Transform: Memproses data (jika diperlukan) dan menyimpannya dalam format sementara (CSV).  
  * Load: Memuat data ke dalam BigQuery, baik ke tabel staging maupun tabel final.  
* Otomatisasi Proses Data:  
  * Menggunakan Apache Airflow untuk mengotomatisasi alur kerja ETL, termasuk penjadwalan, monitoring, dan notifikasi.  
* Integrasi dengan BigQuery:  
  * Membuat dataset dan tabel di BigQuery, serta memastikan data yang dimuat adalah data yang valid dan terstruktur dengan baik.  
* Notifikasi dan Monitoring:  
  * Mengirim notifikasi ke Discord untuk memantau status tugas (sukses, gagal, atau retry) sehingga tim dapat merespons dengan cepat jika terjadi masalah.  
* Penggunaan Data Dummy:  
  * Menggunakan library Faker untuk menghasilkan data dummy yang realistis, sehingga pipeline dapat diuji tanpa perlu data produksi yang sebenarnya.  
* Pembelajaran Best Practices:  
  * Menerapkan praktik terbaik dalam pengelolaan data, seperti partisi tabel di BigQuery, penggunaan staging table, dan penanganan error yang baik.

**1\. Apa itu DAG?**  
DAG (Directed Acyclic Graph) adalah representasi grafis dari alur kerja (workflow) yang terdiri dari serangkaian tugas (tasks) yang diatur dalam urutan tertentu. Dalam konteks Apache Airflow, DAG digunakan untuk mendefinisikan dan mengelola alur kerja data.

* Directed: Tugas-tugas memiliki arah, artinya ada urutan yang jelas dari satu tugas ke tugas berikutnya.  
* Acyclic: Tidak ada siklus atau loop dalam alur kerja, artinya tugas-tugas tidak dapat kembali ke tugas sebelumnya.  
* Graph: Tugas-tugas dihubungkan dalam bentuk graf, di mana setiap node mewakili tugas dan edge mewakili ketergantungan antar tugas.

DAG memungkinkan Data Engineer untuk mengotomatisasi, menjadwalkan, dan memonitor alur kerja data secara efisien.

1. Hasil dari Project yang Dikerjakan  
   Hasil dari project ini adalah pipeline data otomatis yang:  
* Membuat dan Memasukkan Data Dummy:  
  * Data dummy untuk tabel users dan books dihasilkan menggunakan library Faker.  
  * Data tersebut dimasukkan ke dalam database PostgreSQL.  
* Mengekstrak Data dari PostgreSQL:  
  * Data dari tabel users, books, dan rents diekstrak dan disimpan dalam file CSV.  
* Memuat Data ke BigQuery:  
  * Data dari file CSV dimuat ke tabel staging di BigQuery.  
  * Data yang valid dari tabel staging kemudian dimuat ke tabel final di BigQuery.  
* Notifikasi Otomatis:  
  * Notifikasi dikirim ke Discord untuk memberi tahu status tugas (sukses, gagal, atau retry).  
* Otomatisasi dengan Airflow:  
  * Seluruh proses diatur dan dijadwalkan menggunakan DAG di Apache Airflow, sehingga pipeline dapat berjalan secara otomatis tanpa intervensi manual.


2. Tahapan Project yang Dikerjakan  
   Berikut adalah tahapan project yang telah dikerjakan, dengan beberapa poin penting yang di-highlight:  
   Tahap 1: Persiapan dan Setup  
* Membuat Koneksi ke PostgreSQL:  
  * Fungsi create\_connection() di helper\_postgres.py digunakan untuk membuat koneksi ke database PostgreSQL.  
* Membuat Schema dan Tabel:  
  * Fungsi create\_schema\_and\_tables() di helper\_postgres.py membuat schema library dan tabel users, books, serta rents jika belum ada.

  Tahap 2: Pembuatan Data Dummy

* Generate Data Dummy:  
  * Fungsi generate\_data() di helper\_postgres.py menghasilkan data dummy untuk tabel users dan books menggunakan library Faker.  
* Insert Data ke PostgreSQL:  
  * Fungsi insert\_data() di helper\_postgres.py memasukkan data dummy ke dalam tabel users dan books, serta membuat data dummy untuk tabel rents.

  Tahap 3: Ekstraksi Data dari PostgreSQL

* Ekstraksi Data:  
  * Fungsi extract\_table\_data() di ext\_sql.py mengekstrak data dari tabel users, books, dan rents di PostgreSQL.  
* Penyimpanan ke CSV:  
  * Fungsi save\_to\_csv() di ext\_sql.py menyimpan data yang diekstrak ke dalam file CSV.

  Tahap 4: Memuat Data ke BigQuery

* Membuat Dataset dan Tabel di BigQuery:  
  * Fungsi create\_dataset\_if\_not\_exists() dan create\_table\_if\_not\_exists() di crt\_dataset\_table.py digunakan untuk membuat dataset dan tabel di BigQuery jika belum ada.  
* Memuat Data ke Staging Table:  
  * Fungsi load\_data\_to\_staging() di sql\_to\_bigquery.py memuat data dari file CSV ke tabel staging di BigQuery.  
* Memuat Data ke Final Table:  
  * Fungsi load\_data\_to\_final() di sql\_to\_bigquery.py memuat data dari tabel staging ke tabel final di BigQuery.

  Tahap 5: Otomatisasi dengan Airflow

* Membuat DAG untuk Ingest Data ke PostgreSQL:  
  * DAG ingest\_data\_db di ingest\_sql.py mengotomatisasi proses pembuatan dan pemasukan data dummy ke PostgreSQL.  
* Membuat DAG untuk Ekstraksi dan Load ke BigQuery:  
  * DAG extract\_load\_to\_bigquery di sql\_to\_bigquery.py mengotomatisasi proses ekstraksi data dari PostgreSQL, penyimpanan ke CSV, dan pemuatan ke BigQuery.

  Tahap 6: Notifikasi dan Monitoring

* Notifikasi ke Discord:  
  * Fungsi notify\_on\_success(), notify\_on\_error(), dan notify\_on\_retry() di notify.py mengirim notifikasi ke Discord berdasarkan status tugas (sukses, gagal, atau retry).

**2\. Apa itu DBT?**  
DBT (data build tool) adalah alat yang digunakan untuk melakukan transformasi data dalam data warehouse. dbt memungkinkan Data Engineer dan Analis Data untuk:

* Mengubah data mentah menjadi data yang siap digunakan untuk analisis.  
* Mengelola transformasi data menggunakan SQL dan konfigurasi berbasis file (seperti YAML).  
* Membuat dokumentasi otomatis untuk model data dan kolom-kolomnya.  
* Mengotomatisasi pipeline data dengan menjalankan transformasi secara teratur.

DBT tidak menangani ekstraksi atau pemuatan data (EL), tetapi fokus pada transformasi data (T) dalam proses ETL/ELT. DBT bekerja dengan data warehouse seperti BigQuery, Snowflake, Redshift, dan lainnya.

1.  Hasil dari Project yang Dikerjakan

Hasil dari project ini adalah pipeline transformasi data yang:

* Mengubah Data Mentah Menjadi Data Siap Analisis:  
  * Data dari tabel users, books, dan rents di BigQuery diproses dan disiapkan untuk analisis lebih lanjut.  
  * Data yang sudah dibersihkan dan ditransformasi disimpan dalam bentuk view atau tabel materialized di BigQuery.  
* Incremental Load:  
  * Model dbt diatur untuk melakukan incremental load, artinya hanya data baru yang diproses setiap kali pipeline dijalankan. Ini diatur menggunakan partisi berdasarkan kolom created\_at.  
* Dokumentasi dan Organisasi:  
  * dbt digunakan untuk mendokumentasikan model data dan kolom-kolomnya.  
  * Model data diorganisasi ke dalam beberapa schema (diego\_dwh\_library\_preparation, diego\_dwh\_library\_dimfact, diego\_dwh\_library\_marts) untuk memisahkan tahapan transformasi data.  
* Integrasi dengan BigQuery:  
  * dbt terintegrasi dengan BigQuery untuk melakukan transformasi data secara efisien.

2. Tahapan Project yang Dikerjakan

Tahap 1: Setup dbt dan BigQuery

* Konfigurasi dbt:  
  * File profiles.yml digunakan untuk mengkonfigurasi koneksi dbt ke BigQuery. Ini mencakup informasi seperti project ID, dataset, lokasi, dan file kredensial.  
  * File dbt\_project.yml digunakan untuk mengkonfigurasi project dbt, termasuk path model, target path, dan konfigurasi materialisasi model (view atau tabel materialized).

Tahap 2: Pembuatan Model Data (Preparation Layer)

* Model untuk users:  
  * File prep\_users.sql digunakan untuk membuat model data untuk tabel users. Model ini melakukan incremental load dan partisi berdasarkan kolom created\_at.  
  * Data yang sudah dibersihkan dan dipilih kolom yang relevan disimpan dalam schema diego\_dwh\_library\_preparation.  
* Model untuk books:  
  * File prep\_books.sql digunakan untuk membuat model data untuk tabel books. Model ini juga melakukan incremental load dan partisi berdasarkan kolom created\_at.  
  * Data yang sudah dibersihkan dan dipilih kolom yang relevan disimpan dalam schema diego\_dwh\_library\_preparation.  
* Model untuk rents:  
  * File prep\_rents.sql digunakan untuk membuat model data untuk tabel rents. Model ini melakukan incremental load dan partisi berdasarkan kolom created\_at.  
  * Data yang sudah dibersihkan dan dipilih kolom yang relevan disimpan dalam schema diego\_dwh\_library\_preparation.

Tahap 3: Definisi Sumber Data

* File sources.yml:  
  * File ini digunakan untuk mendefinisikan sumber data di BigQuery yang akan digunakan oleh dbt. Ini mencakup tabel users, books, dan rents dari dataset diego\_library\_final\_project.

Tahap 4: Pembuatan Model Data (Dimensi dan Fakta)

* Model Dimensi (Dim):  
  * Model dimensi dibuat untuk menyimpan data yang bersifat deskriptif dan statis, seperti informasi pengguna (dim\_users), informasi buku (dim\_books).  
  * Model dimensi ini disimpan dalam schema diego\_dwh\_library\_dimfact.  
* Model Fakta (Fact):  
  * Model fakta dibuat untuk menyimpan data yang bersifat transaksional, seperti transaksi peminjaman buku (fact\_rents).  
  * Model fakta ini disimpan dalam schema diego\_dwh\_library\_dimfact.

Tahap 5: Pembuatan Datamart

* Datamart:  
  * Datamart dibuat untuk menyimpan data yang sudah diolah dan siap digunakan untuk analisis bisnis. Datamart ini menggabungkan data dari model dimensi dan fakta.  
  * Datamart ini disimpan dalam schema diego\_dwh\_library\_marts.

Tahap 6: Dokumentasi dan Organisasi

* Dokumentasi:  
  * dbt digunakan untuk mendokumentasikan model data dan kolom-kolomnya. Ini memudahkan tim analisis dan bisnis untuk memahami struktur data.  
* Organisasi Model:  
  * Model data diorganisasi ke dalam beberapa schema (diego\_dwh\_library\_preparation, diego\_dwh\_library\_dimfact, diego\_dwh\_library\_marts) untuk memisahkan tahapan transformasi data.

	

**Gambar.1** workflow data pipeline

**3\. Apa itu Web Scraping?**  
Web Scraping adalah teknik yang digunakan untuk mengekstrak data dari website secara otomatis. Proses ini melibatkan pengambilan konten dari halaman web, biasanya dalam bentuk HTML, dan kemudian mengurai (parsing) konten tersebut untuk mengambil informasi yang diinginkan. Web scraping sering digunakan untuk mengumpulkan data dari berbagai sumber di internet, seperti harga produk, berita, informasi kontak, atau data statistik.

1. Cara Kerja Web Scraping  
* Mengakses Halaman Web:  
  * Sebuah program atau script mengakses URL halaman web menggunakan library atau tools seperti requests (Python) atau browser otomatis seperti Selenium.  
* Mengambil Konten HTML:  
  * Konten HTML dari halaman web diambil dan disimpan dalam bentuk teks.  
* Mengurai (Parsing) HTML:  
  * Konten HTML diurai menggunakan library seperti BeautifulSoup (Python) atau lxml untuk mengekstrak informasi yang diinginkan. Ini melibatkan pencarian elemen HTML tertentu (seperti tag, class, atau id) yang mengandung data yang relevan.  
* Menyimpan Data:  
  * Data yang diekstrak disimpan dalam format yang dapat digunakan, seperti CSV, JSON, atau database.

2. Kegunaan Web Scraping  
1. Pengumpulan Data:  
   * Mengumpulkan data dari berbagai sumber untuk analisis, seperti harga produk, data pasar, atau informasi kompetitor.  
2. Monitoring:  
   * Memantau perubahan konten di website, seperti perubahan harga atau ketersediaan stok.  
3. Penelitian:  
   * Mengumpulkan data untuk keperluan penelitian akademis atau bisnis.  
4. Integrasi Data:  
   * Mengumpulkan data dari berbagai website untuk diintegrasikan ke dalam sistem internal.

c. Etika dan Legalitas Web Scraping

* Etika: Selalu periksa robots.txt di website untuk melihat apakah scraping diizinkan.  
* Legalitas: Beberapa website melarang scraping dalam syarat dan ketentuan mereka. Selalu pastikan untuk mematuhi aturan yang berlaku.

**Gambar.2** Alur proses Web Scraping pada project