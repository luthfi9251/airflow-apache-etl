# Airflow ETL

Aplikasi untuk melakukan otomatisasi pada proses migrasi database. Berjalan diatas container dengan versi Python 3.11 dan Apache Airflow 2.8.3

## Langkah Instalasi

1. Clone repository
2. Lakukan Inisialisasi airflow melalui perintah `docker compose up airflow-init`
3. Tunggu hingga selesai

## Menjalankan Aplikasi

1. Menjalankan perintah `docker compose up`
2. Setelah berjalan, buka `localhost` pada browser dengan port default `8080`
3. Login dengan username `airflow` dan password `airflow`

> [!WARNING]
> Pastikan port 8080 belum digunakan oleh service lain

## Environtment Variable

Environtment variable digunakan untuk menentukan koneksi database yang digunakan (production / development). Variabel dapat disimpan pada file `.env`. Berikut variabel yang harus ada dan contoh nilainya:

-   `AIRFLOW_CONN_MYSQL_DEV_CONN` ex. (`mysql://username:password@host:port`)
-   `AIRFLOW_CONN_MYSQL_PROD_CONN` ex. (`mysql://username:password@host:port`)
-   `AIRFLOW_DB_PRODUCTION` ex. `true` atau `false`

`AIRFLOW_DB_PRODUCTION` digunakan untuk menentukan koneksi database yang digunakan produksi/development

> [!TIP]
> Untuk melakukan test koneksi ke database, jalankan DAG dengan nama `utils_test_connection_db`

## Koneksi Database

Koneksi database dilakukan dengan memanfaatkan _connector_ yang telah disediakan oleh Airflow. Berikut langkah penggunaanya:

1. Masuk ke halaman Connection melalui Navbar **Admin** lalu pilih **Connection**
2. Tambahkan Koneksi dengan menekan tombol tambah.
3. Pilih tipe koneksi database dan isi input dengan data yang sesuai.

> Harap setting Environment Variabel sesuai langkah-langkah yang telah diinstruksikan untuk dapat terkoneksi ke database

## Menjalankan Script

Untuk menjalankan script, pastikan airflow sudah memiliki koneksi database yang sesuai. Cari script pada halaman **Dags** dan kemudian jalankan.

## Daftar Script

-   [x] Pengajaran_dosen_perta
-   [x] Yudisium_mhs_angkatan
-   [x] Pendaftaran_mhs_perta
-   [x] Nilai_mhs_perta
-   [x] Kuliah_mhs_perta
-   [x] utils_test_connection_db (melakukan test koneksi database)
