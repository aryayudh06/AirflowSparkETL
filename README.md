# AirflowSparkETL

Proyek ETL (Extract, Transform, Load) untuk memproses data taksi NYC menggunakan Apache Airflow sebagai orkestrator dan Apache Spark untuk pemrosesan data besar. Proyek ini dirancang untuk menjalankan pipeline ETL secara terjadwal atau manual, dengan integrasi Docker untuk kemudahan deployment.

## Fitur

- **Orkestrasi dengan Airflow**: Menggunakan Apache Airflow untuk mengelola dan menjadwalkan tugas ETL.
- **Pemrosesan dengan Spark**: Memanfaatkan Apache Spark untuk transformasi data skala besar.
- **Integrasi Database**: Menyimpan data hasil ETL ke MongoDB.
- **Monitoring**: Terintegrasi dengan Prometheus dan StatsD untuk pemantauan metrik.
- **Dockerized**: Semua komponen dijalankan dalam container Docker untuk konsistensi lingkungan.
- **DAGs untuk Berbagai Periode**: Tersedia DAG untuk pemrosesan data bulanan (Januari, Februari, Maret) dan bulk.

## Prasyarat

- Docker dan Docker Compose terinstal di sistem Anda.
- Minimal 4GB RAM untuk menjalankan semua layanan.
- Port 8080 (Airflow Web UI), 5432 (PostgreSQL), 6379 (Redis), 9090 (Prometheus) harus tersedia.

## Instalasi

1. **Clone Repository**:
   ```bash
   git clone <repository-url>
   cd AirflowSparkETL
   ```
2. **Siapkan data input**
   - Letakkan file CSV data taksi di folder `data/`
   - File yang didukung: `fhv_tripdata_2025-*.csv`, `green_tripdata_2025-*.csv`, `yellow_tripdata_2025-*.csv`, `fhvhv_tripdata_2025-*.csv`

3. **Bangun dan Jalankan Layanan**:
   ```bash
   docker-compose up --build
   ```

   Perintah ini akan:
   - Membangun image Airflow dengan Spark.
   - Menjalankan PostgreSQL, Redis, Airflow Webserver, Scheduler, Worker, dan Prometheus.

4. **Akses Airflow Web UI**:
   - Buka browser dan kunjungi `http://localhost:8080`
   - Username: `airflow`
   - Password: `airflow`

## Penggunaan

### Menjalankan DAG

1. Masuk ke Airflow Web UI.
2. Aktifkan DAG yang diinginkan (misalnya `nyc_taxi_etl_january`).
3. Klik tombol "Trigger DAG" untuk menjalankan secara manual, atau biarkan scheduler menjalankan sesuai jadwal.

### DAG yang Tersedia

- `nyc_taxi_etl_january`: Memproses data taksi bulan Januari.
- `nyc_taxi_etl_bulk`: Memproses data taksi dalam mode bulk.
- `nyc_taxi_etl_jan_feb_concurrent`: Menjalankan ETL Januari dan Februari secara konkuren.
- `nyc_taxi_etl_jan_feb_mar_concurrent`: Menjalankan ETL Januari, Februari, dan Maret secara konkuren.

### Menambahkan Data Input

Tempatkan file data taksi (CSV) di folder `data/` sesuai dengan struktur yang diharapkan oleh DAG (misalnya `data/january/`, `data/february/`, dll.).

### Monitoring

- **Prometheus**: Akses di `http://localhost:9090` untuk melihat metrik.
- **Airflow Logs**: Logs tersimpan di `logs/` dan dapat dilihat melalui Web UI.

## Struktur Proyek

```
AirflowSparkETL/
├── docker-compose.yml          # Konfigurasi Docker Compose
├── Dockerfile                  # Dockerfile untuk image Airflow dengan Spark
├── requirements.txt            # Dependencies Python
├── config/
│   └── airflow.cfg             # Konfigurasi Airflow
├── dags/                       # Folder DAG Airflow
│   ├── nyc_taxi_etl_*.py       # File DAG untuk berbagai periode
│   └── spark_jobs/             # Job Spark
│       ├── taxi_etl.py         # Script ETL utama
│       └── taxi_bulk_etl.py    # Script ETL bulk
├── data/                       # Folder untuk data input
├── logs/                       # Logs Airflow
├── plugins/                    # Plugin Airflow
├── prometheus/
│   └── prometheus.yml          # Konfigurasi Prometheus
└── spark/
    └── logs/                   # Logs Spark
```

## Kontribusi

1. Fork repository ini.
2. Buat branch fitur baru (`git checkout -b feature/AmazingFeature`).
3. Commit perubahan Anda (`git commit -m 'Add some AmazingFeature'`).
4. Push ke branch (`git push origin feature/AmazingFeature`).
5. Buat Pull Request.

## Lisensi

Proyek ini menggunakan lisensi MIT. Lihat file `LICENSE` untuk detail lebih lanjut.

## Troubleshooting

- **Port Conflict**: Jika port sudah digunakan, ubah port di `docker-compose.yml`.
- **Memory Issues**: Tingkatkan limit memori Docker jika terjadi out-of-memory.
- **Spark Job Failures**: Periksa logs di `logs/` dan `spark/logs/` untuk detail error.

Untuk pertanyaan lebih lanjut, buka issue di repository ini.