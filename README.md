# Scott Baker — Data Engineer & DuckDB Specialist

**172,871,932 rows/sec. 167M rows. 971ms. One machine. No cluster.**

---

## About

I am a data engineer specializing in high-performance columnar analytics — replacing overpriced
Spark clusters and Databricks bills with single-node DuckDB pipelines that are faster, cheaper,
and operationally simpler.

I have over a decade of experience spanning enterprise security, cloud infrastructure, and data
engineering across AWS, Azure, and GCP. I benchmark production workloads, architect cloud-native
data pipelines, and deliver results you can verify.

**Certifications:**
- [Databricks Certified Associate Developer for Apache Spark (Scala)](https://credentials.databricks.com/embed/96e65e17-56d5-4682-a8f4-c9e77b2a7251)
- [AWS Certified Solutions Architect — Associate](https://www.credly.com/badges/0957ca40-e7ba-4984-99b8-444E8DE58A18/public_url)

---

## Verified Benchmark — April 2026

```
Dataset : NYC Yellow Cab 2022–2025 · 48 Parquet files · 2.4 GB compressed
Rows    : 167,858,646
Queries : 5 analytical queries (COUNT, GROUP BY, PIVOT, window aggregates)
Time    : 971ms total wall time · cold NVMe · no warmup
Speed   : 172,871,932 rows/sec
vs Spark: 25× faster than Apache Spark (50 GB heap, pre-warmed, identical hardware)
Cost    : $0 cluster · 1 machine · 1 process
```

| Query | Description | Time |
|-------|-------------|------|
| Q1 | Row count per year — 4 years | 29ms |
| Q2 | Avg fare, distance, tip, total, passengers YoY — 143M filtered rows | 401ms |
| Q3 | Monthly trip volume pivot — 48 cells, native DuckDB PIVOT | 312ms |
| Q4 | Payment type shift 2022→2025 — cash collapse 19.6% → 9.6% | 158ms |
| Q5 | CBD congestion fee — 2025-only column, 72.8% of trips, $25.03M captured | 64ms |

Hardware: Intel Ultra 7 265KF (20-core) · NVMe SSD · 64GB RAM · Arch Linux · DuckDB 1.4.4

---

## What's in This Repo

```
docs/
  index.html   — duckdatamaster.guru (GitHub Pages)
  resume.html  — Full resume
```

---

## Services

- DuckDB pipeline builds — S3, Azure Blob, GCP Storage
- Databricks cost audits — identify what can move off the cluster today
- Bare-metal / NVMe architecture — stop paying S3 I/O tax on hot analytical workloads
- Streamlit dashboards — live analytics backed by DuckDB, zero cluster dependency
- Spark migration — port SparkSQL jobs to standard DuckDB SQL
- Post-quantum secured pipelines — ML-DSA-65 signing + ML-KEM key exchange
- AWS / Azure / GCP data architecture
- Architecture reviews — written recommendations delivered in 48 hours

---

## Contact

**scott@duckdatamaster.guru** · [duckdatamaster.guru](https://duckdatamaster.guru)
