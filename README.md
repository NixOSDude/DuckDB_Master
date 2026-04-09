# DuckDB Data Master

**171M rows/sec. Single node. No Spark. No cluster bill.**

Data engineering consulting by Scott Baker — Databricks Certified, AWS Solutions Architect.

## What's in this repo

```
scripts/
  benchmark_nyc_4year.sh   # 5 DuckDB queries across 685M rows — the benchmark
  download_nyc_taxi.sh     # Download NYC TLC Parquet data (2022–2025)

notebooks/
  nyc_taxi_viz.py          # Streamlit dashboard — live charts from DuckDB

docs/
  index.html               # Website → duckdatamaster.guru (GitHub Pages)
```

## Run the benchmark

```bash
# 1. Install DuckDB (https://duckdb.org/docs/installation)

# 2. Download the data (~12GB for yellow cab 2022–2025)
bash scripts/download_nyc_taxi.sh --type=yellow --year=2022
bash scripts/download_nyc_taxi.sh --type=yellow --year=2023
bash scripts/download_nyc_taxi.sh --type=yellow --year=2024
bash scripts/download_nyc_taxi.sh --type=yellow --year=2025

# 3. Run the benchmark
bash scripts/benchmark_nyc_4year.sh
```

## Run the Streamlit dashboard

```bash
pip install streamlit duckdb pandas plotly
streamlit run notebooks/nyc_taxi_viz.py
```

## Contact

**scott.bakerphx@gmail.com** · [duckdatamaster.guru](https://duckdatamaster.guru)
