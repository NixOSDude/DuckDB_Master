#!/usr/bin/env bash
# =============================================================================
# benchmark_nyc_4year.sh — NYC Yellow Cab 2022–2025 DuckDB Benchmark
#
# Runs 5 analytical queries across 4 years of NYC taxi Parquet data.
# Achieved: 171M rows/sec on a single node (Intel Ultra 7 265KF, NVMe).
#
# Usage:
#   bash scripts/benchmark_nyc_4year.sh
#
# Data directory layout expected:
#   data/nyc_taxi/parquet/yellow/2022/yellow_tripdata_2022-*.parquet
#   data/nyc_taxi/parquet/yellow/2023/yellow_tripdata_2023-*.parquet
#   data/nyc_taxi/parquet/yellow/yellow_tripdata_2024-*.parquet
#   data/nyc_taxi/parquet/yellow/2025/yellow_tripdata_2025-*.parquet
#
# Download data first: bash scripts/download_nyc_taxi.sh
# =============================================================================

set -e
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Use duckdb from PATH (install: https://duckdb.org/docs/installation)
DUCKDB=${DUCKDB_BIN:-duckdb}

command -v "$DUCKDB" >/dev/null 2>&1 || {
    echo "ERROR: duckdb not found. Install from https://duckdb.org/docs/installation"
    echo "       or set DUCKDB_BIN=/path/to/duckdb"
    exit 1
}

Y22="data/nyc_taxi/parquet/yellow/2022/yellow_tripdata_2022-*.parquet"
Y23="data/nyc_taxi/parquet/yellow/2023/yellow_tripdata_2023-*.parquet"
Y24="data/nyc_taxi/parquet/yellow/yellow_tripdata_2024-*.parquet"
Y25="data/nyc_taxi/parquet/yellow/2025/yellow_tripdata_2025-*.parquet"

ALL="
  SELECT 2022 as year, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('$Y22')
  UNION ALL
  SELECT 2023, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('$Y23')
  UNION ALL
  SELECT 2024, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('$Y24')
  UNION ALL
  SELECT 2025, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('$Y25')
"

BOLD='\033[1m'; CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'

echo -e "${BOLD}${CYAN}"
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║   NYC Yellow Cab 2022–2025  —  DuckDB 4-Year Benchmark     ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo "  DuckDB: $($DUCKDB --version)"
echo "  Started: $(date)"
echo ""

T_GLOBAL=$(date +%s%3N)

# ── 1. ROW COUNT ─────────────────────────────────────────────────────────────
echo -e "${BOLD}[1/5] Total trip volume — 4 years${NC}"
T0=$(date +%s%3N)
$DUCKDB -c "
SELECT year, COUNT(*) as total_trips,
       ROUND(COUNT(*)/1e6,2) as millions
FROM (
  SELECT 2022 as year FROM read_parquet('$Y22')
  UNION ALL
  SELECT 2023        FROM read_parquet('$Y23')
  UNION ALL
  SELECT 2024        FROM read_parquet('$Y24')
  UNION ALL
  SELECT 2025        FROM read_parquet('$Y25')
) GROUP BY year ORDER BY year;"
T1=$(date +%s%3N); echo -e "  ${GREEN}⏱  $((T1-T0)) ms${NC}"; echo ""

# ── 2. FARE / DISTANCE / TOTAL YoY ───────────────────────────────────────────
echo -e "${BOLD}[2/5] Fare, distance, tip, total — year-over-year${NC}"
T0=$(date +%s%3N)
$DUCKDB -c "
SELECT year,
  COUNT(*)                              as trips,
  ROUND(AVG(fare_amount),2)             as avg_fare,
  ROUND(AVG(trip_distance),3)           as avg_miles,
  ROUND(AVG(tip_amount),2)              as avg_tip,
  ROUND(AVG(total_amount),2)            as avg_total,
  ROUND(AVG(passenger_count::DOUBLE),2) as avg_pax
FROM ($ALL)
WHERE fare_amount   BETWEEN 0.01 AND 500
  AND trip_distance BETWEEN 0.01 AND 100
  AND passenger_count BETWEEN 1 AND 6
GROUP BY year ORDER BY year;"
T1=$(date +%s%3N); echo -e "  ${GREEN}⏱  $((T1-T0)) ms${NC}"; echo ""

# ── 3. MONTHLY TREND — 4-YEAR YoY PIVOT ──────────────────────────────────────
echo -e "${BOLD}[3/5] Monthly trip volume — 4-year YoY pivot${NC}"
T0=$(date +%s%3N)
$DUCKDB -c "
PIVOT (
  SELECT year, MONTH(tpep_pickup_datetime) as month, COUNT(*) as trips
  FROM ($ALL)
  GROUP BY year, month
) ON year USING SUM(trips) GROUP BY month ORDER BY month;"
T1=$(date +%s%3N); echo -e "  ${GREEN}⏱  $((T1-T0)) ms${NC}"; echo ""

# ── 4. PAYMENT TYPE SHIFT 2022→2025 ──────────────────────────────────────────
echo -e "${BOLD}[4/5] Payment type shift 2022→2025${NC}"
T0=$(date +%s%3N)
$DUCKDB -c "
SELECT year,
  ROUND(100.0*SUM(CASE WHEN payment_type=1 THEN 1 ELSE 0 END)/COUNT(*),1) as credit_pct,
  ROUND(100.0*SUM(CASE WHEN payment_type=2 THEN 1 ELSE 0 END)/COUNT(*),1) as cash_pct,
  ROUND(100.0*SUM(CASE WHEN payment_type=3 THEN 1 ELSE 0 END)/COUNT(*),1) as no_charge_pct,
  ROUND(100.0*SUM(CASE WHEN payment_type=4 THEN 1 ELSE 0 END)/COUNT(*),1) as dispute_pct
FROM ($ALL)
GROUP BY year ORDER BY year;"
T1=$(date +%s%3N); echo -e "  ${GREEN}⏱  $((T1-T0)) ms${NC}"; echo ""

# ── 5. CBD CONGESTION FEE (2025 only) ────────────────────────────────────────
echo -e "${BOLD}[5/5] CBD Congestion Fee — 2025${NC}"
T0=$(date +%s%3N)
$DUCKDB -c "
SELECT
  COUNT(*)                                                              as total_trips,
  SUM(CASE WHEN cbd_congestion_fee > 0 THEN 1 ELSE 0 END)             as cbd_charged,
  ROUND(100.0*SUM(CASE WHEN cbd_congestion_fee > 0 THEN 1 ELSE 0 END)/COUNT(*),1) as cbd_pct,
  ROUND(AVG(CASE WHEN cbd_congestion_fee > 0 THEN cbd_congestion_fee END),2)       as avg_cbd_fee,
  ROUND(SUM(cbd_congestion_fee)/1e6,2)                                 as total_cbd_millions
FROM read_parquet('$Y25')
WHERE fare_amount > 0;"
T1=$(date +%s%3N); echo -e "  ${GREEN}⏱  $((T1-T0)) ms${NC}"; echo ""

# ── TOTALS ────────────────────────────────────────────────────────────────────
T_END=$(date +%s%3N)
TOTAL_MS=$((T_END - T_GLOBAL))

TOTAL_ROWS=$($DUCKDB -csv -noheader -c "
SELECT COUNT(*) FROM (
  SELECT 1 FROM read_parquet('$Y22')
  UNION ALL SELECT 1 FROM read_parquet('$Y23')
  UNION ALL SELECT 1 FROM read_parquet('$Y24')
  UNION ALL SELECT 1 FROM read_parquet('$Y25')
);" | tr -d '\r\n ')

THROUGHPUT=$(awk -v r="$TOTAL_ROWS" -v t="$TOTAL_MS" 'BEGIN{printf "%d", r*1000/t}')

echo -e "${BOLD}${GREEN}"
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║   BENCHMARK COMPLETE                                        ║"
echo "  ║                                                              ║"
printf "  ║   Total rows scanned : %-38s║\n" "$TOTAL_ROWS"
printf "  ║   Wall time          : %-38s║\n" "${TOTAL_MS} ms"
printf "  ║   Throughput         : %-38s║\n" "${THROUGHPUT} rows/sec"
echo "  ║                                                              ║"
echo "  ║   Stack: DuckDB — single node, pure SQL, zero cluster      ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
