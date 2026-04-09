"""
nyc_taxi_viz.py — NYC Yellow Cab 2022–2025 DuckDB Dashboard
Run: streamlit run notebooks/nyc_taxi_viz.py

Requirements:
    pip install streamlit duckdb pandas plotly

Data:
    bash scripts/download_nyc_taxi.sh --type=yellow
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

st.set_page_config(
    page_title="NYC Taxi — DuckDB Dashboard",
    page_icon="🚕",
    layout="wide"
)

st.title("🚕 NYC Yellow Cab 2022–2025")
st.caption("Powered by DuckDB · Single node · Zero cluster · 171M rows/sec")

# ── Data paths ────────────────────────────────────────────────────────────────
Y22 = "data/nyc_taxi/parquet/yellow/2022/yellow_tripdata_2022-*.parquet"
Y23 = "data/nyc_taxi/parquet/yellow/2023/yellow_tripdata_2023-*.parquet"
Y24 = "data/nyc_taxi/parquet/yellow/yellow_tripdata_2024-*.parquet"
Y25 = "data/nyc_taxi/parquet/yellow/2025/yellow_tripdata_2025-*.parquet"

ALL_SQL = f"""
  SELECT 2022 as year, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('{Y22}')
  UNION ALL
  SELECT 2023, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('{Y23}')
  UNION ALL
  SELECT 2024, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('{Y24}')
  UNION ALL
  SELECT 2025, fare_amount, trip_distance, tip_amount, total_amount,
         passenger_count, tpep_pickup_datetime, payment_type
  FROM read_parquet('{Y25}')
"""

@st.cache_data(show_spinner="Running DuckDB query...")
def query(sql: str) -> pd.DataFrame:
    return duckdb.query(sql).df()

# ── Row counts ────────────────────────────────────────────────────────────────
with st.spinner("Scanning 4 years of data..."):
    t0 = time.time()
    counts = query(f"""
        SELECT year, COUNT(*) as trips, ROUND(COUNT(*)/1e6,2) as millions
        FROM ({ALL_SQL})
        GROUP BY year ORDER BY year
    """)
    elapsed = time.time() - t0

total_rows = int(counts["trips"].sum())
throughput = int(total_rows / elapsed) if elapsed > 0 else 0

# ── KPI cards ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Trips (4yr)", f"{total_rows:,}")
c2.metric("Wall Time", f"{elapsed:.2f}s")
c3.metric("Throughput", f"{throughput:,} rows/sec")
c4.metric("Years Covered", "2022 – 2025")

st.divider()

# ── Trip volume bar chart ─────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Annual Trip Volume")
    fig = px.bar(counts, x="year", y="millions",
                 labels={"millions": "Trips (millions)", "year": "Year"},
                 color="millions", color_continuous_scale="Blues",
                 text="millions")
    fig.update_traces(texttemplate="%{text:.1f}M", textposition="outside")
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

# ── YoY fare trends ───────────────────────────────────────────────────────────
with col2:
    st.subheader("Average Fare Year-over-Year")
    fares = query(f"""
        SELECT year,
          ROUND(AVG(fare_amount),2)  as avg_fare,
          ROUND(AVG(tip_amount),2)   as avg_tip,
          ROUND(AVG(total_amount),2) as avg_total
        FROM ({ALL_SQL})
        WHERE fare_amount BETWEEN 0.01 AND 500
          AND trip_distance BETWEEN 0.01 AND 100
          AND passenger_count BETWEEN 1 AND 6
        GROUP BY year ORDER BY year
    """)
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=fares["year"], y=fares["avg_fare"],
                              name="Avg Fare", mode="lines+markers"))
    fig2.add_trace(go.Scatter(x=fares["year"], y=fares["avg_tip"],
                              name="Avg Tip", mode="lines+markers"))
    fig2.add_trace(go.Scatter(x=fares["year"], y=fares["avg_total"],
                              name="Avg Total", mode="lines+markers"))
    fig2.update_layout(yaxis_title="USD", xaxis_title="Year")
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Monthly pivot ─────────────────────────────────────────────────────────────
st.subheader("Monthly Trip Volume — 4-Year Comparison")
monthly = query(f"""
    SELECT year, MONTH(tpep_pickup_datetime) as month, COUNT(*) as trips
    FROM ({ALL_SQL})
    GROUP BY year, month ORDER BY year, month
""")
monthly["month"] = monthly["month"].astype(int)
fig3 = px.line(monthly, x="month", y="trips", color="year",
               labels={"trips": "Trips", "month": "Month", "year": "Year"},
               markers=True)
fig3.update_xaxes(tickvals=list(range(1,13)),
                  ticktext=["Jan","Feb","Mar","Apr","May","Jun",
                            "Jul","Aug","Sep","Oct","Nov","Dec"])
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ── Payment type shift ────────────────────────────────────────────────────────
st.subheader("Payment Type Shift 2022→2025")
pay = query(f"""
    SELECT year,
      ROUND(100.0*SUM(CASE WHEN payment_type=1 THEN 1 ELSE 0 END)/COUNT(*),1) as Credit,
      ROUND(100.0*SUM(CASE WHEN payment_type=2 THEN 1 ELSE 0 END)/COUNT(*),1) as Cash,
      ROUND(100.0*SUM(CASE WHEN payment_type=3 THEN 1 ELSE 0 END)/COUNT(*),1) as No_Charge,
      ROUND(100.0*SUM(CASE WHEN payment_type=4 THEN 1 ELSE 0 END)/COUNT(*),1) as Dispute
    FROM ({ALL_SQL})
    GROUP BY year ORDER BY year
""")
pay_melted = pay.melt(id_vars="year", var_name="Payment Type", value_name="Percent")
fig4 = px.bar(pay_melted, x="year", y="Percent", color="Payment Type",
              barmode="stack",
              labels={"year": "Year", "Percent": "% of Trips"})
st.plotly_chart(fig4, use_container_width=True)

st.divider()
st.caption("Data: NYC TLC Trip Record Data · Engine: DuckDB · Scott Baker — duckdatamaster.guru")
