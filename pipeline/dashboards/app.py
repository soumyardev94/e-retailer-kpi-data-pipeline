import pandas as pd
import streamlit as st
from pathlib import Path

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = PIPELINE_ROOT / "data" / "processed" / "retailer_year_kpis_flagged.csv"

st.set_page_config(page_title="Growth Watchlist Dashboard", layout="wide")

st.title("European Fashion E-Commerce Growth Watchlist")
st.caption("Prototype dashboard: KPI monitoring + data readiness / quality flags")

if not DATA_FILE.exists():
    st.error(f"Missing dataset: {DATA_FILE}")
    st.stop()

df = pd.read_csv(DATA_FILE)

# Sidebar filters
st.sidebar.header("Filters")
years = sorted(df["year"].dropna().unique().tolist())
selected_year = st.sidebar.selectbox("Year", years if years else [None])

filtered = df[df["year"] == selected_year] if selected_year is not None else df

# Top KPIs
col1, col2, col3 = st.columns(3)
col1.metric("Retailer-Year rows", len(filtered))
col2.metric("Avg readiness score", round(filtered["data_readiness_score"].mean(), 2) if "data_readiness_score" in filtered else "NA")
col3.metric("YoY growth available", int(filtered["revenue_yoy_growth"].notna().sum()) if "revenue_yoy_growth" in filtered else 0)

st.subheader("Retailer KPI Table (with Flags)")
show_cols = [
    "company_id", "company_name", "year",
    "revenue", "employees",
    "revenue_per_employee", "revenue_yoy_growth",
    "data_readiness_score",
    "flag_missing_revenue", "flag_missing_employees", "flag_yoy_extreme"
]
existing = [c for c in show_cols if c in filtered.columns]
st.dataframe(filtered[existing].sort_values("data_readiness_score", ascending=False), use_container_width=True)

st.subheader("Ranking: Revenue per Employee")
if "revenue_per_employee" in filtered.columns:
    rank_df = filtered[["company_name", "revenue_per_employee", "data_readiness_score"]].copy()
    rank_df = rank_df.sort_values("revenue_per_employee", ascending=False)
    st.dataframe(rank_df, use_container_width=True)
else:
    st.info("Revenue per employee not available yet.")

st.subheader("Data Quality Flags Overview")
flag_cols = [c for c in filtered.columns if c.startswith("flag_")]
if flag_cols:
    flag_summary = filtered[flag_cols].sum(numeric_only=True).sort_values(ascending=False)
    st.write(flag_summary)
else:
    st.info("No flags available yet.")
