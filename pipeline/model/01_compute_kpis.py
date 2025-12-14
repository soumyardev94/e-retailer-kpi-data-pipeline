from __future__ import annotations

from pathlib import Path
import pandas as pd

PIPELINE_ROOT = Path(__file__).resolve().parents[1]
IN_FACTS = PIPELINE_ROOT / "data" / "processed" / "retailer_year_facts.csv"
OUT_KPIS = PIPELINE_ROOT / "data" / "processed" / "retailer_year_kpis.csv"


def main() -> int:
    if not IN_FACTS.exists():
        raise FileNotFoundError(f"Missing input: {IN_FACTS}")

    df = pd.read_csv(IN_FACTS, dtype=str)

    # Parse numeric columns
    for col in ["revenue", "employees"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    df = df.sort_values(["company_id", "year"])

    # Revenue per employee
    if "revenue" in df.columns and "employees" in df.columns:
        df["revenue_per_employee"] = df["revenue"] / df["employees"]

    # YoY revenue growth (requires multiple years per company)
    if "revenue" in df.columns:
        df["revenue_yoy_growth"] = df.groupby("company_id")["revenue"].pct_change()

    OUT_KPIS.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_KPIS, index=False, encoding="utf-8")

    print(f"[OK] KPIs written: {OUT_KPIS}")
    print(f"[INFO] Rows: {len(df)} | Columns: {list(df.columns)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())