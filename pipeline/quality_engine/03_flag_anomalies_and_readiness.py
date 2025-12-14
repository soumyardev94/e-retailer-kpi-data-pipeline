from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

PIPELINE_ROOT = Path(__file__).resolve().parents[1]

IN_KPIS = PIPELINE_ROOT/ "data" / "processed" / "retailer_year_kpis.csv"
OUT_FLAGS = PIPELINE_ROOT/ "data" / "processed" / "retailer_year_kpis_flagged.csv"
OUT_REPORT = PIPELINE_ROOT/ "data" / "processed" / "quality_report_readiness.csv"


def main() -> int:
    if not IN_KPIS.exists():
        raise FileNotFoundError(f"Missing input: {IN_KPIS}")

    df = pd.read_csv(IN_KPIS, dtype=str)

    # parse numeric
    for col in ["revenue", "employees", "revenue_per_employee", "revenue_yoy_growth"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # --- Flags ---
    df["flag_missing_revenue"] = df["revenue"].isna()
    df["flag_missing_employees"] = df["employees"].isna()
    df["flag_negative_revenue"] = df["revenue"].fillna(0) < 0
    df["flag_negative_employees"] = df["employees"].fillna(0) < 0

    # revenue per employee plausibility (loose bounds for now; adjust later)
    # here revenue values are placeholders, so keep it tolerant
    rpe = df["revenue_per_employee"]
    df["flag_rpe_extreme"] = (rpe.notna()) & ((rpe < 0) | (rpe > 1000))

    # YoY growth anomaly flag (only meaningful once you have multiple years)
    yoy = df["revenue_yoy_growth"]
    df["flag_yoy_extreme"] = (yoy.notna()) & ((yoy < -0.5) | (yoy > 2.0))

    # --- Readiness Score (simple, transparent) ---
    # Start at 100, subtract points for issues
    score = np.full(len(df), 100, dtype=int)
    score -= df["flag_missing_revenue"].astype(int) * 40
    score -= df["flag_missing_employees"].astype(int) * 20
    score -= df["flag_negative_revenue"].astype(int) * 40
    score -= df["flag_negative_employees"].astype(int) * 40
    score -= df["flag_rpe_extreme"].astype(int) * 10
    score -= df["flag_yoy_extreme"].astype(int) * 10

    df["data_readiness_score"] = np.clip(score, 0, 100)

    # Save flagged dataset
    OUT_FLAGS.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FLAGS, index=False, encoding="utf-8")

    # Summary report
    report_rows = []
    report_rows.append({"metric": "rows", "value": len(df)})
    report_rows.append({"metric": "missing_revenue_rows", "value": int(df["flag_missing_revenue"].sum())})
    report_rows.append({"metric": "missing_employees_rows", "value": int(df["flag_missing_employees"].sum())})
    report_rows.append({"metric": "yoy_available_rows", "value": int(df["revenue_yoy_growth"].notna().sum())})
    report_rows.append({"metric": "avg_readiness_score", "value": float(df["data_readiness_score"].mean())})

    rep = pd.DataFrame(report_rows)
    rep.to_csv(OUT_REPORT, index=False, encoding="utf-8")

    print(f"[OK] Flagged KPI table: {OUT_FLAGS}")
    print(f"[OK] Readiness report:  {OUT_REPORT}")
    print(rep.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())