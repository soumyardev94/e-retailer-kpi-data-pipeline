from __future__ import annotations

from pathlib import Path
from datetime import datetime
import pandas as pd


PIPELINE_ROOT = Path(__file__).resolve().parents[1]

IN_TEMPLATE = PIPELINE_ROOT / "data" /"raw"/ "manual_collection_template_synthetic.csv"
OUT_FACTS = PIPELINE_ROOT / "data" / "processed" / "retailer_year_facts.csv"


REQUIRED_COLS = [
    "company_id",
    "company_name",
    "year",
    "metric_name",
    "metric_value",
    "currency",
    "notes",
    "source_url",
    "source_type",
    "last_updated",
]


def _norm(s: str) -> str:
    return " ".join(str(s).strip().split())


def main() -> int:
    if not IN_TEMPLATE.exists():
        raise FileNotFoundError(f"Missing input: {IN_TEMPLATE}")

    df = pd.read_csv(IN_TEMPLATE, dtype=str)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"manual_collection_template.csv missing columns: {missing}. "
            f"Found: {list(df.columns)}"
        )

    df = df.copy()
    # normalize basic fields
    for c in ["company_id", "company_name", "metric_name", "currency", "source_type"]:
        df[c] = df[c].fillna("").map(_norm)

    df["company_id"] = df["company_id"].str.upper()
    df["metric_name"] = df["metric_name"].str.strip().str.lower()

    # year as int-like string
    df["year"] = df["year"].fillna("").map(_norm)

    # metric_value numeric (keep original too)
    df["metric_value_raw"] = df["metric_value"]
    df["metric_value"] = (
        df["metric_value"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    df["metric_value"] = pd.to_numeric(df["metric_value"], errors="coerce")

    # build wide fact table: retailer-year with columns per metric_name
    wide = df.pivot_table(
        index=["company_id", "company_name", "year"],
        columns="metric_name",
        values="metric_value",
        aggfunc="max",
    ).reset_index()

    # metadata: last_updated (max) per retailer-year
    meta = df.groupby(["company_id", "company_name", "year"], as_index=False).agg(
        last_updated=("last_updated", "max")
    )

    out = wide.merge(meta, on=["company_id", "company_name", "year"], how="left")

    # add pipeline timestamps
    today = datetime.today().strftime("%Y-%m-%d")
    out["created_at"] = today
    out["updated_at"] = today

    # stable sort
    out = out.sort_values(["company_id", "year"], ascending=[True, True])

    OUT_FACTS.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FACTS, index=False, encoding="utf-8")

    print(f"[OK] Wrote facts table: {OUT_FACTS}")
    print(f"[INFO] Rows: {len(out)} | Columns: {list(out.columns)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
