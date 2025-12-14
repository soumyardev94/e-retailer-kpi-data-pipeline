from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd


PIPELINE_ROOT = Path(__file__).resolve().parents[1]

RAW_COMPANY_LIST = PIPELINE_ROOT / "data" /"raw"/ "company_list.csv"
OUT_PROCESSED = PIPELINE_ROOT / "data" / "processed" / "retailer_static.csv"
OUT_LOG = PIPELINE_ROOT / "data" / "processed" / "logs" / "01_build_static_table.log"


REQUIRED_COLS = ["company_id", "company_name", "country"]


def _ensure_dirs() -> None:
    (PIPELINE_ROOT / "data" / "processed").mkdir(parents=True, exist_ok=True)
    (PIPELINE_ROOT / "data" / "processed" / "logs").mkdir(parents=True, exist_ok=True)


def _normalize_whitespace(s: str) -> str:
    return " ".join(str(s).strip().split())


def _clean_country(country: str) -> str:
    c = _normalize_whitespace(country)
    # normalize a few common variations
    mapping = {
        "UK": "United Kingdom",
        "U.K.": "United Kingdom",
        "United Kingdom/Global": "United Kingdom",
        "Netherlands/Germany": "Netherlands",
        "Germany/Netherlands": "Germany",
    }
    return mapping.get(c, c)


def build_static_table() -> pd.DataFrame:
    if not RAW_COMPANY_LIST.exists():
        raise FileNotFoundError(f"Missing file: {RAW_COMPANY_LIST}")

    df = pd.read_csv(RAW_COMPANY_LIST, dtype=str)

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"company_list.csv is missing columns: {missing}. Found: {list(df.columns)}")

    # basic cleaning
    df = df.copy()
    for col in ["company_id", "company_name", "country"]:
        df[col] = df[col].astype(str).map(_normalize_whitespace)

    df["company_id"] = df["company_id"].str.upper()
    df["country"] = df["country"].map(_clean_country)

    # optional columns
    if "notes" not in df.columns:
        df["notes"] = ""

    # add system columns
    today = datetime.today().strftime("%Y-%m-%d")
    df["created_at"] = today
    df["updated_at"] = today
    df["status"] = "active"  # can later set "optional"/"inactive"/etc.

    # checks
    issues = []

    if df["company_id"].isna().any() or (df["company_id"] == "").any():
        issues.append("Found empty company_id values.")
    if df["company_name"].isna().any() or (df["company_name"] == "").any():
        issues.append("Found empty company_name values.")
    if df["country"].isna().any() or (df["country"] == "").any():
        issues.append("Found empty country values.")

    dup_ids = df["company_id"][df["company_id"].duplicated()].unique().tolist()
    if dup_ids:
        issues.append(f"Duplicate company_id values: {dup_ids}")

    # enforce stable column order
    ordered_cols = [
        "company_id",
        "company_name",
        "country",
        "notes",
        "status",
        "created_at",
        "updated_at",
    ]
    df = df[ordered_cols]

    return df, issues


def write_log(df: pd.DataFrame, issues: list[str]) -> None:
    lines = []
    lines.append(f"Run: 01_build_static_table.py")
    lines.append(f"Input: {RAW_COMPANY_LIST}")
    lines.append(f"Output: {OUT_PROCESSED}")
    lines.append(f"Rows: {len(df)}")
    lines.append(f"Columns: {list(df.columns)}")

    if issues:
        lines.append("Issues:")
        for i in issues:
            lines.append(f"- {i}")
        # treat issues as non-fatal for now, but surface them
    else:
        lines.append("Issues: none")

    OUT_LOG.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    _ensure_dirs()

    try:
        df, issues = build_static_table()
        df.to_csv(OUT_PROCESSED, index=False, encoding="utf-8")
        write_log(df, issues)
        print(f"[OK] Wrote: {OUT_PROCESSED}")
        print(f"[OK] Log:   {OUT_LOG}")
        if issues:
            print("[WARN] Issues found:")
            for i in issues:
                print(" -", i)
        return 0
    except Exception as e:
        print("[ERROR]", repr(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
