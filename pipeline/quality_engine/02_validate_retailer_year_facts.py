from __future__ import annotations

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
IN_FACTS = PROJECT_ROOT / "data" / "processed" / "retailer_year_facts.csv"
OUT_REPORT = PROJECT_ROOT / "data" / "processed" / "quality_report_retailer_year_facts.csv"


def main() -> int:
    if not IN_FACTS.exists():
        raise FileNotFoundError(f"Missing input: {IN_FACTS}")

    df = pd.read_csv(IN_FACTS, dtype=str)

    checks = []

    # Basic structure checks
    checks.append({
        "check": "rows_gt_0",
        "result": "PASS" if len(df) > 0 else "FAIL",
        "details": f"rows={len(df)}"
    })

    # Required identifier fields
    for col in ["company_id", "company_name", "year"]:
        missing = int(df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum())
        checks.append({
            "check": f"missing_{col}",
            "result": "PASS" if missing == 0 else "FAIL",
            "details": f"missing={missing}"
        })

    # Uniqueness on (company_id, year)
    dup = df.duplicated(subset=["company_id", "year"]).sum()
    checks.append({
        "check": "unique_company_year",
        "result": "PASS" if dup == 0 else "FAIL",
        "details": f"duplicates={int(dup)}"
    })

    # Year validity check
    # (allow only 2000–2026 for this project; adjust later)
    year_num = pd.to_numeric(df["year"], errors="coerce")
    invalid_years = int(((year_num < 2000) | (year_num > 2026) | year_num.isna()).sum())
    checks.append({
        "check": "year_valid_range_2000_2026",
        "result": "PASS" if invalid_years == 0 else "FAIL",
        "details": f"invalid_years={invalid_years}"
    })

    # KPI-critical fields: revenue + employees (not all must exist early, but we measure coverage)
    for col in ["revenue", "employees"]:
        if col not in df.columns:
            checks.append({"check": f"column_exists_{col}", "result": "FAIL", "details": "missing_column"})
            continue

        # numeric parse
        vals = pd.to_numeric(df[col], errors="coerce")
        missing_vals = int(vals.isna().sum())
        negative_vals = int((vals < 0).sum())

        checks.append({
            "check": f"coverage_{col}",
            "result": "PASS" if missing_vals < len(df) else "FAIL",
            "details": f"missing={missing_vals} of {len(df)}"
        })
        checks.append({
            "check": f"no_negative_{col}",
            "result": "PASS" if negative_vals == 0 else "FAIL",
            "details": f"negative={negative_vals}"
        })

    report = pd.DataFrame(checks)
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(OUT_REPORT, index=False, encoding="utf-8")

    print(f"[OK] Quality report written: {OUT_REPORT}")
    print(report.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
