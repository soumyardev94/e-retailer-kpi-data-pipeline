from __future__ import annotations

from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
IN_CURATED = PROJECT_ROOT / "data" / "curated" / "retailer_dimension.csv"
OUT_REPORT = PROJECT_ROOT / "data" / "curated" / "quality_report_retailer_dimension.csv"


def main() -> int:
    if not IN_CURATED.exists():
        raise FileNotFoundError(f"Missing input: {IN_CURATED}")

    df = pd.read_csv(IN_CURATED, dtype=str)

    checks = []

    # Completeness checks
    for col in ["company_id", "company_name", "country"]:
        missing = int(df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum())
        checks.append({"check": f"missing_{col}", "result": "PASS" if missing == 0 else "FAIL", "details": f"missing={missing}"})

    # Uniqueness
    dup = df["company_id"].duplicated().sum()
    checks.append({"check": "unique_company_id", "result": "PASS" if dup == 0 else "FAIL", "details": f"duplicates={int(dup)}"})

    report = pd.DataFrame(checks)
    report.to_csv(OUT_REPORT, index=False, encoding="utf-8")

    print(f"[OK] Quality report written: {OUT_REPORT}")
    print(report.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
