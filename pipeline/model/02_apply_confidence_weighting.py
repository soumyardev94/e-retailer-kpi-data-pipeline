from pathlib import Path
import pandas as pd

# pipeline/ directory (because data/ lives here)
PIPELINE_ROOT = Path(__file__).resolve().parents[1]

IN_FILE = PIPELINE_ROOT / "data" / "processed" / "retailer_year_kpis_flagged.csv"
OUT_FILE = PIPELINE_ROOT / "data" / "processed" / "retailer_year_kpis_final.csv"


def main():
    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input file: {IN_FILE}")

    df = pd.read_csv(IN_FILE)

    if "revenue_per_employee" in df.columns and "data_readiness_score" in df.columns:
        df["confidence_weighted_rpe"] = (
            df["revenue_per_employee"] * (df["data_readiness_score"] / 100)
        )

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_FILE, index=False)
    print(f"[OK] Final KPI table written: {OUT_FILE}")


if __name__ == "__main__":
    main()

