"""Pipeline: input CSV → risk scoring → output CSV."""

import sys
from datetime import datetime
from pathlib import Path
import re

import joblib
import pandas as pd

import model as m

ROOT = Path(__file__).parent
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"
MODEL_LOCAL = ROOT / "battery_risk_model.joblib"

INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


def _filename_datetime(f: Path):
    match = re.match(r"(\d{12})", f.name)
    if match:
        return datetime.strptime(match.group(1), "%Y%m%d%H%M")
    match = re.match(r"(\d{8})", f.name)
    if match:
        return datetime.strptime(match.group(1), "%Y%m%d")
    return datetime.fromtimestamp(f.stat().st_mtime)


def get_input_csv() -> Path:
    if len(sys.argv) >= 2:
        return Path(sys.argv[1])
    csvs = [f for f in INPUT_DIR.glob("*.csv") if "train" not in f.name.lower()]
    csvs = sorted(csvs, key=_filename_datetime, reverse=True)
    if not csvs:
        print("No CSV found in input/. Export your DBeaver query there first.")
        sys.exit(1)
    print(f"Using: {csvs[0].name}")
    return csvs[0]


def main():
    if not MODEL_LOCAL.exists():
        print(f"Model not found at {MODEL_LOCAL}. Run train.py first.")
        sys.exit(1)

    pipe = joblib.load(MODEL_LOCAL)
    print("Model loaded.")

    csv_path = get_input_csv()
    df = pd.read_csv(csv_path, engine="python", on_bad_lines="skip")
    print(f"Loaded {len(df):,} rows from {csv_path.name}")

    print("Scoring...")
    risk_df = m.score(df, pipe)
    print(f"Scored {len(risk_df):,} batteries | alerts: {risk_df['model_alert'].sum()}")

    today = datetime.now().strftime("%Y%m%d")
    output_path = OUTPUT_DIR / f"battery_risk_{today}.csv"
    risk_df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
