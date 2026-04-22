"""Pipeline: input CSV → risk scoring → output CSV → Google Drive upload."""

import sys
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd

import gdrive
import model as m

ROOT = Path(__file__).parent
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"
MODEL_LOCAL = ROOT / "battery_risk_model.joblib"

INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


def get_input_csv() -> Path:
    if len(sys.argv) >= 2:
        return Path(sys.argv[1])
    csvs = sorted(INPUT_DIR.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not csvs:
        print("No CSV found in input/. Export your DBeaver query there first.")
        sys.exit(1)
    print(f"Using: {csvs[0].name}")
    return csvs[0]


def main():
    print("Authenticating with Google Drive...")
    service = gdrive.get_service()
    battery_models_id = gdrive.get_battery_models_folder_id(service)
    dataset_id = gdrive.get_dataset_folder_id(service)

    # Download model if not already local
    if not MODEL_LOCAL.exists():
        print("Downloading model from Drive...")
        gdrive.download_file(service, "battery_risk_model.joblib", battery_models_id, MODEL_LOCAL)

    pipe = joblib.load(MODEL_LOCAL)
    print("Model loaded.")

    # Load input CSV
    csv_path = get_input_csv()
    df = pd.read_csv(csv_path, engine="python", on_bad_lines="skip")
    print(f"Loaded {len(df):,} rows from {csv_path.name}")

    # Score
    print("Scoring...")
    risk_df = m.score(df, pipe)
    print(f"Scored {len(risk_df):,} batteries | alerts: {risk_df['model_alert'].sum()}")

    # Save output
    today = datetime.now().strftime("%Y%m%d")
    output_path = OUTPUT_DIR / f"battery_risk_{today}.csv"
    risk_df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

    # Upload to Drive
    gdrive.upload_file(service, output_path, dataset_id)
    print("Done.")


if __name__ == "__main__":
    main()
