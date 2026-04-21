"""Run battery SoC query on AWS Athena and sync results with Google Drive."""

import os
from datetime import datetime
from pathlib import Path

import awswrangler as wr
import boto3
import pandas as pd
from dotenv import load_dotenv

import gdrive

load_dotenv()

ROOT = Path(__file__).parent
QUERY_FILE = ROOT / "query.sql"
LOCAL_DATA_DIR = ROOT / "data"
LOCAL_DATA_DIR.mkdir(exist_ok=True)

# ── AWS / Athena config ───────────────────────────────────────────────────────
AWS_REGION = os.environ["AWS_REGION"]
ATHENA_S3_OUTPUT = os.environ["ATHENA_S3_OUTPUT"]  # e.g. s3://my-bucket/athena-results/

boto_session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
    region_name=AWS_REGION,
)


def run_athena_query() -> pd.DataFrame:
    sql = QUERY_FILE.read_text()
    print("Running Athena query...")
    df = wr.athena.read_sql_query(
        sql=sql,
        database="prod_bo2dl_bloqs_gluedb_prepared",
        s3_output=ATHENA_S3_OUTPUT,
        boto3_session=boto_session,
    )
    print(f"  Query returned {len(df):,} rows.")
    return df


def download_existing_datasets(service, folder_id: str):
    """Download all files from the Drive dataset folder to LOCAL_DATA_DIR."""
    files = gdrive.list_files(service, folder_id)
    if not files:
        print("  No existing files found in Drive dataset folder.")
        return
    print(f"  Found {len(files)} file(s) in Drive dataset folder. Downloading...")
    for f in files:
        dest = LOCAL_DATA_DIR / f["name"]
        if not dest.exists():
            gdrive.download_file(service, f["id"], dest)
        else:
            print(f"  Skipped (already local): {f['name']}")


def save_and_upload(df: pd.DataFrame, service, folder_id: str):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"battery_soc_{timestamp}.csv"
    local_path = LOCAL_DATA_DIR / filename
    df.to_csv(local_path, index=False)
    print(f"Saved locally: {local_path}")
    gdrive.upload_file(service, local_path, folder_id)


def main():
    # 1. Google Drive setup
    print("Authenticating with Google Drive...")
    service = gdrive.get_service()
    folder_id = gdrive.get_dataset_folder_id(service)
    print(f"  Drive folder id: {folder_id}")

    # 2. Download existing dataset files locally
    download_existing_datasets(service, folder_id)

    # 3. Run Athena query
    df = run_athena_query()

    # 4. Save result and upload to Drive
    save_and_upload(df, service, folder_id)
    print("Done.")


if __name__ == "__main__":
    main()
