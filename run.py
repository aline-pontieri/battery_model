"""Run battery SQL on Athena, save CSV locally, upload to Google Drive."""

from datetime import datetime
from pathlib import Path

import awswrangler as wr
import boto3
from dotenv import load_dotenv
import os

import gdrive

load_dotenv()

ROOT = Path(__file__).parent
OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def run_query() -> Path:
    sql = (ROOT / "query.sql").read_text()

    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
        region_name=os.environ["AWS_REGION"],
    )

    print("Running Athena query...")
    df = wr.athena.read_sql_query(
        sql=sql,
        database="prod_bo2dl_bloqs_gluedb_prepared",
        s3_output=os.environ["ATHENA_S3_OUTPUT"],
        boto3_session=session,
    )
    print(f"  {len(df):,} rows returned.")

    filename = f"battery_soc_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_path = OUTPUT_DIR / filename
    df.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")
    return csv_path


def main():
    csv_path = run_query()

    print("Uploading to Google Drive...")
    service = gdrive.get_service()
    folder_id = gdrive.get_dataset_folder_id(service)
    gdrive.upload_file(service, csv_path, folder_id)
    print("Done.")


if __name__ == "__main__":
    main()
