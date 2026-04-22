"""Pick the latest CSV from output/ and upload it to Google Drive."""

import sys
from pathlib import Path

import gdrive

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def main():
    if len(sys.argv) >= 2:
        csv_path = Path(sys.argv[1])
    else:
        csvs = sorted(OUTPUT_DIR.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not csvs:
            print("No CSV found in output/. Export your DBeaver query there first.")
            sys.exit(1)
        csv_path = csvs[0]
        print(f"Using: {csv_path.name}")

    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        sys.exit(1)

    print("Uploading to Google Drive...")
    service = gdrive.get_service()
    folder_id = gdrive.get_dataset_folder_id(service)
    gdrive.upload_file(service, csv_path, folder_id)
    print("Done.")


if __name__ == "__main__":
    main()
