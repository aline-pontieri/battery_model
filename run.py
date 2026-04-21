"""Upload a CSV exported from DBeaver to Google Drive battery_models/dataset."""

import sys
from pathlib import Path

import gdrive


def main():
    if len(sys.argv) < 2:
        # if no argument, pick the most recently modified CSV in the current folder
        csvs = sorted(Path(".").glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)
        if not csvs:
            print("No CSV file found. Usage: python run.py your_file.csv")
            sys.exit(1)
        csv_path = csvs[0]
        print(f"No file specified — using most recent CSV: {csv_path.name}")
    else:
        csv_path = Path(sys.argv[1])
        if not csv_path.exists():
            print(f"File not found: {csv_path}")
            sys.exit(1)

    print("Authenticating with Google Drive...")
    service = gdrive.get_service()
    folder_id = gdrive.get_dataset_folder_id(service)

    gdrive.upload_file(service, csv_path, folder_id)
    print("Done.")


if __name__ == "__main__":
    main()
