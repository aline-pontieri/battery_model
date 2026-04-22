"""Google Drive helpers: authenticate via service account, upload/download files."""

import io
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive"]
CREDENTIALS_PATH = Path(__file__).parent / "credentials.json"


def get_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def find_folder_id(service, folder_name: str, parent_id: str = None) -> str:
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if not files:
        raise FileNotFoundError(f"Folder '{folder_name}' not found. Make sure you shared it with the service account.")
    return files[0]["id"]


def get_battery_models_folder_id(service) -> str:
    return find_folder_id(service, "battery_models")


def get_dataset_folder_id(service) -> str:
    battery_models_id = get_battery_models_folder_id(service)
    return find_folder_id(service, "dataset", parent_id=battery_models_id)


def download_file(service, filename: str, folder_id: str, dest_path: Path):
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if not files:
        raise FileNotFoundError(f"'{filename}' not found in Drive folder.")
    file_id = files[0]["id"]
    request = service.files().get_media(fileId=file_id)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"Downloaded: {filename} → {dest_path}")


def upload_file(service, local_path: Path, folder_id: str) -> str:
    media = MediaFileUpload(str(local_path), resumable=True)
    query = f"name='{local_path.name}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=query, fields="files(id)").execute().get("files", [])
    if existing:
        file = service.files().update(fileId=existing[0]["id"], media_body=media).execute()
        print(f"Updated on Drive: {local_path.name}")
    else:
        file = service.files().create(
            body={"name": local_path.name, "parents": [folder_id]},
            media_body=media, fields="id"
        ).execute()
        print(f"Uploaded to Drive: {local_path.name}")
    return file["id"]
