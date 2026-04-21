"""Google Drive helpers: authenticate, find folders/files, upload/download."""

import io
import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive"]
TOKEN_PATH = Path(__file__).parent / "token.json"
CREDENTIALS_PATH = Path(__file__).parent / "credentials.json"


def get_service():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json())
    return build("drive", "v3", credentials=creds)


def find_folder_id(service, folder_name: str, parent_id: str = None) -> str:
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if not files:
        raise FileNotFoundError(f"Folder '{folder_name}' not found on Google Drive.")
    return files[0]["id"]


def get_dataset_folder_id(service) -> str:
    """Resolve battery_models/dataset folder ID."""
    battery_models_id = find_folder_id(service, "battery_models")
    dataset_id = find_folder_id(service, "dataset", parent_id=battery_models_id)
    return dataset_id


def list_files(service, folder_id: str) -> list[dict]:
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    return results.get("files", [])


def download_file(service, file_id: str, dest_path: Path):
    request = service.files().get_media(fileId=file_id)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"  Downloaded → {dest_path}")


def upload_file(service, local_path: Path, folder_id: str) -> str:
    file_metadata = {"name": local_path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(local_path), resumable=True)
    # overwrite if file already exists in that folder
    query = f"name='{local_path.name}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=query, fields="files(id)").execute().get("files", [])
    if existing:
        file = service.files().update(
            fileId=existing[0]["id"], media_body=media
        ).execute()
        print(f"  Updated existing file on Drive: {local_path.name} (id={file['id']})")
    else:
        file = service.files().create(
            body=file_metadata, media_body=media, fields="id"
        ).execute()
        print(f"  Uploaded new file to Drive: {local_path.name} (id={file['id']})")
    return file["id"]
