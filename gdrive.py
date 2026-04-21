"""Google Drive helpers: authenticate via service account, upload files."""

from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

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


def get_dataset_folder_id(service) -> str:
    battery_models_id = find_folder_id(service, "battery_models")
    return find_folder_id(service, "dataset", parent_id=battery_models_id)


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
