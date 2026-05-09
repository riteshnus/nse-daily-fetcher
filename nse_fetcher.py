import requests
import json
import time
import csv
import os
from datetime import date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ── CONFIG ──────────────────────────────────────────
FOLDER_ID       = os.environ["GDRIVE_FOLDER_ID"]
CREDENTIALS_JSON = os.environ["GDRIVE_CREDENTIALS"]
TODAY           = date.today().strftime("%d-%b-%Y")

# ── NSE FETCH ────────────────────────────────────────
def get_nse_session():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }
    session = requests.Session()
    session.get(
        "https://www.nseindia.com/market-data/most-active-equities",
        headers={**headers, "sec-fetch-dest": "document"},
        timeout=15
    )
    time.sleep(2)
    return session, headers

def fetch_most_active(session, headers):
    headers["Referer"] = "https://www.nseindia.com/market-data/most-active-equities"
    r = session.get(
        "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value",
        headers=headers,
        timeout=15
    )
    return r.json().get("data", []) if r.status_code == 200 else []

# ── SAVE CSV ─────────────────────────────────────────
def save_csv(data, filename):
    if not data:
        print(f"No data for {filename}")
        return
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Saved {filename} — {len(data)} records")

# ── GOOGLE DRIVE ─────────────────────────────────────
def get_drive_service():
    creds_dict = json.loads(CREDENTIALS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def create_date_folder(service, parent_id, folder_name):
    query = f"name='{folder_name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id)").execute()
    files = results.get("files", [])
    if files:
        return files[0]["id"]
    meta = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    folder = service.files().create(body=meta, fields="id").execute()
    print(f"✅ Created Drive folder: {folder_name}")
    return folder["id"]

def upload_to_drive(service, filepath, folder_id):
    filename = os.path.basename(filepath)
    media = MediaFileUpload(filepath, mimetype="text/csv")
    meta = {"name": filename, "parents": [folder_id]}
    service.files().create(body=meta, media_body=media, fields="id").execute()
    print(f"✅ Uploaded to Drive: {filename}")

# ── MAIN ─────────────────────────────────────────────
def main():
    print(f"\n=== NSE Fetcher — {TODAY} ===\n")

    # Fetch
    session, headers = get_nse_session()
    securities = fetch_most_active(session, headers)

    # Save CSV locally
    filename = f"most_active_{TODAY}.csv"
    save_csv(securities, filename)

    # Upload to Drive
    drive = get_drive_service()
    folder_id = create_date_folder(drive, FOLDER_ID, TODAY)
    upload_to_drive(drive, filename, folder_id)

    print(f"\n✅ Done — {len(securities)} records uploaded to Drive/{TODAY}/")

if __name__ == "__main__":
    main()