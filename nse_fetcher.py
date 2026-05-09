import requests
import json
import time
import csv
import os
from datetime import date

TODAY = date.today().strftime("%d-%b-%Y")

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

def save_csv(data, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Saved {filepath} — {len(data)} records")

def main():
    print(f"\n=== NSE Fetcher — {TODAY} ===\n")

    session, headers = get_nse_session()
    securities = fetch_most_active(session, headers)

    if not securities:
        print("❌ No data fetched")
        return

    filepath = f"data/{TODAY}/most_active.csv"
    save_csv(securities, filepath)
    print(f"✅ Done — {len(securities)} records saved")

if __name__ == "__main__":
    main()