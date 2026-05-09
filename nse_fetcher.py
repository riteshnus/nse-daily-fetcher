import requests
import json
import time
import csv
import os
from datetime import date

TODAY = date.today().strftime("%d-%b-%Y")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# ── SESSION ──────────────────────────────────────────
def create_session():
    session = requests.Session()
    session.get(
        "https://www.nseindia.com/market-data/most-active-equities",
        headers={**HEADERS, "sec-fetch-dest": "document"},
        timeout=15
    )
    time.sleep(2)
    return session

def warm_up(session, page_url):
    session.get(
        page_url,
        headers={**HEADERS, "sec-fetch-dest": "document"},
        timeout=15
    )
    time.sleep(1)

# ── FETCHERS ─────────────────────────────────────────
def fetch_most_active(session):
    headers = {**HEADERS, "Referer": "https://www.nseindia.com/market-data/most-active-equities"}
    r = session.get(
        "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value",
        headers=headers, timeout=15
    )
    print(f"Most Active status: {r.status_code}")
    return r.json().get("data", []) if r.status_code == 200 else []

def fetch_upper_band(session):
    warm_up(session, "https://www.nseindia.com/market-data/upper-band-hitters")
    headers = {**HEADERS, "Referer": "https://www.nseindia.com/market-data/upper-band-hitters"}
    r = session.get(
        "https://www.nseindia.com/api/live-analysis-price-band-hitter",
        headers=headers, timeout=15
    )
    print(f"Upper Band status: {r.status_code}")
    data = r.json() if r.status_code == 200 else {}
    return data.get("data", data.get("upperBand", []))

def fetch_block_deals(session):
    warm_up(session, "https://www.nseindia.com/market-data/large-deals")
    headers = {**HEADERS, "Referer": "https://www.nseindia.com/market-data/large-deals"}
    r = session.get(
        "https://www.nseindia.com/api/snapshot-capital-market-largedeal",
        headers=headers, timeout=15
    )
    print(f"Block Deals status: {r.status_code}")
    data = r.json() if r.status_code == 200 else {}
    return data.get("data", data.get("BLOCK", []))

# ── SAVE CSV ─────────────────────────────────────────
def save_csv(data, filepath):
    if not data:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Saved {filepath}")

# ── BUILD COMBINED JSON FOR MAKE.COM ─────────────────
def build_combined_json(most_active, upper_band, block_deals):

    # Most active — top 10 by value
    active_text = "MOST ACTIVE EQUITIES BY VALUE: "
    for s in most_active[:10]:
        val_cr = round(s.get("totalTradedValue", 0) / 1e7, 2)
        active_text += f"{s.get('symbol')} Price={s.get('lastPrice')} Change={s.get('pChange')}pct Value={val_cr}Cr; "

    # Upper band — sorted by traded value
    sorted_upper = sorted(
        upper_band,
        key=lambda x: x.get("totalTradedValue", x.get("tradedValue", 0)),
        reverse=True
    )
    upper_text = "UPPER BAND HITTERS ordered by traded value: "
    for s in sorted_upper[:15]:
        val = s.get("totalTradedValue", s.get("tradedValue", 0))
        val_cr = round(val / 1e7, 2) if val > 1000 else val
        upper_text += f"{s.get('symbol', s.get('securityName', 'N/A'))} Value={val_cr}Cr; "

    # Block deals — grouped by security name
    grouped = {}
    for deal in block_deals:
        name = deal.get("securityName", deal.get("symbol", "Unknown"))
        if name not in grouped:
            grouped[name] = []
        grouped[name].append(
            f"Client={deal.get('clientName', 'N/A')} "
            f"BuySell={deal.get('buySell', 'N/A')} "
            f"Qty={deal.get('quantityTraded', 'N/A')} "
            f"Price={deal.get('tradePrice', 'N/A')}"
        )
    block_text = "BLOCK DEALS grouped by security: "
    for name, deals in grouped.items():
        block_text += f"{name}: [{' | '.join(deals)}]; "

    # Combine into single prompt string — no newlines
    prompt = (
        f"NSE Market Data for {TODAY}. "
        f"{active_text} "
        f"{upper_text} "
        f"{block_text}"
        f"Answer these 3 questions with emojis for Telegram: "
        f"1. List upper band hitter stocks ordered by highest traded value in crores. "
        f"2. What are the most active equities by value. "
        f"3. What block deals happened grouped by security name."
    )

    return {"prompt": prompt, "date": TODAY}

# ── MAIN ─────────────────────────────────────────────
def main():
    print(f"\n=== NSE Fetcher — {TODAY} ===\n")

    session = create_session()

    most_active = fetch_most_active(session)
    upper_band  = fetch_upper_band(session)
    block_deals = fetch_block_deals(session)

    folder = f"data/{TODAY}"

    # Save individual CSVs
    save_csv(most_active, f"{folder}/most_active.csv")
    save_csv(upper_band,  f"{folder}/upper_band.csv")
    save_csv(block_deals, f"{folder}/block_deals.csv")

    # Save combined JSON for Make.com
    combined = build_combined_json(most_active, upper_band, block_deals)
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/combined.json", "w") as f:
        json.dump(combined, f)
    print(f"✅ Saved combined.json")
    print(f"\n✅ Done — all files saved to {folder}/")

if __name__ == "__main__":
    main()