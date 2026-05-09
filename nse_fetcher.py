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
    # ✅ Correct key: upper -> AllSec -> data
    return r.json().get("upper", {}).get("AllSec", {}).get("data", []) if r.status_code == 200 else []

def fetch_block_deals(session):
    warm_up(session, "https://www.nseindia.com/market-data/large-deals")
    headers = {**HEADERS, "Referer": "https://www.nseindia.com/market-data/large-deals"}
    r = session.get(
        "https://www.nseindia.com/api/snapshot-capital-market-largedeal",
        headers=headers, timeout=15
    )
    print(f"Block Deals status: {r.status_code}")
    # ✅ Correct key: BLOCK_DEALS_DATA
    return r.json().get("BLOCK_DEALS_DATA", []) if r.status_code == 200 else []

# ── SAVE CSV ─────────────────────────────────────────
def save_csv(data, filepath):
    if not data:
        print(f"⚠️ No data to save for {filepath}")
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Saved {filepath} — {len(data)} records")

# ── BUILD COMBINED JSON ───────────────────────────────
def build_combined_json(most_active, upper_band, block_deals):

    # Most active — top 10 by value
    active_lines = []
    for i, s in enumerate(most_active[:10], 1):
        val_lakhs = round(s.get("totalTradedValue", 0) / 1e5, 2)
        active_lines.append(
            f"{i}. {s.get('symbol')} LTP={s.get('lastPrice')} "
            f"Change={s.get('pChange')}pct "
            f"Value={round(s.get('totalTradedValue', 0) / 1e5, 2)}Lakhs"
        )
    active_text = " | ".join(active_lines)

    # Upper band — sorted by turnover (already in crores)
    sorted_upper = sorted(
        upper_band,
        key=lambda x: float(x.get("turnover", 0)),
        reverse=True
    )
    upper_lines = []
    for i, s in enumerate(sorted_upper[:15], 1):
        upper_lines.append(
            f"{i}. {s.get('symbol')} LTP={s.get('ltp')} "
            f"Change={str(s.get('pChange','0')).strip()}pct "
            f"Value={round(float(s.get('turnover', 0)), 2)}Cr"
        )
    upper_text = " | ".join(upper_lines)

    # Block deals — grouped by security name
    grouped = {}
    for deal in block_deals:
        name = deal.get("name", deal.get("symbol", "Unknown"))
        symbol = deal.get("symbol", "")
        if name not in grouped:
            grouped[name] = {"symbol": symbol, "buyers": [], "sellers": []}
        entry = f"Client={deal.get('clientName','N/A')} Qty={deal.get('qty','N/A')} Price={deal.get('watp','N/A')}"
        if deal.get("buySell", "").upper() == "BUY":
            grouped[name]["buyers"].append(entry)
        else:
            grouped[name]["sellers"].append(entry)

    block_lines = []
    for name, info in grouped.items():
        buyers  = "; ".join(info["buyers"])  if info["buyers"]  else "None"
        sellers = "; ".join(info["sellers"]) if info["sellers"] else "None"
        block_lines.append(
            f"Security={name}({info['symbol']}) "
            f"Buyers=[{buyers}] "
            f"Sellers=[{sellers}]"
        )
    block_text = " || ".join(block_lines)

    # Final prompt — single line, no newlines
    prompt = (
        f"You are an NSE market analyst. Generate a daily market report for {TODAY} "
        f"with exactly 3 sections formatted with emojis for Telegram. "
        f"SECTION 1 title: Stocks with Highest Traded Value (crores). "
        f"List upper band hitter stocks ranked by highest traded value in crores first. "
        f"Data: {upper_text}. "
        f"SECTION 2 title: Most Active Equities by Value. "
        f"Show symbol, value in lakhs, last price, percent change. "
        f"Data: {active_text}. "
        f"SECTION 3 title: Block Deal Transactions. "
        f"Group by security name. Show buyers, sellers, quantity, trade price. "
        f"Data: {block_text}. "
        f"Format cleanly with section headers, emojis, bullet points."
    )

    return {"prompt": prompt, "date": TODAY}

# ── MAIN ─────────────────────────────────────────────
def main():
    print(f"\n=== NSE Fetcher — {TODAY} ===\n")

    session = create_session()

    most_active = fetch_most_active(session)
    upper_band  = fetch_upper_band(session)
    block_deals = fetch_block_deals(session)

    print(f"\nRecords — Most Active: {len(most_active)} | Upper Band: {len(upper_band)} | Block Deals: {len(block_deals)}\n")

    folder = f"data/{TODAY}"
    os.makedirs(folder, exist_ok=True)

    save_csv(most_active, f"{folder}/most_active.csv")
    save_csv(upper_band,  f"{folder}/upper_band.csv")
    save_csv(block_deals, f"{folder}/block_deals.csv")

    combined = build_combined_json(most_active, upper_band, block_deals)
    with open(f"{folder}/combined.json", "w") as f:
        json.dump(combined, f)
    print(f"✅ Saved combined.json")

    # Preview prompt
    print(f"\n--- Prompt Preview (first 500 chars) ---")
    print(combined["prompt"][:500])
    print(f"\n✅ Done — all files saved to {folder}/")

if __name__ == "__main__":
    main()