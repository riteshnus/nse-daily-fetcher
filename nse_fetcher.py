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

# ── SANITIZE ─────────────────────────────────────────
def sanitize(text):
    return (str(text)
        .replace('"', "'")
        .replace('\\', '')
        .replace('\r', '')
        .replace('\n', ' ')
        .strip())

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

# ── EQUITY FETCHERS ───────────────────────────────────
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
    return r.json().get("upper", {}).get("AllSec", {}).get("data", []) if r.status_code == 200 else []

def fetch_block_deals(session):
    warm_up(session, "https://www.nseindia.com/market-data/large-deals")
    headers = {**HEADERS, "Referer": "https://www.nseindia.com/market-data/large-deals"}
    r = session.get(
        "https://www.nseindia.com/api/snapshot-capital-market-largedeal",
        headers=headers, timeout=15
    )
    print(f"Block Deals status: {r.status_code}")
    return r.json().get("BLOCK_DEALS_DATA", []) if r.status_code == 200 else []

# ── DERIVATIVES FETCHERS ──────────────────────────────
def fetch_oi_underlyings(session):
    warm_up(session, "https://www.nseindia.com/market-data/change-in-open-interest")
    headers = {**HEADERS, "Referer": "https://www.nseindia.com/market-data/change-in-open-interest"}
    r = session.get(
        "https://www.nseindia.com/api/live-analysis-oi-spurts-underlyings",
        headers=headers, timeout=15
    )
    print(f"OI Underlyings status: {r.status_code}")
    return r.json().get("data", []) if r.status_code == 200 else []

def fetch_oi_contracts(session):
    headers = {**HEADERS, "Referer": "https://www.nseindia.com/market-data/change-in-open-interest"}
    r = session.get(
        "https://www.nseindia.com/api/live-analysis-oi-spurts-contracts",
        headers=headers, timeout=15
    )
    print(f"OI Contracts status: {r.status_code}")
    if r.status_code != 200:
        return {}, {}, {}, {}
    data = r.json().get("data", [])
    slide_slide = data[0].get("Slide-in-OI-Slide", []) if len(data) > 0 else []
    slide_rise  = data[1].get("Slide-in-OI-Rise",  []) if len(data) > 1 else []
    rise_rise   = data[2].get("Rise-in-OI-Rise",   []) if len(data) > 2 else []
    rise_slide  = data[3].get("Rise-in-OI-Slide",  []) if len(data) > 3 else []
    return slide_slide, slide_rise, rise_rise, rise_slide

# ── SAVE CSV ─────────────────────────────────────────
def save_csv(data, filepath):
    if not data:
        print(f"⚠️  No data for {filepath}")
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"✅ Saved {filepath} — {len(data)} records")

# ── BUILD EQUITY PROMPT ───────────────────────────────
def build_equity_prompt(most_active, upper_band, block_deals):

    active_lines = []
    for i, s in enumerate(most_active[:10], 1):
        val_lakhs = round(s.get("totalTradedValue", 0) / 1e5, 2)
        active_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"LTP={sanitize(s.get('lastPrice'))} "
            f"Change={sanitize(s.get('pChange'))}pct "
            f"Value={val_lakhs}Lakhs"
        )
    active_text = " | ".join(active_lines)

    sorted_upper = sorted(
        upper_band,
        key=lambda x: float(x.get("turnover", 0)),
        reverse=True
    )
    upper_lines = []
    for i, s in enumerate(sorted_upper[:15], 1):
        upper_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"LTP={sanitize(s.get('ltp'))} "
            f"Change={sanitize(str(s.get('pChange', 0)).strip())}pct "
            f"Value={round(float(s.get('turnover', 0)), 2)}Cr"
        )
    upper_text = " | ".join(upper_lines)

    grouped = {}
    for deal in block_deals:
        name   = sanitize(deal.get("name",   deal.get("symbol", "Unknown")))
        symbol = sanitize(deal.get("symbol", ""))
        if name not in grouped:
            grouped[name] = {"symbol": symbol, "buyers": [], "sellers": []}
        entry = (
            f"Client={sanitize(deal.get('clientName', 'NA'))} "
            f"Qty={sanitize(deal.get('qty', 'NA'))} "
            f"Price={sanitize(deal.get('watp', 'NA'))}"
        )
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
            f"Buyers=[{buyers}] Sellers=[{sellers}]"
        )
    block_text = " || ".join(block_lines)

    return (
        f"You are an NSE market analyst. Generate a daily equity market report for {TODAY} "
        f"with exactly 3 sections formatted with emojis for Telegram. "
        f"SECTION 1 title: Stocks with Highest Traded Value (crores). "
        f"List upper band hitter stocks ranked by highest traded value first. "
        f"Data: {upper_text}. "
        f"SECTION 2 title: Most Active Equities by Value. "
        f"Show symbol, value in lakhs, last price, percent change. "
        f"Data: {active_text}. "
        f"SECTION 3 title: Block Deal Transactions. "
        f"Group by security name, show buyers, sellers, quantity, price. "
        f"Data: {block_text}. "
        f"Format with section headers, emojis and bullet points."
    )

# ── BUILD DERIVATIVES PROMPT ──────────────────────────
def build_derivatives_prompt(oi_underlyings, slide_slide, slide_rise, rise_rise, rise_slide):

    # OI Underlyings — top 10 by changeInOI
    sorted_underlying = sorted(
        oi_underlyings,
        key=lambda x: abs(x.get("changeInOI", 0)),
        reverse=True
    )
    underlying_lines = []
    for i, s in enumerate(sorted_underlying[:10], 1):
        underlying_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"LatestOI={s.get('latestOI')} "
            f"PrevOI={s.get('prevOI')} "
            f"ChangeInOI={s.get('changeInOI')} "
            f"ChangePct={s.get('avgInOI')}pct "
            f"UnderlyingValue={s.get('underlyingValue')}"
        )
    underlying_text = " | ".join(underlying_lines)

    # Filter stock futures only from contracts (exclude index options for clarity)
    def filter_stocks(contracts):
        return [c for c in contracts if c.get("instrumentType") == "FUTSTK"]

    def format_contracts(contracts, limit=10):
        lines = []
        for i, c in enumerate(contracts[:limit], 1):
            lines.append(
                f"{i}. {sanitize(c.get('symbol'))} "
                f"LTP={sanitize(c.get('ltp'))} "
                f"Change={sanitize(c.get('pChange'))}pct "
                f"ChangeInOI={c.get('changeInOI')} "
                f"ChangePctOI={c.get('pChangeInOI')}pct"
            )
        return " | ".join(lines)

    long_buildup_text   = format_contracts(filter_stocks(rise_rise))
    short_buildup_text  = format_contracts(filter_stocks(rise_slide))
    short_cover_text    = format_contracts(filter_stocks(slide_rise))
    long_unwind_text    = format_contracts(filter_stocks(slide_slide))

    return (
        f"You are an NSE derivatives analyst. Generate a daily derivatives report for {TODAY} "
        f"with exactly 4 sections formatted with emojis for Telegram. "
        f"SECTION 1 title: Top OI Change by Underlying. "
        f"Show which stocks have highest open interest change. "
        f"Data: {underlying_text}. "
        f"SECTION 2 title: Long Buildup Stocks (Bullish). "
        f"OI rising + Price rising = new buyers entering. These stocks may go UP. "
        f"Data: {long_buildup_text}. "
        f"SECTION 3 title: Short Buildup Stocks (Bearish). "
        f"OI rising + Price falling = new sellers entering. These stocks may go DOWN. "
        f"Data: {short_buildup_text}. "
        f"SECTION 4 title: Short Covering Stocks (Watch). "
        f"OI falling + Price rising = shorts covering positions. "
        f"Data: {short_cover_text}. "
        f"End with one line overall market sentiment based on the data. "
        f"Format with section headers, emojis and bullet points."
    )

# ── BUILD GEMINI REQUEST ──────────────────────────────
def build_gemini_request(prompt):
    return {
        "contents": [
            {"parts": [{"text": prompt}]}
        ]
    }

# ── MAIN ─────────────────────────────────────────────
def main():
    print(f"\n=== NSE Fetcher — {TODAY} ===\n")

    session = create_session()

    # Equity data
    most_active = fetch_most_active(session)
    upper_band  = fetch_upper_band(session)
    block_deals = fetch_block_deals(session)

    # Derivatives data
    oi_underlyings                              = fetch_oi_underlyings(session)
    slide_slide, slide_rise, rise_rise, rise_slide = fetch_oi_contracts(session)

    print(f"\nEquity    — Most Active: {len(most_active)} | Upper Band: {len(upper_band)} | Block Deals: {len(block_deals)}")
    print(f"Derivatives — OI Underlyings: {len(oi_underlyings)} | Long Buildup: {len(rise_rise)} | Short Buildup: {len(rise_slide)} | Short Cover: {len(slide_rise)}\n")

    folder = f"data/{TODAY}"
    os.makedirs(folder, exist_ok=True)

    # Save CSVs
    save_csv(most_active,    f"{folder}/most_active.csv")
    save_csv(upper_band,     f"{folder}/upper_band.csv")
    save_csv(block_deals,    f"{folder}/block_deals.csv")
    save_csv(oi_underlyings, f"{folder}/oi_underlyings.csv")
    save_csv(rise_rise,      f"{folder}/long_buildup.csv")
    save_csv(rise_slide,     f"{folder}/short_buildup.csv")
    save_csv(slide_rise,     f"{folder}/short_covering.csv")
    save_csv(slide_slide,    f"{folder}/long_unwinding.csv")

    # Build and save equity Gemini request
    equity_prompt = build_equity_prompt(most_active, upper_band, block_deals)
    equity_request = build_gemini_request(equity_prompt)
    with open(f"{folder}/gemini_equity_request.txt", "w", encoding="utf-8") as f:
        json.dump(equity_request, f, ensure_ascii=True)
    print(f"✅ Saved gemini_equity_request.txt")

    # Build and save derivatives Gemini request
    deriv_prompt = build_derivatives_prompt(oi_underlyings, slide_slide, slide_rise, rise_rise, rise_slide)
    deriv_request = build_gemini_request(deriv_prompt)
    with open(f"{folder}/gemini_derivatives_request.txt", "w", encoding="utf-8") as f:
        json.dump(deriv_request, f, ensure_ascii=True)
    print(f"✅ Saved gemini_derivatives_request.txt")

    print(f"\n✅ Done — all files saved to {folder}/")
    print(f"Equity prompt length:      {len(equity_prompt)}")
    print(f"Derivatives prompt length: {len(deriv_prompt)}")

if __name__ == "__main__":
    main()