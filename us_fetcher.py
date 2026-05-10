import yfinance as yf
import requests
import json
import csv
import os
import time
from datetime import date, datetime

TODAY = date.today().strftime("%d-%b-%Y")

# ── WATCHLIST ─────────────────────────────────────────
WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "META", "GOOGL", "AMD", "NFLX", "SPY",
    "QQQ", "AAPL", "BABA", "UBER", "COIN"
]

# ── SANITIZE ─────────────────────────────────────────
def sanitize(text):
    return (str(text)
        .replace('"', "'")
        .replace('\\', '')
        .replace('\r', '')
        .replace('\n', ' ')
        .strip())

# ── FETCH MOST ACTIVE ─────────────────────────────────
def fetch_most_active():
    print("Fetching most active stocks...")
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"formatted": "true", "scrIds": "most_actives", "count": 15}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        print(f"Most Active status: {r.status_code}")
        quotes = r.json()["finance"]["result"][0]["quotes"]
        result = []
        for q in quotes:
            result.append({
                "symbol":        q.get("symbol", ""),
                "shortName":     q.get("shortName", ""),
                "price":         q.get("regularMarketPrice", {}).get("raw", 0),
                "change":        round(q.get("regularMarketChange", {}).get("raw", 0), 2),
                "changePct":     round(q.get("regularMarketChangePercent", {}).get("raw", 0), 2),
                "volume":        q.get("regularMarketVolume", {}).get("raw", 0),
                "marketCap":     q.get("marketCap", {}).get("raw", 0),
                "avgVolume":     q.get("averageDailyVolume3Month", {}).get("raw", 0),
            })
        print(f"✅ Most Active: {len(result)} stocks")
        return result
    except Exception as e:
        print(f"❌ Most Active error: {e}")
        return []

# ── FETCH GAINERS ─────────────────────────────────────
def fetch_gainers():
    print("Fetching top gainers...")
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"formatted": "true", "scrIds": "day_gainers", "count": 10}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        print(f"Gainers status: {r.status_code}")
        quotes = r.json()["finance"]["result"][0]["quotes"]
        result = []
        for q in quotes:
            result.append({
                "symbol":    q.get("symbol", ""),
                "shortName": q.get("shortName", ""),
                "price":     q.get("regularMarketPrice", {}).get("raw", 0),
                "changePct": round(q.get("regularMarketChangePercent", {}).get("raw", 0), 2),
                "volume":    q.get("regularMarketVolume", {}).get("raw", 0),
            })
        print(f"✅ Gainers: {len(result)} stocks")
        return result
    except Exception as e:
        print(f"❌ Gainers error: {e}")
        return []

# ── FETCH LOSERS ──────────────────────────────────────
def fetch_losers():
    print("Fetching top losers...")
    url = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    params = {"formatted": "true", "scrIds": "day_losers", "count": 10}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        print(f"Losers status: {r.status_code}")
        quotes = r.json()["finance"]["result"][0]["quotes"]
        result = []
        for q in quotes:
            result.append({
                "symbol":    q.get("symbol", ""),
                "shortName": q.get("shortName", ""),
                "price":     q.get("regularMarketPrice", {}).get("raw", 0),
                "changePct": round(q.get("regularMarketChangePercent", {}).get("raw", 0), 2),
                "volume":    q.get("regularMarketVolume", {}).get("raw", 0),
            })
        print(f"✅ Losers: {len(result)} stocks")
        return result
    except Exception as e:
        print(f"❌ Losers error: {e}")
        return []

# ── FETCH OPTIONS OI ──────────────────────────────────
def fetch_options_oi():
    print("Fetching options OI...")
    key_stocks = ["SPY", "QQQ", "AAPL", "NVDA", "TSLA"]
    result = []
    for symbol in key_stocks:
        try:
            ticker = yf.Ticker(symbol)
            dates  = ticker.options
            if not dates:
                continue
            chain = ticker.option_chain(dates[0])
            # Top calls by OI
            top_calls = chain.calls.nlargest(3, "openInterest")[
                ["strike", "lastPrice", "openInterest", "volume", "impliedVolatility"]
            ].to_dict("records")
            # Top puts by OI
            top_puts = chain.puts.nlargest(3, "openInterest")[
                ["strike", "lastPrice", "openInterest", "volume", "impliedVolatility"]
            ].to_dict("records")

            result.append({
                "symbol":    symbol,
                "expiry":    dates[0],
                "top_calls": top_calls,
                "top_puts":  top_puts,
            })
            print(f"  {symbol} options fetched")
            time.sleep(0.5)
        except Exception as e:
            print(f"  {symbol} options error: {e}")
    print(f"✅ Options OI: {len(result)} stocks")
    return result

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

# ── BUILD US PROMPT ───────────────────────────────────
def build_us_prompt(most_active, gainers, losers, options_oi):

    # Most active
    active_lines = []
    for i, s in enumerate(most_active[:10], 1):
        vol_m = round(s.get("volume", 0) / 1e6, 2)
        active_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"${sanitize(s.get('price'))} "
            f"Change={sanitize(s.get('changePct'))}pct "
            f"Volume={vol_m}M"
        )
    active_text = " | ".join(active_lines)

    # Gainers
    gainer_lines = []
    for i, s in enumerate(gainers[:5], 1):
        gainer_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"${sanitize(s.get('price'))} "
            f"+{sanitize(s.get('changePct'))}pct"
        )
    gainer_text = " | ".join(gainer_lines)

    # Losers
    loser_lines = []
    for i, s in enumerate(losers[:5], 1):
        loser_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"${sanitize(s.get('price'))} "
            f"{sanitize(s.get('changePct'))}pct"
        )
    loser_text = " | ".join(loser_lines)

    # Options OI
    options_lines = []
    for s in options_oi:
        calls = s.get("top_calls", [])
        puts  = s.get("top_puts",  [])
        top_call_oi = calls[0].get("openInterest", 0) if calls else 0
        top_put_oi  = puts[0].get("openInterest",  0) if puts  else 0
        top_call_strike = calls[0].get("strike", 0) if calls else 0
        top_put_strike  = puts[0].get("strike",  0) if puts  else 0
        options_lines.append(
            f"{sanitize(s.get('symbol'))} "
            f"TopCallStrike={top_call_strike} CallOI={top_call_oi} "
            f"TopPutStrike={top_put_strike} PutOI={top_put_oi}"
        )
    options_text = " | ".join(options_lines)

    return (
        f"You are a US stock market analyst. Generate a daily market report for {TODAY} "
        f"with exactly 4 sections formatted with emojis for Telegram. "
        f"SECTION 1 title: Most Active Stocks by Volume. "
        f"Show symbol, price, percent change, volume in millions. "
        f"Data: {active_text}. "
        f"SECTION 2 title: Top Gainers. "
        f"Show symbol, price, percent gain. "
        f"Data: {gainer_text}. "
        f"SECTION 3 title: Top Losers. "
        f"Show symbol, price, percent loss. "
        f"Data: {loser_text}. "
        f"SECTION 4 title: Options Activity. "
        f"Show top call and put OI for key stocks. Analyze if market is bullish or bearish. "
        f"Data: {options_text}. "
        f"End with one line overall US market sentiment. "
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
    print(f"\n=== US Market Fetcher — {TODAY} ===\n")

    most_active = fetch_most_active()
    gainers     = fetch_gainers()
    losers      = fetch_losers()
    options_oi  = fetch_options_oi()

    print(f"\nRecords — Most Active: {len(most_active)} | Gainers: {len(gainers)} | Losers: {len(losers)} | Options: {len(options_oi)}\n")

    folder = f"data/{TODAY}"
    os.makedirs(folder, exist_ok=True)

    save_csv(most_active, f"{folder}/us_most_active.csv")
    save_csv(gainers,     f"{folder}/us_gainers.csv")
    save_csv(losers,      f"{folder}/us_losers.csv")

    prompt  = build_us_prompt(most_active, gainers, losers, options_oi)
    request = build_gemini_request(prompt)

    with open(f"{folder}/gemini_us_request.txt", "w", encoding="utf-8") as f:
        json.dump(request, f, ensure_ascii=True)
    print(f"✅ Saved gemini_us_request.txt")
    print(f"US prompt length: {len(prompt)}")
    print(f"\n✅ Done — all US files saved to {folder}/")

if __name__ == "__main__":
    main()