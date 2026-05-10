import yfinance as yf
import requests
import json
import csv
import os
import time
from datetime import date

TODAY = date.today().strftime("%d-%b-%Y")

# ── COMPREHENSIVE WATCHLIST ───────────────────────────
WATCHLIST = [
    # Big Tech
    "AAPL", "MSFT", "NVDA", "AMD", "INTC", "QCOM", "AVGO",
    "MU", "TXN", "AMAT", "LRCX", "KLAC", "ADI", "MRVL", "ARM",
    "STX", "DELL", "SNDK",

    # Internet / Social
    "META", "GOOGL", "AMZN", "NFLX", "SNAP", "PINS", "RDDT",
    "SPOT", "UBER", "LYFT", "ABNB",

    # AI / Cloud
    "PLTR", "AI", "PATH", "SNOW", "DDOG", "NET", "ZS",
    "CRWD", "S", "GTLB", "MDB", "CFLT",

    # EV / Auto
    "TSLA", "RIVN", "LCID", "NIO", "XPEV", "LI", "F", "GM",

    # Finance
    "JPM", "BAC", "GS", "MS", "WFC", "C", "BLK",
    "V", "MA", "PYPL", "SQ", "COIN", "HOOD",

    # Healthcare / Pharma
    "JNJ", "PFE", "MRNA", "ABBV", "BMY", "LLY",
    "UNH", "CVS", "AMGN", "GILD",

    # Energy
    "XOM", "CVX", "SLB", "OXY", "COP",

    # Consumer / Retail
    "WMT", "TGT", "COST", "HD", "MCD", "SBUX", "NKE", "DIS",

    # Chinese Stocks
    "BABA", "JD", "PDD", "BIDU", "TSM",

    # ETFs
    "SPY", "QQQ", "IWM", "DIA", "GLD", "TLT",

    # Random
    "ASTS", "NOW", "CRWV", "IREN", "OKLO", "ORCL", "SMCI", 
    "COIN", "ASML", "CRCL", "SEZL",
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
    params = {"formatted": "true", "scrIds": "most_actives", "count": 25}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        print(f"Most Active status: {r.status_code}")
        quotes = r.json()["finance"]["result"][0]["quotes"]
        result = []
        for q in quotes:
            result.append({
                "symbol":    q.get("symbol", ""),
                "shortName": q.get("shortName", ""),
                "price":     q.get("regularMarketPrice",         {}).get("raw", 0),
                "change":    round(q.get("regularMarketChange",  {}).get("raw", 0), 2),
                "changePct": round(q.get("regularMarketChangePercent", {}).get("raw", 0), 2),
                "volume":    q.get("regularMarketVolume",        {}).get("raw", 0),
                "avgVolume": q.get("averageDailyVolume3Month",   {}).get("raw", 0),
                "marketCap": q.get("marketCap",                  {}).get("raw", 0),
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
    params = {"formatted": "true", "scrIds": "day_gainers", "count": 15}
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
                "price":     q.get("regularMarketPrice",              {}).get("raw", 0),
                "changePct": round(q.get("regularMarketChangePercent",{}).get("raw", 0), 2),
                "volume":    q.get("regularMarketVolume",             {}).get("raw", 0),
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
    params = {"formatted": "true", "scrIds": "day_losers", "count": 15}
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
                "price":     q.get("regularMarketPrice",              {}).get("raw", 0),
                "changePct": round(q.get("regularMarketChangePercent",{}).get("raw", 0), 2),
                "volume":    q.get("regularMarketVolume",             {}).get("raw", 0),
            })
        print(f"✅ Losers: {len(result)} stocks")
        return result
    except Exception as e:
        print(f"❌ Losers error: {e}")
        return []

# ── FETCH OPTIONS OI ──────────────────────────────────
def fetch_options_oi():
    print("Fetching options OI...")
    key_stocks = ["SPY", "QQQ", "AAPL", "NVDA", "TSLA", "META", "MSFT", "AMD"]
    result = []
    for symbol in key_stocks:
        try:
            ticker = yf.Ticker(symbol)
            dates  = ticker.options
            if not dates:
                continue
            chain     = ticker.option_chain(dates[0])
            top_calls = chain.calls.nlargest(3, "openInterest")[
                ["strike", "lastPrice", "openInterest", "volume"]
            ].to_dict("records")
            top_puts  = chain.puts.nlargest(3, "openInterest")[
                ["strike", "lastPrice", "openInterest", "volume"]
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
            print(f"  {symbol} error: {e}")
    print(f"✅ Options OI: {len(result)} stocks")
    return result

# ── FETCH PRE/POST MARKET MOVERS ─────────────────────
def fetch_prepost_movers():
    print(f"Fetching pre/post market movers for {len(WATCHLIST)} stocks...")
    post_movers = []
    pre_movers  = []

    for symbol in WATCHLIST:
        try:
            info = yf.Ticker(symbol).info

            reg_price   = info.get("regularMarketPrice", 0)

            # Post market
            post_price  = info.get("postMarketPrice")
            post_change = info.get("postMarketChangePercent")

            # Pre market
            pre_price   = info.get("preMarketPrice")
            pre_change  = info.get("preMarketChangePercent")

            if post_price and post_change and abs(post_change) > 2:
                post_movers.append({
                    "symbol":     symbol,
                    "regPrice":   reg_price,
                    "postPrice":  round(post_price, 2),
                    "postChange": round(post_change, 2),
                    "direction":  "UP" if post_change > 0 else "DOWN"
                })

            if pre_price and pre_change and abs(pre_change) > 2:
                pre_movers.append({
                    "symbol":    symbol,
                    "regPrice":  reg_price,
                    "prePrice":  round(pre_price, 2),
                    "preChange": round(pre_change, 2),
                    "direction": "UP" if pre_change > 0 else "DOWN"
                })

            time.sleep(0.2)
        except Exception as e:
            print(f"  {symbol} error: {e}")

    post_movers = sorted(post_movers, key=lambda x: abs(x["postChange"]), reverse=True)
    pre_movers  = sorted(pre_movers,  key=lambda x: abs(x["preChange"]),  reverse=True)

    print(f"✅ Post Market Movers: {len(post_movers)}")
    print(f"✅ Pre Market Movers:  {len(pre_movers)}")
    return post_movers, pre_movers

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

# ── BUILD US FULL REPORT PROMPT ───────────────────────
def build_us_prompt(most_active, gainers, losers, options_oi):

    active_lines = []
    for i, s in enumerate(most_active[:10], 1):
        vol_m = round(s.get("volume", 0) / 1e6, 2)
        active_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"${sanitize(s.get('price'))} "
            f"Change={sanitize(s.get('changePct'))}pct "
            f"Vol={vol_m}M"
        )
    active_text = " | ".join(active_lines)

    gainer_lines = []
    for i, s in enumerate(gainers[:8], 1):
        gainer_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"${sanitize(s.get('price'))} "
            f"+{sanitize(s.get('changePct'))}pct"
        )
    gainer_text = " | ".join(gainer_lines)

    loser_lines = []
    for i, s in enumerate(losers[:8], 1):
        loser_lines.append(
            f"{i}. {sanitize(s.get('symbol'))} "
            f"${sanitize(s.get('price'))} "
            f"{sanitize(s.get('changePct'))}pct"
        )
    loser_text = " | ".join(loser_lines)

    options_lines = []
    for s in options_oi:
        calls = s.get("top_calls", [])
        puts  = s.get("top_puts",  [])
        top_call_oi     = calls[0].get("openInterest", 0) if calls else 0
        top_put_oi      = puts[0].get("openInterest",  0) if puts  else 0
        top_call_strike = calls[0].get("strike", 0)       if calls else 0
        top_put_strike  = puts[0].get("strike",  0)       if puts  else 0
        options_lines.append(
            f"{sanitize(s.get('symbol'))} "
            f"TopCall={top_call_strike} OI={top_call_oi} "
            f"TopPut={top_put_strike} OI={top_put_oi}"
        )
    options_text = " | ".join(options_lines)

    return (
        f"You are a US stock market analyst. Generate a daily market report for {TODAY} "
        f"with exactly 4 sections. Format with emojis and plain text only. "
        f"No markdown symbols. Keep entire response under 3500 characters. "
        f"SECTION 1 title: Most Active Stocks by Volume. "
        f"Show symbol, price, percent change, volume. "
        f"Data: {active_text}. "
        f"SECTION 2 title: Top Gainers. "
        f"Show symbol, price, percent gain. "
        f"Data: {gainer_text}. "
        f"SECTION 3 title: Top Losers. "
        f"Show symbol, price, percent loss. "
        f"Data: {loser_text}. "
        f"SECTION 4 title: Options Activity Analysis. "
        f"Analyse call vs put OI to determine market sentiment. "
        f"Data: {options_text}. "
        f"End with one line overall US market sentiment."
    )

# ── BUILD PRE/POST MARKET PROMPT ─────────────────────
def build_prepost_prompt(post_movers, pre_movers):

    post_lines = []
    for s in post_movers[:15]:
        arrow = "🟢" if s["direction"] == "UP" else "🔴"
        post_lines.append(
            f"{arrow} {sanitize(s['symbol'])} "
            f"RegPrice=${sanitize(s['regPrice'])} "
            f"PostPrice=${sanitize(s['postPrice'])} "
            f"Change={sanitize(s['postChange'])}pct"
        )
    post_text = " | ".join(post_lines) if post_lines else "No significant moves"

    pre_lines = []
    for s in pre_movers[:15]:
        arrow = "🟢" if s["direction"] == "UP" else "🔴"
        pre_lines.append(
            f"{arrow} {sanitize(s['symbol'])} "
            f"RegPrice=${sanitize(s['regPrice'])} "
            f"PrePrice=${sanitize(s['prePrice'])} "
            f"Change={sanitize(s['preChange'])}pct"
        )
    pre_text = " | ".join(pre_lines) if pre_lines else "No significant moves"

    return (
        f"You are a US stock market analyst. Generate a pre and post market report for {TODAY}. "
        f"Format with emojis and plain text only. No markdown. Keep under 3000 characters. "
        f"SECTION 1 title: Post Market Movers (After Hours). "
        f"List stocks with big moves after market close. "
        f"Explain likely reason for each big move. "
        f"Data: {post_text}. "
        f"SECTION 2 title: Pre Market Movers (Before Open). "
        f"List stocks moving significantly before market open. "
        f"Explain likely reason. "
        f"Data: {pre_text}. "
        f"End with what to watch at market open tomorrow."
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

    most_active              = fetch_most_active()
    gainers                  = fetch_gainers()
    losers                   = fetch_losers()
    options_oi               = fetch_options_oi()
    post_movers, pre_movers  = fetch_prepost_movers()

    print(f"\nRecords — Most Active: {len(most_active)} | "
          f"Gainers: {len(gainers)} | Losers: {len(losers)} | "
          f"Options: {len(options_oi)} | "
          f"Post Market: {len(post_movers)} | Pre Market: {len(pre_movers)}\n")

    folder = f"data/{TODAY}"
    os.makedirs(folder, exist_ok=True)

    save_csv(most_active, f"{folder}/us_most_active.csv")
    save_csv(gainers,     f"{folder}/us_gainers.csv")
    save_csv(losers,      f"{folder}/us_losers.csv")
    save_csv(post_movers, f"{folder}/us_post_market.csv")
    save_csv(pre_movers,  f"{folder}/us_pre_market.csv")

    # US full report
    us_prompt   = build_us_prompt(most_active, gainers, losers, options_oi)
    us_request  = build_gemini_request(us_prompt)
    with open(f"{folder}/gemini_us_request.txt", "w", encoding="utf-8") as f:
        json.dump(us_request, f, ensure_ascii=True)
    print(f"✅ Saved gemini_us_request.txt — length: {len(us_prompt)}")

    # Pre/Post market report
    prepost_prompt  = build_prepost_prompt(post_movers, pre_movers)
    prepost_request = build_gemini_request(prepost_prompt)
    with open(f"{folder}/gemini_us_prepost_request.txt", "w", encoding="utf-8") as f:
        json.dump(prepost_request, f, ensure_ascii=True)
    print(f"✅ Saved gemini_us_prepost_request.txt — length: {len(prepost_prompt)}")

    print(f"\n✅ Done — all US files saved to {folder}/")

if __name__ == "__main__":
    main()