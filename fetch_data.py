import json
import os
import time
import datetime
import urllib.request
import urllib.error

FINNHUB_KEY  = os.environ.get("FINNHUB_API_KEY", "")
POLYGON_KEY  = os.environ.get("POLYGON_API_KEY", "")
AIRTABLE_KEY = os.environ.get("AIRTABLE_API_KEY", "")

TICKERS = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "AMD", "MSFT", "PLTR", "META"]

POLYGON_BATCH_SIZE  = 4
POLYGON_BATCH_SLEEP = 62

def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "DRVN/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return None

def finnhub_quote(ticker):
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_KEY}"
    data = fetch_json(url)
    if not data or data.get("c", 0) == 0:
        return None
    return {
        "price":      round(data["c"], 2),
        "open":       round(data["o"], 2),
        "high":       round(data["h"], 2),
        "low":        round(data["l"], 2),
        "prev":       round(data["pc"], 2),
        "change":     round(data["c"] - data["pc"], 2),
        "change_pct": round(((data["c"] - data["pc"]) / data["pc"]) * 100, 2) if data["pc"] else 0
    }

def polygon_snapshot(ticker):
    url = (
        f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
        f"?apiKey={POLYGON_KEY}"
    )
    data = fetch_json(url)
    if not data or not data.get("ticker"):
        return None
    t          = data["ticker"]
    day        = t.get("day", {})
    prev       = t.get("prevDay", {})
    day_range  = day.get("h", 0) - day.get("l", 0)
    prev_close = prev.get("c", 0)
    atr_proxy  = round(max(day_range, abs(day.get("h", 0) - prev_close), abs(day.get("l", 0) - prev_close)), 4) if prev_close else round(day_range, 4)
    return {
        "volume":     day.get("v", 0),
        "vwap":       round(day.get("vw", 0), 2),
        "volume_avg": prev.get("v", 0),
        "atr_proxy":  atr_proxy,
        "atr_pct":    round((atr_proxy / prev_close) * 100, 4) if prev_close else None
    }

def calc_atr_from_snapshot(snap, quote):
    if not snap or not quote:
        return None
    return round((quote["high"] - quote["low"]) / 8, 4)

def bias_from_data(quote, atr_30m, atr_threshold=0.67):
    if not quote:
        return "NEUTRAL"
    change_pct = quote.get("change_pct", 0)
    compressed = atr_30m and atr_30m < atr_threshold
    if change_pct > 0.3:
        return "BULLISH"
    elif change_pct < -0.3:
        return "BEARISH"
    elif compressed:
        return "NEUTRAL - ATR COMPRESSED"
    return "NEUTRAL"

def main():
    print(f"DRVN Engine Data Fetch - {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    output = {
        "updated_at":    datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_at_et": datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p ET"),
        "tickers":       {}
    }
    quotes = {}
    for ticker in TICKERS:
        quotes[ticker] = finnhub_quote(ticker)
        time.sleep(0.3)
    snaps = {}
    for i in range(0, len(TICKERS), POLYGON_BATCH_SIZE):
        batch = TICKERS[i:i + POLYGON_BATCH_SIZE]
        for ticker in batch:
            snaps[ticker] = polygon_snapshot(ticker)
            time.sleep(1)
        if TICKERS[i + POLYGON_BATCH_SIZE:]:
            time.sleep(POLYGON_BATCH_SLEEP)
    for ticker in TICKERS:
        q       = quotes.get(ticker)
        s       = snaps.get(ticker)
        atr_30m = calc_atr_from_snapshot(s, q)
        bias    = bias_from_data(q, atr_30m)
        output["tickers"][ticker] = {
            "symbol":         ticker,
            "price":          q["price"]      if q else None,
            "change":         q["change"]     if q else None,
            "change_pct":     q["change_pct"] if q else None,
            "open":           q["open"]       if q else None,
            "high":           q["high"]       if q else None,
            "low":            q["low"]        if q else None,
            "prev_close":     q["prev"]       if q else None,
            "atr_30m":        atr_30m,
            "atr_compressed": bool(atr_30m and atr_30m < 0.67),
            "volume":         s["volume"]     if s else None,
            "vwap":           s["vwap"]       if s else None,
            "volume_avg":     s["volume_avg"] if s else None,
            "bias":           bias
        }
    with open("data.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"data.json written - {output['updated_at_et']}")

if __name__ == "__main__":
    main()
