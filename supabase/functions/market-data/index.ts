import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

// DRVN market-data Edge Function
// Proxies Airtable + Finnhub — staggered 200ms between Finnhub calls
// Secrets: AIRTABLE_KEY, AIRTABLE_BASE, FINNHUB_KEY

const TICKERS = ["SPY","QQQ","NVDA","TSLA","AAPL","META","MSFT","PLTR","AMD","IWM"];
const STAGGER_MS = 200;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json",
};

serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS });
  }

  const AIRTABLE_KEY  = Deno.env.get("AIRTABLE_KEY");
  const AIRTABLE_BASE = Deno.env.get("AIRTABLE_BASE");
  const FINNHUB_KEY   = Deno.env.get("FINNHUB_KEY");

  if (!AIRTABLE_KEY || !AIRTABLE_BASE || !FINNHUB_KEY) {
    return new Response(
      JSON.stringify({ error: "Missing secrets" }),
      { status: 500, headers: CORS }
    );
  }

  try {
    const [airtableData, finnhubData] = await Promise.all([
      fetchAirtable(AIRTABLE_BASE, AIRTABLE_KEY),
      fetchFinnhubStaggered(TICKERS, FINNHUB_KEY),
    ]);

    const merged: Record<string, any> = {};
    TICKERS.forEach(sym => {
      const fh = finnhubData[sym] ?? {};
      merged[sym] = {
        price:      fh.c  ?? null,
        prev_close: fh.pc ?? null,
        change:     fh.d  ?? null,
        change_pct: fh.dp ?? null,
        open:       fh.o  ?? null,
        high:       fh.h  ?? null,
        low:        fh.l  ?? null,
        fields:     airtableData[sym] ?? {},
      };
    });

    return new Response(
      JSON.stringify({ updated_at: new Date().toISOString(), tickers: merged }),
      { status: 200, headers: CORS }
    );
  } catch (err) {
    return new Response(
      JSON.stringify({ error: "Fetch failed", detail: String(err) }),
      { status: 502, headers: CORS }
    );
  }
});

async function fetchAirtable(base: string, key: string): Promise<Record<string, any>> {
  const url = `https://api.airtable.com/v0/${base}/Investment%20Matrix?maxRecords=20`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
  if (!resp.ok) throw new Error(`Airtable ${resp.status}`);
  const json = await resp.json();
  const result: Record<string, any> = {};
  for (const rec of (json.records ?? [])) {
    const sym = rec.fields["Global Quote symbol"];
    if (sym) result[sym] = rec.fields;
  }
  return result;
}

async function fetchFinnhubStaggered(tickers: string[], key: string): Promise<Record<string, any>> {
  const out: Record<string, any> = {};
  for (let i = 0; i < tickers.length; i++) {
    const sym = tickers[i];
    if (i > 0) await sleep(STAGGER_MS);
    try {
      const resp = await fetch(`https://finnhub.io/api/v1/quote?symbol=${sym}&token=${key}`);
      if (resp.ok) out[sym] = await resp.json();
    } catch(e) { console.warn("Finnhub failed:", sym, e); }
  }
  return out;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}