// DRVN gamma-compute v2
// CHANGES from v1:
//   - Added circuit-breaker: check today's FlashAlpha usage before every call
//   - Aborts if usage ≥ 95/100
//   - Increments usage counter after every successful fetch
//
// DR 000 000 003 US

import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const AIRTABLE_TABLE = "tbltBwEPlkGpec8Kf";
const USAGE_LIMIT_HARD = 95;

const TICKER_TO_RECORD: Record<string, string> = {
  SPY:  "recUEWZaNYhCRK055",
  QQQ:  "recyIpyvUZU4Y6KMH",
  IWM:  "recDg8tPC5q4XaGDm",
  NVDA: "recHE2nTBTGWwuWv6",
  TSLA: "rec6ESJ4TGVLXSAsM",
  AAPL: "recw8vxJs8fMr19fv",
  AMD:  "recbqtmLYJkUtClmf",
  MSFT: "recmbWCIgPPqwLhlm",
  PLTR: "rec0ocnlt3TVvZi0w",
  META: "recBu9qIKLmhvfRIM",
};

const F = {
  gammaFlip:     "fldoG4jwk17iAtUOu",
  callWall:      "fldkLr6miAbvI5mfq",
  putWall:       "fldXomRksC1u4mHsw",
  gammaRegime:   "fldMqjfmBy7Wn3oTU",
  regimeContext: "fldhUa2ulY4A046HE",
};

const STAGGER_MS = 300;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json",
};

function sleep(ms: number) { return new Promise(r => setTimeout(r, ms)); }

async function getUsageToday(supabaseUrl: string, serviceKey: string): Promise<number> {
  const today = new Date().toISOString().slice(0, 10);
  const url = `${supabaseUrl}/rest/v1/flashalpha_usage_log?select=call_count&call_date=eq.${today}&limit=1`;
  try {
    const resp = await fetch(url, {
      headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` },
    });
    if (!resp.ok) return 0;
    const rows = await resp.json();
    if (!rows?.length) return 0;
    return Number(rows[0].call_count ?? 0);
  } catch { return 0; }
}

async function incrementUsage(supabaseUrl: string, serviceKey: string, delta: number): Promise<void> {
  const today = new Date().toISOString().slice(0, 10);
  const url = `${supabaseUrl}/rest/v1/rpc/flashalpha_increment_usage`;
  try {
    await fetch(url, {
      method: "POST",
      headers: {
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ p_date: today, p_delta: delta }),
    });
  } catch (e) { console.warn("Usage increment failed (non-fatal):", e); }
}

function classifyRegime(price: number | null, flip: number | null, apiRegime: string | null): string {
  if (price == null || flip == null) {
    if (apiRegime) {
      const r = apiRegime.toLowerCase();
      if (r.includes("positive")) return "Long Gamma";
      if (r.includes("negative")) return "Short Gamma";
    }
    return "Unknown";
  }
  const distPct = Math.abs((price - flip) / flip) * 100;
  if (distPct < 0.5) return "At Flip";
  if (price > flip)  return "Long Gamma";
  return "Short Gamma";
}

function buildRegimeContext(ticker: string, price: number | null, flip: number | null, callWall: number | null, putWall: number | null, regime: string): string {
  if (price == null || flip == null) return `${regime} — gamma data incomplete`;
  const diff = price - flip;
  const distPct = Math.abs((diff / flip) * 100);
  const distPts = Math.abs(diff).toFixed(2);
  if (regime === "At Flip") return `Danger zone — regime flipping (spot ${distPts} pts from flip, ${distPct.toFixed(2)}%)`;
  if (regime === "Long Gamma") return `Fade day — walls hold (flip ${distPts} pts below, ${distPct.toFixed(2)}%)`;
  if (regime === "Short Gamma") {
    if (distPct >= 3) return `Trend day — walls break (flip ${distPts} pts above, ${distPct.toFixed(2)}%)`;
    return `Trend day setup — shallow short gamma (flip ${distPts} pts above, ${distPct.toFixed(2)}%)`;
  }
  return `${regime} — spot ${distPts} pts from flip`;
}

async function fetchFlashAlpha(ticker: string, apiKey: string): Promise<{ ok: boolean; data?: any; error?: string }> {
  const url = `https://lab.flashalpha.com/v1/stock/${ticker.toLowerCase()}/summary`;
  try {
    const resp = await fetch(url, { headers: { "X-Api-Key": apiKey } });
    if (!resp.ok) {
      const body = await resp.text();
      return { ok: false, error: `FlashAlpha ${resp.status}: ${body.substring(0, 200)}` };
    }
    return { ok: true, data: await resp.json() };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function writeAirtable(base: string, key: string, recordId: string, flip: number | null, callWall: number | null, putWall: number | null, regime: string, context: string): Promise<{ ok: boolean; error?: string }> {
  const url = `https://api.airtable.com/v0/${base}/${AIRTABLE_TABLE}/${recordId}`;
  const fields: Record<string, unknown> = {
    [F.gammaRegime]: regime,
    [F.regimeContext]: context,
  };
  if (flip     != null) fields[F.gammaFlip] = flip;
  if (callWall != null) fields[F.callWall]  = callWall;
  if (putWall  != null) fields[F.putWall]   = putWall;
  try {
    const resp = await fetch(url, {
      method: "PATCH",
      headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
      body: JSON.stringify({ fields }),
    });
    if (!resp.ok) return { ok: false, error: `Airtable ${resp.status}: ${(await resp.text()).substring(0, 200)}` };
    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function readAirtableClose(base: string, key: string, recordId: string): Promise<number | null> {
  const url = `https://api.airtable.com/v0/${base}/${AIRTABLE_TABLE}/${recordId}`;
  try {
    const resp = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
    if (!resp.ok) return null;
    const rec = await resp.json();
    const v = rec?.fields?.["Daily Close"];
    if (v == null) return null;
    const n = parseFloat(String(v));
    return isNaN(n) ? null : n;
  } catch { return null; }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  const startedAt = new Date().toISOString();
  const AIRTABLE_KEY   = Deno.env.get("AIRTABLE_KEY");
  const AIRTABLE_BASE  = Deno.env.get("AIRTABLE_BASE");
  const FLASHALPHA_KEY = Deno.env.get("FLASHALPHA_KEY");
  const SUPABASE_URL   = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY    = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

  if (!AIRTABLE_KEY || !AIRTABLE_BASE) {
    return new Response(JSON.stringify({ error: "Missing AIRTABLE_KEY or AIRTABLE_BASE" }), { status: 500, headers: CORS });
  }
  if (!FLASHALPHA_KEY) {
    return new Response(JSON.stringify({ error: "Missing FLASHALPHA_KEY" }), { status: 500, headers: CORS });
  }

  const url = new URL(req.url);
  const singleTicker = url.searchParams.get("ticker")?.toUpperCase();
  const dryRun = url.searchParams.get("dry_run") === "1" || url.searchParams.get("dry_run") === "true";
  const runSource = url.searchParams.get("source") || "manual";

  const tickers = singleTicker
    ? (TICKER_TO_RECORD[singleTicker] ? [singleTicker] : [])
    : Object.keys(TICKER_TO_RECORD);

  if (tickers.length === 0) {
    return new Response(JSON.stringify({ error: `Unknown ticker: ${singleTicker}` }), { status: 400, headers: CORS });
  }

  // ---- CIRCUIT BREAKER ----
  let usageToday = 0;
  if (SUPABASE_URL && SERVICE_KEY) {
    usageToday = await getUsageToday(SUPABASE_URL, SERVICE_KEY);
    const projectedUsage = usageToday + tickers.length;
    if (projectedUsage > USAGE_LIMIT_HARD) {
      return new Response(JSON.stringify({
        ok: false,
        circuit_breaker: "TRIPPED",
        usage_today: usageToday,
        tickers_requested: tickers.length,
        projected_usage: projectedUsage,
        limit: USAGE_LIMIT_HARD,
        message: `Aborted — ${tickers.length}-ticker run would bring today's usage to ${projectedUsage}/${USAGE_LIMIT_HARD}. Protecting daily quota.`,
      }, null, 2), { status: 429, headers: CORS });
    }
  }

  const results: any[] = [];
  let fetchCount = 0, writeCount = 0;

  for (let i = 0; i < tickers.length; i++) {
    const ticker   = tickers[i];
    const recordId = TICKER_TO_RECORD[ticker];

    if (i > 0) await sleep(STAGGER_MS);

    const tResult: any = { ticker, ok: false };

    const fa = await fetchFlashAlpha(ticker, FLASHALPHA_KEY);
    fetchCount++;

    if (SUPABASE_URL && SERVICE_KEY && !dryRun) {
      await incrementUsage(SUPABASE_URL, SERVICE_KEY, 1);
    }

    if (!fa.ok || !fa.data) {
      tResult.error = fa.error;
      results.push(tResult);
      continue;
    }

    const exposure = fa.data.exposure ?? {};
    const price    = fa.data.price?.last ?? fa.data.price?.mid ?? null;
    const flip     = typeof exposure.gamma_flip === "number" ? exposure.gamma_flip : null;
    const callWall = typeof exposure.call_wall  === "number" ? exposure.call_wall  : null;
    const putWall  = typeof exposure.put_wall   === "number" ? exposure.put_wall   : null;
    const apiRegime = exposure.regime ?? null;

    let spotForRegime = price;
    if (spotForRegime == null) {
      spotForRegime = await readAirtableClose(AIRTABLE_BASE, AIRTABLE_KEY, recordId);
    }

    const regime  = classifyRegime(spotForRegime, flip, apiRegime);
    const context = buildRegimeContext(ticker, spotForRegime, flip, callWall, putWall, regime);

    tResult.spot = price;
    tResult.gamma_flip = flip;
    tResult.call_wall = callWall;
    tResult.put_wall = putWall;
    tResult.gamma_regime = regime;
    tResult.regime_context = context;
    tResult.api_regime = apiRegime;
    tResult.as_of = fa.data.as_of;

    if (!dryRun) {
      const w = await writeAirtable(AIRTABLE_BASE, AIRTABLE_KEY, recordId, flip, callWall, putWall, regime, context);
      if (!w.ok) {
        tResult.error = w.error;
        results.push(tResult);
        continue;
      }
      writeCount++;
    }

    tResult.ok = true;
    results.push(tResult);
  }

  return new Response(JSON.stringify({
    ok: true,
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    dry_run: dryRun,
    run_source: runSource,
    usage_before_run: usageToday,
    usage_after_run: usageToday + fetchCount,
    limit: USAGE_LIMIT_HARD,
    tickers_processed: tickers.length,
    fetch_count: fetchCount,
    write_count: writeCount,
    results,
  }, null, 2), { status: 200, headers: CORS });
});
