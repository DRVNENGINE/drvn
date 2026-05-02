// DRVN cipher-public v2
// Public-facing CIPHER endpoint — returns everything the speedometer page needs.
// NEW in v2: component breakdown (Energy / Mechanical / Auction / Positioning)
// sourced from the most recent Conviction Log row for SPY.
// No JWT required. CORS open.
// DR 000 000 004 US

import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const AIRTABLE_TABLE = "tbltBwEPlkGpec8Kf";   // Investment Matrix
const SPY_RECORD_ID  = "recUEWZaNYhCRK055";
const LOG_TABLE      = "tblJ5WKlSOrS9zohd";    // Conviction Log

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json",
  "Cache-Control": "public, max-age=30",
};

function getMarketState(now: Date) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "short", hour: "numeric", minute: "numeric", hour12: false,
  });
  const parts = fmt.formatToParts(now);
  const weekday = parts.find(p => p.type === "weekday")?.value ?? "";
  const hour    = parseInt(parts.find(p => p.type === "hour")?.value ?? "0") % 24;
  const minute  = parseInt(parts.find(p => p.type === "minute")?.value ?? "0");
  const etTime  = `${weekday} ${String(hour).padStart(2,"0")}:${String(minute).padStart(2,"0")} ET`;

  if (weekday === "Sat" || weekday === "Sun") {
    return { state: "CLOSED" as const, reason: "Weekend — market closed. Next open: Monday 9:30 AM ET", et_time: etTime };
  }
  const mins = hour * 60 + minute;
  if (mins < 570)  return { state: "CLOSED" as const, reason: "Pre-market — opens 9:30 AM ET", et_time: etTime };
  if (mins > 960)  return { state: "CLOSED" as const, reason: "Market closed — final score locked", et_time: etTime };
  return { state: "LIVE" as const, reason: "Market open — refreshing every 30 min", et_time: etTime };
}

function unwrap(v: unknown): string | null {
  if (v == null) return null;
  if (typeof v === "object" && v !== null && "name" in (v as Record<string, unknown>)) {
    return String((v as { name: unknown }).name);
  }
  return String(v);
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  const AIRTABLE_KEY  = Deno.env.get("AIRTABLE_KEY");
  const AIRTABLE_BASE = Deno.env.get("AIRTABLE_BASE");
  if (!AIRTABLE_KEY || !AIRTABLE_BASE) {
    return new Response(JSON.stringify({ error: "Missing secrets" }), { status: 500, headers: CORS });
  }

  try {
    // 1. Investment Matrix → headline score + snapshot
    const matrixUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE}/${AIRTABLE_TABLE}/${SPY_RECORD_ID}`;
    const matrixResp = await fetch(matrixUrl, { headers: { Authorization: `Bearer ${AIRTABLE_KEY}` } });
    if (!matrixResp.ok) throw new Error(`Matrix ${matrixResp.status}: ${await matrixResp.text()}`);
    const record = await matrixResp.json();
    const f = record.fields ?? {};

    const score    = f["DRVN Conviction Score"] ?? null;
    const tier     = unwrap(f["Conviction Tier"]);
    const updated  = f["Conviction Updated"] ?? null;
    const regime   = f["Regime Context"] ?? null;

    // 2. Conviction Log → latest SPY row for component breakdown
    const logUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE}/${LOG_TABLE}` +
      `?filterByFormula=${encodeURIComponent("{Ticker}='SPY'")}` +
      `&sort%5B0%5D%5Bfield%5D=Timestamp&sort%5B0%5D%5Bdirection%5D=desc` +
      `&maxRecords=1`;
    const logResp = await fetch(logUrl, { headers: { Authorization: `Bearer ${AIRTABLE_KEY}` } });
    let energy = 0, mechanical = 0, auction = 0, positioning = 0;
    if (logResp.ok) {
      const logJson = await logResp.json();
      const latest = logJson.records?.[0]?.fields ?? {};
      energy      = Number(latest["Energy Score"]       ?? 0);
      mechanical  = Number(latest["Mechanical Score"]   ?? 0);
      auction     = Number(latest["Auction Score"]      ?? 0);
      positioning = Number(latest["Positioning Score"]  ?? 0);
    }

    const market = getMarketState(new Date());

    return new Response(JSON.stringify({
      ticker: "SPY",
      score,
      tier,
      updated_at: updated,
      market_state: market.state,       // "LIVE" | "CLOSED"
      market_reason: market.reason,
      et_time: market.et_time,
      regime_context: regime,
      components: {
        energy,       energy_max: 30,
        mechanical,   mechanical_max: 30,
        auction,      auction_max: 25,
        positioning,  positioning_max: 15,
      },
      snapshot: {
        close:        f["Daily Close"]   ?? null,
        pulse_state:  unwrap(f["PULSE State"]),
        gamma_regime: unwrap(f["Gamma Regime"]),
        gamma_flip:   f["Gamma Flip"]    ?? null,
        call_wall:    f["Call Wall"]     ?? null,
        put_wall:     f["Put Wall"]      ?? null,
      },
      fetched_at: new Date().toISOString(),
    }, null, 2), { status: 200, headers: CORS });
  } catch (err) {
    return new Response(JSON.stringify({ error: String(err) }), { status: 500, headers: CORS });
  }
});
