// DRVN conviction-compute v9
// DR 000 000 004 US
//
// CHANGE FROM v8: Daily Close is now LIVE from Finnhub /quote, not stale Airtable field.
// Rationale: Investment Matrix's Daily Close field has no automated writer and was
// frozen at 711.58 for 4+ days while real spot moved through put wall to ATH.
// Mechanical and Positioning components were scoring against stale price.
// Solution: pull current price from Finnhub /quote on every run.
//
// Energy still uses Airtable PULSE State + ATR Direction (those ARE live via atr-compute).
// Mechanical still uses Airtable Gamma Flip + Gamma Regime (those ARE live via gamma-compute).
// Positioning still uses Airtable Upper/Lower bands (those ARE live).
// Only the Daily Close input switches from frozen Airtable to live Finnhub.
//
// Auction Flow median baseline (from v8) preserved.
//
// New debug fields:
//   live_price_debug.spot_used        — the price actually used for scoring
//   live_price_debug.airtable_close   — the stale value, kept for visibility
//   live_price_debug.finnhub_quote    — raw Finnhub /quote response
//   live_price_debug.source           — "finnhub" or "airtable_fallback"

import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const TICKER = "SPY";
const AIRTABLE_TABLE = "tbltBwEPlkGpec8Kf";
const SPY_RECORD_ID = "recUEWZaNYhCRK055";
const LOG_TABLE = "tblJ5WKlSOrS9zohd";

const F = {
  convictionScore: "fldgeYxcuKS3Y8UGe",
  convictionTier: "fldQ88ixSR5AGxKIQ",
  convictionUpdated: "fldw1HKdCVkmKoMYt",
};

const L = {
  timestamp: "fldgLJSA0WXb5Mrhx",
  ticker: "fldwO0feZRuc5mOuB",
  score: "fldbDzgsnL4oVRgLZ",
  tier: "fldGQ6U0qGxkk9kiZ",
  energyScore: "fldAWPbqiO9qffmmT",
  mechanicalScore: "fldi2FPBgmbJNDGet",
  auctionScore: "fldhAKEgsXyyJ2zq8",
  positioningScore: "fldhXDYf5UDQacWJS",
  rvol: "fldOGKDp3zrry9gTO",
  reasonSummary: "fldsp4zSo7w9ZcZ1b",
  pulseState: "flduBhsif10ri6VDp",
  atrDirection: "fldUk4bicdPdsUx6K",
  gammaRegime: "fld3QLxTlb8ewlk0d",
  close: "fldIR0unKzI94es3y",
  runSource: "fldmA94KomCQwMwL7",
};

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json",
};

interface AirtableRecord { id: string; fields: Record<string, any>; }
interface ConvictionBreakdown {
  energy: { score: number; reason: string };
  mechanical: { score: number; reason: string };
  auction: { score: number; reason: string; rvol: number | null };
  positioning: { score: number; reason: string };
  total: number;
  tier: "Elite" | "High" | "Moderate" | "Low" | "Dead";
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
  const startedAt = new Date().toISOString();

  const AIRTABLE_KEY = Deno.env.get("AIRTABLE_KEY");
  const AIRTABLE_BASE = Deno.env.get("AIRTABLE_BASE");
  const FINNHUB_KEY = Deno.env.get("FINNHUB_KEY");
  if (!AIRTABLE_KEY || !AIRTABLE_BASE || !FINNHUB_KEY) return jsonResponse({ error: "Missing secrets" }, 500);

  const url = new URL(req.url);
  const dryRun = url.searchParams.get("dry_run") === "1" || url.searchParams.get("dry_run") === "true";
  const force = url.searchParams.get("force") === "1" || url.searchParams.get("force") === "true";
  const runSource = url.searchParams.get("source") || (force ? "manual-force" : "manual");

  const marketHours = isMarketHours(new Date());
  if (!marketHours.open && !force && !dryRun) {
    return jsonResponse({
      ok: true, skipped: true,
      reason: marketHours.reason,
      market_time_et: marketHours.etTime,
      note: "Conviction refresh runs 9:30 AM - 4:00 PM ET, Mon-Fri. Use ?force=1 to override for testing.",
    }, 200);
  }

  try {
    const record = await fetchAirtableRecord(AIRTABLE_BASE, AIRTABLE_KEY);
    const fields = record.fields;

    // v9: Pull live price from Finnhub /quote, fall back to Airtable Daily Close if Finnhub fails
    const livePriceResult = await fetchLivePrice(TICKER, FINNHUB_KEY);
    const airtableClose = num(fields["Daily Close"]);
    const spotForScoring = livePriceResult.price ?? airtableClose;
    const livePriceDebug = {
      spot_used: spotForScoring,
      airtable_close: airtableClose,
      airtable_close_stale: airtableClose !== null && livePriceResult.price !== null && Math.abs(spotForScoring - airtableClose) > 0.5,
      stale_drift_pts: airtableClose !== null && livePriceResult.price !== null ? Number((livePriceResult.price - airtableClose).toFixed(2)) : null,
      finnhub_quote: livePriceResult.raw,
      source: livePriceResult.price !== null ? "finnhub" : "airtable_fallback",
      finnhub_error: livePriceResult.error ?? null,
    };

    const rvolResult = await computeRvol(TICKER, FINNHUB_KEY);
    const breakdown = computeConviction(fields, rvolResult.rvol, spotForScoring);

    const acceleration = detectAcceleration(rvolResult);

    let wrote = false;
    let logged = false;
    if (!dryRun) {
      await writeAirtable(AIRTABLE_BASE, AIRTABLE_KEY, breakdown);
      wrote = true;

      try {
        await writeLog(AIRTABLE_BASE, AIRTABLE_KEY, breakdown, fields, rvolResult, runSource, acceleration, spotForScoring);
        logged = true;
      } catch (logErr) {
        console.error("Log write failed (non-fatal):", logErr);
      }
    }

    return jsonResponse({
      ok: true,
      version: "v9-live-price",
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      ticker: TICKER, dry_run: dryRun, force,
      market_hours: marketHours, wrote, logged, breakdown,
      acceleration,
      live_price_debug: livePriceDebug,
      rvol_debug: rvolResult.debug,
      inputs: {
        spot_used: spotForScoring,
        airtable_close: airtableClose,
        pulse_state: unwrap(fields["PULSE State"]),
        atr_direction: unwrap(fields["ATR Direction"]),
        gamma_regime: unwrap(fields["Gamma Regime"]),
        gamma_flip: fields["Gamma Flip"],
        call_wall: fields["Call Wall"],
        put_wall: fields["Put Wall"],
        atr_14: fields["ATR (14)"],
        daily_upper_1: fields["Daily Upper 1"],
        daily_upper_2: fields["Daily Upper 2"],
        daily_lower_1: fields["Daily Lower 1"],
        daily_lower_2: fields["Daily Lower 2"],
      },
    }, 200);
  } catch (err) {
    return jsonResponse({ ok: false, error: String(err), stack: err instanceof Error ? err.stack : undefined }, 500);
  }
});

// v9: Fetch live SPY quote from Finnhub
async function fetchLivePrice(ticker: string, key: string): Promise<{ price: number | null; raw: any; error: string | null }> {
  try {
    const url = `https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${key}`;
    const resp = await fetch(url);
    if (!resp.ok) {
      return { price: null, raw: null, error: `Finnhub /quote HTTP ${resp.status}` };
    }
    const body = await resp.json();
    // Finnhub /quote returns: c=current, h=high, l=low, o=open, pc=prev close, d=change, dp=change%, t=timestamp
    const c = typeof body?.c === "number" ? body.c : null;
    if (c === null || c === 0) {
      return { price: null, raw: body, error: "Finnhub returned null/zero current price" };
    }
    return { price: c, raw: body, error: null };
  } catch (err) {
    return { price: null, raw: null, error: String(err) };
  }
}

function detectAcceleration(rvolResult: { rvol: number | null; debug: Record<string, any> }): Record<string, any> {
  const result: Record<string, any> = {
    event: false,
    current_rvol: rvolResult.rvol,
    prior_rvol: null,
    ratio: null,
    note: "Baseline (no prior bar in this run)",
  };

  const priorRvol = rvolResult.debug.prior_scoring_bar_rvol;
  if (priorRvol == null || rvolResult.rvol == null) return result;

  result.prior_rvol = priorRvol;
  if (priorRvol === 0) { result.ratio = null; result.note = "Prior RVOL was zero"; return result; }

  const ratio = rvolResult.rvol / priorRvol;
  result.ratio = Number(ratio.toFixed(2));

  if (ratio >= 2.0) {
    result.event = true;
    result.note = `\ud83d\udea8 ACCELERATION EVENT: RVOL ${priorRvol.toFixed(2)}x \u2192 ${rvolResult.rvol.toFixed(2)}x (${ratio.toFixed(2)}x increase period-over-period)`;
  } else if (ratio >= 1.5) {
    result.note = `Volume stepping up: ${priorRvol.toFixed(2)}x \u2192 ${rvolResult.rvol.toFixed(2)}x (${ratio.toFixed(2)}x)`;
  } else if (ratio <= 0.5) {
    result.note = `Volume decelerating: ${priorRvol.toFixed(2)}x \u2192 ${rvolResult.rvol.toFixed(2)}x (${ratio.toFixed(2)}x)`;
  } else {
    result.note = `Volume stable: ${priorRvol.toFixed(2)}x \u2192 ${rvolResult.rvol.toFixed(2)}x`;
  }
  return result;
}

function isMarketHours(now: Date): { open: boolean; reason: string; etTime: string } {
  const etFormatter = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "numeric", hour12: false });
  const parts = etFormatter.formatToParts(now);
  const weekday = parts.find(p => p.type === "weekday")?.value ?? "";
  const hourStr = parts.find(p => p.type === "hour")?.value ?? "0";
  const minuteStr = parts.find(p => p.type === "minute")?.value ?? "0";
  const hour = parseInt(hourStr) % 24;
  const minute = parseInt(minuteStr);
  const etTime = `${weekday} ${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")} ET`;
  if (weekday === "Sat" || weekday === "Sun") return { open: false, reason: "Weekend", etTime };
  const mins = hour * 60 + minute;
  if (mins < 570) return { open: false, reason: "Before market open", etTime };
  if (mins > 960) return { open: false, reason: "After market close", etTime };
  return { open: true, reason: "Regular trading hours", etTime };
}

async function fetchAirtableRecord(base: string, key: string): Promise<AirtableRecord> {
  const url = `https://api.airtable.com/v0/${base}/${AIRTABLE_TABLE}/${SPY_RECORD_ID}`;
  const resp = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
  if (!resp.ok) throw new Error(`Airtable fetch ${resp.status}: ${await resp.text()}`);
  return await resp.json();
}

async function writeAirtable(base: string, key: string, breakdown: ConvictionBreakdown): Promise<void> {
  const url = `https://api.airtable.com/v0/${base}/${AIRTABLE_TABLE}/${SPY_RECORD_ID}`;
  const payload = {
    fields: {
      [F.convictionScore]: breakdown.total,
      [F.convictionTier]: breakdown.tier,
      [F.convictionUpdated]: new Date().toISOString(),
    },
  };
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) throw new Error(`Airtable PATCH ${resp.status}: ${await resp.text()}`);
}

async function writeLog(
  base: string, key: string,
  breakdown: ConvictionBreakdown,
  fields: Record<string, any>,
  rvolResult: { rvol: number | null; debug: Record<string, any> },
  runSource: string,
  acceleration: Record<string, any>,
  spotUsed: number | null,
): Promise<void> {
  const url = `https://api.airtable.com/v0/${base}/${LOG_TABLE}`;

  const baseSummary = [
    `Energy: ${breakdown.energy.reason}`,
    `Mechanical: ${breakdown.mechanical.reason}`,
    `Auction: ${breakdown.auction.reason}`,
    `Positioning: ${breakdown.positioning.reason}`,
    rvolResult.debug.scoring_bar ? `Scoring bar: ${rvolResult.debug.scoring_bar.et_key} ${rvolResult.debug.scoring_bar.weekday}` : "",
  ].filter(Boolean).join(" | ");

  const accelLine = acceleration.event
    ? ` | \ud83d\udea8 ${acceleration.note}`
    : acceleration.ratio != null ? ` | Accel: ${acceleration.note}` : "";

  const reasonSummary = baseSummary + accelLine;

  const payload = {
    records: [{
      fields: {
        [L.timestamp]: new Date().toISOString(),
        [L.ticker]: TICKER,
        [L.score]: breakdown.total,
        [L.tier]: breakdown.tier,
        [L.energyScore]: breakdown.energy.score,
        [L.mechanicalScore]: breakdown.mechanical.score,
        [L.auctionScore]: breakdown.auction.score,
        [L.positioningScore]: breakdown.positioning.score,
        [L.rvol]: breakdown.auction.rvol !== null ? Number(breakdown.auction.rvol.toFixed(3)) : null,
        [L.reasonSummary]: reasonSummary,
        [L.pulseState]: unwrap(fields["PULSE State"]) || "",
        [L.atrDirection]: unwrap(fields["ATR Direction"]) || "",
        [L.gammaRegime]: unwrap(fields["Gamma Regime"]) || "",
        [L.close]: spotUsed,  // v9: log the LIVE price used, not the stale Airtable close
        [L.runSource]: acceleration.event ? `${runSource}-ACCEL\ud83d\udea8` : runSource,
      },
    }],
  };

  const resp = await fetch(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) throw new Error(`Log POST ${resp.status}: ${await resp.text()}`);
}

async function computeRvol(ticker: string, key: string): Promise<{ rvol: number | null; debug: Record<string, any> }> {
  const debug: Record<string, any> = { baseline_method: "median" };
  try {
    const now = Math.floor(Date.now() / 1000);
    const from = now - (25 * 24 * 60 * 60);
    const candleUrl = `https://finnhub.io/api/v1/stock/candle?symbol=${ticker}&resolution=30&from=${from}&to=${now}&token=${key}`;
    const resp = await fetch(candleUrl);
    if (!resp.ok) { debug.error = `Finnhub HTTP ${resp.status}`; return { rvol: null, debug }; }
    const body = await resp.json();
    if (body.s !== "ok" || !Array.isArray(body.v) || !Array.isArray(body.t) || body.v.length < 10) {
      debug.error = `Bad Finnhub response: s=${body.s}, len=${body.v?.length}`;
      return { rvol: null, debug };
    }
    const timestamps: number[] = body.t;
    const volumes: number[] = body.v;
    debug.total_bars = volumes.length;
    const etFmt = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", weekday: "short", hour: "numeric", minute: "numeric", hour12: false });
    interface Bar { ts: number; vol: number; etKey: string; isRth: boolean; weekday: string; }
    const bars: Bar[] = [];
    for (let i = 0; i < timestamps.length; i++) {
      const date = new Date(timestamps[i] * 1000);
      const parts = etFmt.formatToParts(date);
      const weekday = parts.find(p => p.type === "weekday")?.value ?? "";
      const hourStr = parts.find(p => p.type === "hour")?.value ?? "0";
      const minuteStr = parts.find(p => p.type === "minute")?.value ?? "0";
      const hour = parseInt(hourStr) % 24;
      const minute = parseInt(minuteStr);
      const etKey = `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
      const mins = hour * 60 + minute;
      const isRth = weekday !== "Sat" && weekday !== "Sun" && mins >= 570 && mins <= 930;
      bars.push({ ts: timestamps[i], vol: volumes[i], etKey, isRth, weekday });
    }
    const rthBars = bars.filter(b => b.isRth);
    if (rthBars.length < 2) { debug.error = "Not enough RTH bars"; return { rvol: null, debug }; }
    const latestBar = rthBars[rthBars.length - 1];
    const priorClosedBar = rthBars[rthBars.length - 2];
    debug.latest_bar_skipped = {
      ts: new Date(latestBar.ts * 1000).toISOString(),
      et_key: latestBar.etKey, volume: latestBar.vol,
      note: "Skipped \u2014 may still be building",
    };
    let scoringBar = priorClosedBar;
    const priorMatchingBars = bars.filter(b =>
      b.etKey === scoringBar.etKey && b.ts !== scoringBar.ts &&
      b.weekday !== "Sat" && b.weekday !== "Sun"
    );
    if (priorMatchingBars.length < 3) {
      debug.error = `Insufficient prior bars at ${scoringBar.etKey}: only ${priorMatchingBars.length}`;
      return { rvol: null, debug };
    }
    const medianVol = median(priorMatchingBars.map(b => b.vol));
    let usedFallback = false;
    if (medianVol > 0 && scoringBar.vol / medianVol < 0.3 && rthBars.length >= 3) {
      const fallbackBar = rthBars[rthBars.length - 3];
      const fallbackHistory = bars.filter(b =>
        b.etKey === fallbackBar.etKey && b.ts !== fallbackBar.ts &&
        b.weekday !== "Sat" && b.weekday !== "Sun"
      );
      if (fallbackHistory.length >= 3) {
        const fallbackMedian = median(fallbackHistory.map(b => b.vol));
        if (fallbackMedian > 0 && fallbackBar.vol / fallbackMedian > 0.3) {
          scoringBar = fallbackBar;
          usedFallback = true;
        }
      }
    }
    const finalHistory = bars.filter(b =>
      b.etKey === scoringBar.etKey && b.ts !== scoringBar.ts &&
      b.weekday !== "Sat" && b.weekday !== "Sun"
    );
    const finalMedian = median(finalHistory.map(b => b.vol));
    const finalMean = finalHistory.reduce((s, b) => s + b.vol, 0) / finalHistory.length;
    debug.scoring_bar = {
      ts: new Date(scoringBar.ts * 1000).toISOString(),
      et_key: scoringBar.etKey, weekday: scoringBar.weekday,
      volume: scoringBar.vol, used_fallback: usedFallback,
    };
    debug.matching_prior_bars = finalHistory.length;
    debug.baseline_median_volume = Math.round(finalMedian);
    debug.baseline_mean_volume_for_comparison = Math.round(finalMean);
    if (finalMedian === 0) { debug.error = "Median prior vol zero"; return { rvol: null, debug }; }
    const rvol = scoringBar.vol / finalMedian;
    debug.computed_rvol = rvol;

    const scoringBarIdx = rthBars.findIndex(b => b.ts === scoringBar.ts);
    if (scoringBarIdx > 0) {
      const priorScoringBar = rthBars[scoringBarIdx - 1];
      const priorHistory = bars.filter(b =>
        b.etKey === priorScoringBar.etKey && b.ts !== priorScoringBar.ts &&
        b.weekday !== "Sat" && b.weekday !== "Sun"
      );
      if (priorHistory.length >= 3) {
        const priorMedian = median(priorHistory.map(b => b.vol));
        if (priorMedian > 0) {
          const priorRvol = priorScoringBar.vol / priorMedian;
          debug.prior_scoring_bar = {
            et_key: priorScoringBar.etKey,
            volume: priorScoringBar.vol,
            median_baseline: Math.round(priorMedian),
            rvol: Number(priorRvol.toFixed(3)),
          };
          debug.prior_scoring_bar_rvol = priorRvol;
        }
      }
    }
    return { rvol, debug };
  } catch (err) {
    debug.exception = String(err);
    return { rvol: null, debug };
  }
}

// v9: spot is now a parameter (live Finnhub price), not read from Airtable Daily Close
function computeConviction(fields: Record<string, any>, rvol: number | null, spot: number | null): ConvictionBreakdown {
  const pulseState = unwrap(fields["PULSE State"]);
  const atrDirection = unwrap(fields["ATR Direction"]);
  const gammaFlip = num(fields["Gamma Flip"]);
  const gammaRegime = unwrap(fields["Gamma Regime"]);
  const u1 = num(fields["Daily Upper 1"]);
  const u2 = num(fields["Daily Upper 2"]);
  const l1 = num(fields["Daily Lower 1"]);
  const l2 = num(fields["Daily Lower 2"]);
  const energy = scoreEnergy(pulseState, atrDirection);
  const mechanical = scoreMechanical(spot, gammaFlip, gammaRegime);
  const auction = scoreAuction(rvol);
  const positioning = scorePositioning(spot, u1, u2, l1, l2);
  const total = energy.score + mechanical.score + auction.score + positioning.score;
  const tier = classifyTier(total);
  return { energy, mechanical, auction, positioning, total, tier };
}

function scoreEnergy(pulse: string | null, atrDir: string | null): { score: number; reason: string } {
  const p = (pulse || "").toLowerCase();
  const d = (atrDir || "").toLowerCase();
  if (p === "compressed" && d === "rising") return { score: 30, reason: "Compressed + Rising (max energy)" };
  if (p === "compressed" && d === "flat") return { score: 26, reason: "Compressed + Flat (coiled, waiting)" };
  if (p === "normal" && d === "rising") return { score: 19, reason: "Normal + Rising (expanding from base)" };
  if (p === "expanded" && d === "rising") return { score: 11, reason: "Expanded + Rising (late stage)" };
  if (p === "normal" && d === "flat") return { score: 8, reason: "Normal + Flat (dormant)" };
  if (p === "compressed" && d === "falling") return { score: 4, reason: "Compressed + Falling (no catalyst)" };
  if (p === "normal" && d === "falling") return { score: 4, reason: "Normal + Falling (losing momentum)" };
  if (p === "expanded" && d === "flat") return { score: 2, reason: "Expanded + Flat (fatigue)" };
  if (p === "expanded" && d === "falling") return { score: 0, reason: "Expanded + Falling (post-climax)" };
  return { score: 0, reason: `Unknown PULSE/ATR combo: ${pulse}/${atrDir}` };
}

function scoreMechanical(close: number | null, flip: number | null, regime: string | null): { score: number; reason: string } {
  if (close === null || flip === null || !regime) return { score: 15, reason: "Gamma data missing (neutral 15)" };
  const distPct = ((close - flip) / flip) * 100;
  const r = regime.toLowerCase();
  if (r === "at flip" || Math.abs(distPct) < 2) return { score: 30, reason: `At Flip zone (${distPct.toFixed(2)}% from flip)` };
  if (r === "long gamma") {
    if (distPct >= 5) return { score: 20, reason: `Long Gamma, deep (${distPct.toFixed(2)}% above flip)` };
    if (distPct >= 2) return { score: 25, reason: `Long Gamma, shallow (${distPct.toFixed(2)}% above flip)` };
  }
  if (r === "short gamma") {
    if (distPct >= -5) return { score: 28, reason: `Short Gamma, shallow (${distPct.toFixed(2)}% below flip)` };
    return { score: 22, reason: `Short Gamma, deep (${distPct.toFixed(2)}% below flip)` };
  }
  return { score: 15, reason: `Unknown regime: ${regime}` };
}

function scoreAuction(rvol: number | null): { score: number; reason: string; rvol: number | null } {
  if (rvol === null) return { score: 12, reason: "RVOL unavailable (neutral 12)", rvol: null };
  let score: number; let reason: string;
  if (rvol >= 2.0) { score = 25; reason = `Elite volume (${rvol.toFixed(2)}x avg)`; }
  else if (rvol >= 1.5) { score = 20; reason = `Heavy volume (${rvol.toFixed(2)}x avg)`; }
  else if (rvol >= 1.2) { score = 16; reason = `Above average (${rvol.toFixed(2)}x)`; }
  else if (rvol >= 0.8) { score = 10; reason = `Normal volume (${rvol.toFixed(2)}x)`; }
  else if (rvol >= 0.5) { score = 5; reason = `Thin volume (${rvol.toFixed(2)}x) \u2014 no options entries`; }
  else { score = 0; reason = `Dead (${rvol.toFixed(2)}x) \u2014 no options entries`; }
  return { score, reason, rvol };
}

function scorePositioning(close: number | null, u1: number | null, u2: number | null, l1: number | null, l2: number | null): { score: number; reason: string } {
  if (close === null || u1 === null || l1 === null) return { score: 8, reason: "Levels missing (neutral 8)" };
  if (u2 !== null && close >= u2) return { score: 15, reason: "Above Upper 2 (stretched high)" };
  if (l2 !== null && close <= l2) return { score: 15, reason: "Below Lower 2 (stretched low)" };
  const nearU1 = Math.abs(close - u1) / u1 < 0.003;
  const nearL1 = Math.abs(close - l1) / l1 < 0.003;
  if (nearU1) return { score: 13, reason: "At Upper 1 (decision zone)" };
  if (nearL1) return { score: 13, reason: "At Lower 1 (decision zone)" };
  if (u2 !== null && close > u1 && close < u2) return { score: 12, reason: "Between U1 and U2 (upper zone)" };
  if (l2 !== null && close < l1 && close > l2) return { score: 12, reason: "Between L1 and L2 (lower zone)" };
  return { score: 8, reason: "Inside normal range" };
}

function classifyTier(score: number): "Elite" | "High" | "Moderate" | "Low" | "Dead" {
  if (score >= 80) return "Elite";
  if (score >= 65) return "High";
  if (score >= 45) return "Moderate";
  if (score >= 25) return "Low";
  return "Dead";
}

function unwrap(v: any): string | null {
  if (v == null) return null;
  if (typeof v === "object" && v.name) return v.name;
  return String(v);
}
function num(v: any): number | null {
  if (v == null || v === "") return null;
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}
function jsonResponse(body: any, status: number): Response {
  return new Response(JSON.stringify(body, null, 2), { status, headers: CORS });
}
