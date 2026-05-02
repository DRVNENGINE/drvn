// DRVN Edge Function: atr-compute v5
// Wilder ATR(14) + 50-day baseline + PER-TICKER THRESHOLDS
// Writes FULL PULSE state to Airtable: ATR(14), Compression Ratio, Baseline ATR(50), PULSE State, ATR Direction

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const DEFAULT_TICKERS = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "AMD", "MSFT", "PLTR", "META"];
const ATR_PERIOD = 14;
const BASELINE_50 = 50;
const BASELINE_90 = 90;

const FALLBACK_COMPRESS = 0.80;
const FALLBACK_EXPAND = 1.20;

const AIRTABLE_BASE_ID = "appq8EtS5o9d801kZ";
const AIRTABLE_TABLE_ID = "tbltBwEPlkGpec8Kf";

// Field IDs for the full PULSE payload
const FIELD_ATR_14 = "fldL86bUIX9Zjndw5";
const FIELD_COMPRESSION_RATIO = "fldo1kfrIzugVR4RG";
const FIELD_BASELINE_50 = "fldjbUUaNhYuqEjm6";
const FIELD_PULSE_STATE = "fldkawJCMz7DZgD0Z";
const FIELD_ATR_DIRECTION = "fldJi4fVGb6Tfn06d";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
  "Content-Type": "application/json",
};

interface Candle { trade_date: string; open: number; high: number; low: number; close: number; }
interface Threshold { ticker: string; compress_threshold: number; expand_threshold: number; }

function trueRange(h: number, l: number, pc: number): number {
  return Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
}

function computeWilderAtrSeries(candles: Candle[], period: number): number[] {
  if (candles.length < period + 1) return [];
  const trs: number[] = [];
  for (let i = 1; i < candles.length; i++) {
    trs.push(trueRange(candles[i].high, candles[i].low, candles[i - 1].close));
  }
  const atrs: number[] = [];
  let sum = 0;
  for (let i = 0; i < period; i++) sum += trs[i];
  atrs.push(sum / period);
  for (let i = period; i < trs.length; i++) {
    const prev = atrs[atrs.length - 1];
    atrs.push(((prev * (period - 1)) + trs[i]) / period);
  }
  return atrs;
}

function rollingAverage(values: number[], n: number): number | null {
  if (values.length < n) return null;
  const slice = values.slice(-n);
  return slice.reduce((a, b) => a + b, 0) / n;
}

function classifyState(ratio: number | null, compress: number, expand: number): string {
  if (ratio === null || isNaN(ratio)) return "Unknown";
  if (ratio < compress) return "Compressed";
  if (ratio > expand) return "Expanded";
  return "Normal";
}

function classifyDirection(curr: number, prev: number | undefined): string {
  if (prev === undefined) return "Unknown";
  const delta = curr - prev;
  const pct = Math.abs(delta) / prev;
  if (pct < 0.005) return "Flat";
  return delta > 0 ? "Rising" : "Falling";
}

const AIRTABLE_RECORD_IDS: Record<string, string> = {
  SPY: "recUEWZaNYhCRK055", QQQ: "recyIpyvUZU4Y6KMH", IWM: "recDg8tPC5q4XaGDm",
  NVDA: "recHE2nTBTGWwuWv6", TSLA: "rec6ESJ4TGVLXSAsM", AAPL: "recw8vxJs8fMr19fv",
  AMD: "recbqtmLYJkUtClmf", MSFT: "recmbWCIgPPqwLhlm", PLTR: "rec0ocnlt3TVvZi0w",
  META: "recBu9qIKLmhvfRIM",
};

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  const AIRTABLE_KEY = Deno.env.get("AIRTABLE_KEY");

  if (!SUPABASE_URL || !SERVICE_KEY) {
    return new Response(JSON.stringify({ error: "Missing Supabase secrets" }), { status: 500, headers: CORS });
  }

  const url = new URL(req.url);
  const singleSymbol = url.searchParams.get("symbol");
  const skipAirtable = url.searchParams.get("skip_airtable") === "1";
  const tickers = singleSymbol ? [singleSymbol.toUpperCase()] : DEFAULT_TICKERS;

  const supabase = createClient(SUPABASE_URL, SERVICE_KEY);
  const today = new Date().toISOString().split("T")[0];
  const results: any[] = [];

  const { data: thresholds } = await supabase.from("ticker_thresholds").select("*");
  const thresholdMap: Record<string, Threshold> = {};
  (thresholds ?? []).forEach((t: any) => { thresholdMap[t.ticker] = t; });

  for (const ticker of tickers) {
    const result: any = { ticker, status: "error" };
    const tMeta = thresholdMap[ticker];
    const compressT = tMeta?.compress_threshold ?? FALLBACK_COMPRESS;
    const expandT = tMeta?.expand_threshold ?? FALLBACK_EXPAND;

    try {
      const { data: candles, error } = await supabase
        .from("daily_candles")
        .select("trade_date, open, high, low, close")
        .eq("ticker", ticker)
        .order("trade_date", { ascending: true });

      if (error) { result.error = `DB error: ${error.message}`; results.push(result); continue; }
      if (!candles || candles.length < ATR_PERIOD + 1) {
        result.status = "insufficient_data";
        result.error = `Need ${ATR_PERIOD + 1} candles, have ${candles?.length ?? 0}`;
        results.push(result); continue;
      }

      const atrSeries = computeWilderAtrSeries(candles, ATR_PERIOD);
      if (atrSeries.length === 0) { result.error = "Empty ATR series"; results.push(result); continue; }

      const currentAtr = atrSeries[atrSeries.length - 1];
      const previousAtr = atrSeries.length >= 2 ? atrSeries[atrSeries.length - 2] : undefined;

      const baseline50 = rollingAverage(atrSeries, BASELINE_50);
      const baseline90 = rollingAverage(atrSeries, BASELINE_90);
      const baseline = baseline50 ?? baseline90 ?? (atrSeries.reduce((a, b) => a + b, 0) / atrSeries.length);

      const compressionRatio = baseline > 0 ? currentAtr / baseline : null;
      const volatilityState = classifyState(compressionRatio, compressT, expandT);
      const atrDirection = classifyDirection(currentAtr, previousAtr);

      const round4 = (v: number | null | undefined) => v === null || v === undefined ? null : Math.round(v * 10000) / 10000;
      const round2 = (v: number | null | undefined) => v === null || v === undefined ? null : Math.round(v * 100) / 100;
      const round3 = (v: number | null | undefined) => v === null || v === undefined ? null : Math.round(v * 1000) / 1000;

      const row = {
        ticker, compute_date: today,
        atr_14: round4(currentAtr),
        atr_baseline_50: round4(baseline50),
        atr_baseline_90: round4(baseline90),
        compression_ratio: round4(compressionRatio),
        volatility_state: volatilityState,
        atr_direction: atrDirection,
        atr_14_previous: round4(previousAtr ?? null),
        candles_used: candles.length,
        last_candle_date: candles[candles.length - 1].trade_date,
      };

      const { error: upErr } = await supabase.from("atr_analytics").upsert(row, { onConflict: "ticker,compute_date" });
      if (upErr) { result.error = `Upsert: ${upErr.message}`; results.push(result); continue; }

      result.status = "success";
      Object.assign(result, row);
      result.threshold_source = tMeta ? "per_ticker" : "fallback_universal";
      result.compress_threshold = compressT;
      result.expand_threshold = expandT;

      // Push FULL PULSE payload to Airtable
      if (!skipAirtable && AIRTABLE_KEY) {
        const recordId = AIRTABLE_RECORD_IDS[ticker];
        if (recordId) {
          const airtablePayload = {
            fields: {
              [FIELD_ATR_14]: round2(currentAtr),
              [FIELD_COMPRESSION_RATIO]: round3(compressionRatio),
              [FIELD_BASELINE_50]: round2(baseline50),
              [FIELD_PULSE_STATE]: volatilityState,
              [FIELD_ATR_DIRECTION]: atrDirection,
            }
          };
          const atResp = await fetch(
            `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}/${recordId}`,
            {
              method: "PATCH",
              headers: { Authorization: `Bearer ${AIRTABLE_KEY}`, "Content-Type": "application/json" },
              body: JSON.stringify(airtablePayload),
            }
          );
          result.airtable_updated = atResp.ok;
          if (!atResp.ok) {
            const err = await atResp.text();
            result.airtable_error = `HTTP ${atResp.status}: ${err.substring(0, 200)}`;
          }
        }
      }

      results.push(result);
    } catch (err) {
      result.error = `Exception: ${String(err)}`;
      results.push(result);
    }
  }

  const summary = {
    computed_at: new Date().toISOString(),
    compute_date: today,
    version: "v5 - full PULSE payload to Airtable",
    tickers_processed: results.length,
    successful: results.filter(r => r.status === "success").length,
    state_counts: {
      Compressed: results.filter(r => r.volatility_state === "Compressed").length,
      Normal: results.filter(r => r.volatility_state === "Normal").length,
      Expanded: results.filter(r => r.volatility_state === "Expanded").length,
    },
    airtable_success_count: results.filter(r => r.airtable_updated === true).length,
    airtable_failure_count: results.filter(r => r.airtable_updated === false).length,
    results,
  };

  return new Response(JSON.stringify(summary, null, 2), { status: 200, headers: CORS });
});
