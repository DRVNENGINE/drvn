// DRVN ohlc-update v2
// DR 000 000 011 RN
//
// FIX FROM v1: Removed write to fldyXQeEDmkP1Kpmn ("TPO Updated") which is a
// computed field. PATCH was being rejected with INVALID_VALUE_FOR_COLUMN, causing
// all 9 H/L/C writes per ticker to revert.
//
// On every run:
//   - For each ticker in WATCHLIST:
//     - Pull Finnhub /quote for today's intraday OHLC + previous close
//     - Read daily_candles table for the ticker
//     - Compute weekly H/L/Close from current ISO week's daily candles
//     - Compute monthly H/L/Close from current calendar month's daily candles
//     - PATCH the corresponding Investment Matrix row
//
// Designed to run on-demand (manual fix) and via pg_cron at 4:05 PM ET daily.
//
// Secrets: FINNHUB_KEY, AIRTABLE_KEY, AIRTABLE_BASE, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const AIRTABLE_TABLE = "tbltBwEPlkGpec8Kf";

// Ticker -> Investment Matrix record ID
const TICKER_RECORDS: Record<string, string> = {
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

// Investment Matrix field IDs
const F = {
  // Daily
  dailyHigh:  "fldXrq1iIoeVQ5v7R",
  dailyLow:   "fldyJQJ8lUvxfpuhK",
  dailyClose: "fldCQ7LdF0AGUbWF7",
  // Weekly
  weeklyHigh:  "fldaAGNXq7YoovhFf",
  weeklyLow:   "fld6dudXd806pZNv7",
  weeklyClose: "fldo91L8BLfgRe4xy",
  // Monthly
  monthlyHigh:  "fldYBtJtwkI22UeLr",
  monthlyLow:   "fldw2mLlzn0ewT7K2",
  monthlyClose: "fldDmIvfXNcUqb4j4",
  // NOTE: "TPO Updated" (fldyXQeEDmkP1Kpmn) is a COMPUTED field. Do not write.
};

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json",
};

const STAGGER_MS = 200;

interface TickerResult {
  ticker: string;
  status: "success" | "error" | "partial";
  daily?:   { open: number; high: number; low: number; close: number; prev_close: number };
  weekly?:  { high: number; low: number; close: number; bars_used: number; week_start: string };
  monthly?: { high: number; low: number; close: number; bars_used: number; month: string };
  written?: Record<string, number>;
  error?: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function getISOWeekStart(now: Date): string {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit", day: "2-digit",
    weekday: "short",
  });
  const parts = fmt.formatToParts(now);
  const y = parts.find(p => p.type === "year")!.value;
  const m = parts.find(p => p.type === "month")!.value;
  const d = parts.find(p => p.type === "day")!.value;
  const wd = parts.find(p => p.type === "weekday")!.value;
  const todayStr = `${y}-${m}-${d}`;
  const dayOffset: Record<string, number> = {
    Mon: 0, Tue: 1, Wed: 2, Thu: 3, Fri: 4, Sat: 5, Sun: 6,
  };
  const offset = dayOffset[wd] ?? 0;
  const todayMs = new Date(`${todayStr}T12:00:00-04:00`).getTime();
  const mondayMs = todayMs - offset * 24 * 60 * 60 * 1000;
  return new Date(mondayMs).toISOString().slice(0, 10);
}

function getCurrentMonth(now: Date): string {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit",
  });
  const parts = fmt.formatToParts(now);
  const y = parts.find(p => p.type === "year")!.value;
  const m = parts.find(p => p.type === "month")!.value;
  return `${y}-${m}`;
}

function getTodayNY(now: Date): string {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric", month: "2-digit", day: "2-digit",
  });
  const parts = fmt.formatToParts(now);
  const y = parts.find(p => p.type === "year")!.value;
  const m = parts.find(p => p.type === "month")!.value;
  const d = parts.find(p => p.type === "day")!.value;
  return `${y}-${m}-${d}`;
}

async function fetchFinnhubQuote(ticker: string, key: string): Promise<{ o: number; h: number; l: number; c: number; pc: number } | null> {
  try {
    const url = `https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${key}`;
    const resp = await fetch(url);
    if (!resp.ok) return null;
    const body = await resp.json();
    if (typeof body?.c !== "number" || body.c === 0) return null;
    return { o: body.o, h: body.h, l: body.l, c: body.c, pc: body.pc };
  } catch {
    return null;
  }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  const FINNHUB_KEY  = Deno.env.get("FINNHUB_KEY");
  const AIRTABLE_KEY = Deno.env.get("AIRTABLE_KEY");
  const AIRTABLE_BASE = Deno.env.get("AIRTABLE_BASE");
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY  = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!FINNHUB_KEY || !AIRTABLE_KEY || !AIRTABLE_BASE || !SUPABASE_URL || !SERVICE_KEY) {
    return new Response(JSON.stringify({ error: "Missing required secrets" }), { status: 500, headers: CORS });
  }

  const url = new URL(req.url);
  const dryRun = url.searchParams.get("dry_run") === "1";
  const tickerFilter = url.searchParams.get("symbol")?.toUpperCase();
  const runSource = url.searchParams.get("source") || "manual";

  const supabase = createClient(SUPABASE_URL, SERVICE_KEY);
  const startedAt = new Date().toISOString();
  const now = new Date();
  const todayNY = getTodayNY(now);
  const weekStart = getISOWeekStart(now);
  const currentMonth = getCurrentMonth(now);

  const tickers = tickerFilter
    ? (TICKER_RECORDS[tickerFilter] ? [tickerFilter] : [])
    : Object.keys(TICKER_RECORDS);

  if (tickers.length === 0) {
    return new Response(JSON.stringify({ error: `Unknown ticker: ${tickerFilter}` }), { status: 400, headers: CORS });
  }

  const results: TickerResult[] = [];

  for (let i = 0; i < tickers.length; i++) {
    const ticker = tickers[i];
    const recordId = TICKER_RECORDS[ticker];
    if (i > 0) await sleep(STAGGER_MS);

    const result: TickerResult = { ticker, status: "error" };

    try {
      const quote = await fetchFinnhubQuote(ticker, FINNHUB_KEY);
      if (!quote) {
        result.error = "Finnhub quote unavailable";
        results.push(result);
        continue;
      }
      result.daily = { open: quote.o, high: quote.h, low: quote.l, close: quote.c, prev_close: quote.pc };

      const { data: candles, error: candleErr } = await supabase
        .from("daily_candles")
        .select("trade_date, open, high, low, close")
        .eq("ticker", ticker)
        .gte("trade_date", weekStart)
        .order("trade_date", { ascending: true });

      if (candleErr) {
        result.error = `daily_candles query failed: ${candleErr.message}`;
        results.push(result);
        continue;
      }

      const weekBars = (candles ?? []).filter(c => c.trade_date >= weekStart && c.trade_date < todayNY);
      const todayBar = { trade_date: todayNY, open: quote.o, high: quote.h, low: quote.l, close: quote.c };
      weekBars.push(todayBar);

      let weekHigh = -Infinity, weekLow = Infinity;
      for (const b of weekBars) {
        if (b.high > weekHigh) weekHigh = b.high;
        if (b.low  < weekLow)  weekLow = b.low;
      }
      const weekClose = todayBar.close;
      result.weekly = { high: weekHigh, low: weekLow, close: weekClose, bars_used: weekBars.length, week_start: weekStart };

      const monthStart = `${currentMonth}-01`;
      const { data: monthCandles, error: monthErr } = await supabase
        .from("daily_candles")
        .select("trade_date, open, high, low, close")
        .eq("ticker", ticker)
        .gte("trade_date", monthStart)
        .order("trade_date", { ascending: true });

      if (monthErr) {
        result.error = `daily_candles month query failed: ${monthErr.message}`;
        result.status = "partial";
        results.push(result);
        continue;
      }

      const monthBars = (monthCandles ?? []).filter(c => c.trade_date >= monthStart && c.trade_date < todayNY);
      monthBars.push(todayBar);

      let monthHigh = -Infinity, monthLow = Infinity;
      for (const b of monthBars) {
        if (b.high > monthHigh) monthHigh = b.high;
        if (b.low  < monthLow)  monthLow = b.low;
      }
      const monthClose = todayBar.close;
      result.monthly = { high: monthHigh, low: monthLow, close: monthClose, bars_used: monthBars.length, month: currentMonth };

      // v2: removed fldyXQeEDmkP1Kpmn write — it's a computed field
      const writePayload = {
        [F.dailyHigh]: quote.h,
        [F.dailyLow]: quote.l,
        [F.dailyClose]: quote.c,
        [F.weeklyHigh]: weekHigh,
        [F.weeklyLow]: weekLow,
        [F.weeklyClose]: weekClose,
        [F.monthlyHigh]: monthHigh,
        [F.monthlyLow]: monthLow,
        [F.monthlyClose]: monthClose,
      };
      result.written = writePayload;

      if (!dryRun) {
        const patchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE}/${AIRTABLE_TABLE}/${recordId}`;
        const patchResp = await fetch(patchUrl, {
          method: "PATCH",
          headers: { Authorization: `Bearer ${AIRTABLE_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({ fields: writePayload }),
        });
        if (!patchResp.ok) {
          result.error = `Airtable PATCH ${patchResp.status}: ${(await patchResp.text()).substring(0, 200)}`;
          result.status = "partial";
          results.push(result);
          continue;
        }
      }

      result.status = "success";
      results.push(result);
    } catch (err) {
      result.error = String(err);
      results.push(result);
    }
  }

  const successCount = results.filter(r => r.status === "success").length;
  const errorCount = results.filter(r => r.status === "error").length;
  const partialCount = results.filter(r => r.status === "partial").length;

  return new Response(JSON.stringify({
    ok: errorCount === 0 && partialCount === 0,
    version: "v2-no-tpo-write",
    started_at: startedAt,
    completed_at: new Date().toISOString(),
    run_source: runSource,
    dry_run: dryRun,
    today_ny: todayNY,
    week_start: weekStart,
    current_month: currentMonth,
    tickers_attempted: tickers.length,
    tickers_successful: successCount,
    tickers_partial: partialCount,
    tickers_errored: errorCount,
    results,
  }, null, 2), { status: 200, headers: CORS });
});
