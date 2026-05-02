// DRVN Edge Function: market-data-candles
// Pulls historical daily OHLC candles from Finnhub and upserts into daily_candles table.
// Designed to run on-demand (backfill) and via cron (daily refresh).
//
// Usage:
//   GET /functions/v1/market-data-candles                -> pulls default 90 days for all 10 tickers
//   GET /functions/v1/market-data-candles?days=365       -> custom lookback
//   GET /functions/v1/market-data-candles?symbol=SPY     -> single ticker
//
// Secrets required: FINNHUB_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const DEFAULT_TICKERS = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "AMD", "MSFT", "PLTR", "META"];
const DEFAULT_LOOKBACK_DAYS = 90;
const STAGGER_MS = 150; // Basic tier allows 150 calls/min = 1 every 400ms, we stagger at 150ms for safety

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
  "Content-Type": "application/json",
};

interface TickerResult {
  ticker: string;
  status: "success" | "error" | "no_data";
  candles_received: number;
  candles_written: number;
  error?: string;
  date_range?: { from: string; to: string };
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS });
  }

  const FINNHUB_KEY = Deno.env.get("FINNHUB_KEY");
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

  if (!FINNHUB_KEY || !SUPABASE_URL || !SERVICE_KEY) {
    return new Response(
      JSON.stringify({ error: "Missing required secrets: FINNHUB_KEY, SUPABASE_URL, or SUPABASE_SERVICE_ROLE_KEY" }),
      { status: 500, headers: CORS }
    );
  }

  // Parse query params
  const url = new URL(req.url);
  const days = parseInt(url.searchParams.get("days") ?? "") || DEFAULT_LOOKBACK_DAYS;
  const singleSymbol = url.searchParams.get("symbol");
  const tickers = singleSymbol ? [singleSymbol.toUpperCase()] : DEFAULT_TICKERS;

  // Initialize Supabase client with service role (bypasses RLS)
  const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

  const startedAt = new Date().toISOString();
  const results: TickerResult[] = [];

  // Compute date range (Unix timestamps for Finnhub)
  const toUnix = Math.floor(Date.now() / 1000);
  const fromUnix = toUnix - (days * 24 * 60 * 60);

  for (let i = 0; i < tickers.length; i++) {
    const ticker = tickers[i];

    // Stagger to respect rate limits
    if (i > 0) await sleep(STAGGER_MS);

    const result: TickerResult = {
      ticker,
      status: "error",
      candles_received: 0,
      candles_written: 0,
    };

    try {
      const candleUrl = `https://finnhub.io/api/v1/stock/candle?symbol=${ticker}&resolution=D&from=${fromUnix}&to=${toUnix}&token=${FINNHUB_KEY}`;
      const resp = await fetch(candleUrl);

      if (!resp.ok) {
        result.error = `HTTP ${resp.status}`;
        results.push(result);
        continue;
      }

      const body = await resp.json();

      if (body.s === "no_data") {
        result.status = "no_data";
        results.push(result);
        continue;
      }

      if (body.s !== "ok" || !Array.isArray(body.t) || body.t.length === 0) {
        result.error = `Unexpected response: ${JSON.stringify(body).substring(0, 200)}`;
        results.push(result);
        continue;
      }

      result.candles_received = body.t.length;

      // Transform Finnhub arrays into row records
      const rows = body.t.map((unixTs: number, idx: number) => ({
        ticker,
        trade_date: new Date(unixTs * 1000).toISOString().split("T")[0], // YYYY-MM-DD
        open: body.o[idx],
        high: body.h[idx],
        low: body.l[idx],
        close: body.c[idx],
        volume: body.v?.[idx] ?? null,
      }));

      result.date_range = {
        from: rows[0].trade_date,
        to: rows[rows.length - 1].trade_date,
      };

      // Upsert (insert or update on conflict with PK)
      const { error: upsertError, count } = await supabase
        .from("daily_candles")
        .upsert(rows, { onConflict: "ticker,trade_date", count: "exact" });

      if (upsertError) {
        result.error = `DB error: ${upsertError.message}`;
        results.push(result);
        continue;
      }

      result.candles_written = count ?? rows.length;
      result.status = "success";
      results.push(result);
    } catch (err) {
      result.error = `Exception: ${String(err)}`;
      results.push(result);
    }
  }

  // Summary
  const totalWritten = results.reduce((sum, r) => sum + r.candles_written, 0);
  const successCount = results.filter(r => r.status === "success").length;
  const errorCount = results.filter(r => r.status === "error").length;

  return new Response(
    JSON.stringify({
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      lookback_days: days,
      tickers_attempted: tickers.length,
      tickers_successful: successCount,
      tickers_errored: errorCount,
      total_candles_written: totalWritten,
      results,
    }, null, 2),
    { status: 200, headers: CORS }
  );
});

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
