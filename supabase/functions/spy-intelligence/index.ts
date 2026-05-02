// DRVN spy-intelligence v1
// Pulls the FULL FlashAlpha /summary payload for SPY and writes 15 market-context
// fields to SPY's row in Investment Matrix. Runs 3x/day via pg_cron.
//
// Includes hard circuit-breaker: if today's total FlashAlpha usage ≥ 95, abort.
// Usage is tracked in Supabase Postgres table `public.flashalpha_usage_log`.
//
// Secrets required: AIRTABLE_KEY, AIRTABLE_BASE, FLASHALPHA_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
//
// DR 000 000 005 US

import "jsr:@supabase/functions-js/edge-runtime.d.ts";

const AIRTABLE_TABLE   = "tbltBwEPlkGpec8Kf";
const SPY_RECORD_ID    = "recUEWZaNYhCRK055";
const USAGE_LIMIT_HARD = 95;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json",
};

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
  } catch {
    return 0;
  }
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
  } catch (e) {
    console.warn("Usage increment failed (non-fatal):", e);
  }
}

async function fetchFlashAlpha(apiKey: string): Promise<any> {
  const url = "https://lab.flashalpha.com/v1/stock/spy/summary";
  const resp = await fetch(url, { headers: { "X-Api-Key": apiKey } });
  if (!resp.ok) throw new Error(`FlashAlpha ${resp.status}: ${(await resp.text()).substring(0, 200)}`);
  return await resp.json();
}

function buildMarketContext(data: {
  iv: number | null; hv20: number | null;
  vix: number | null; vixTerm: string | null;
  pcOi: number | null; fearGreed: number | null;
}): string {
  const parts: string[] = [];
  if (data.vix != null) {
    if (data.vix < 15) parts.push(`VIX ${data.vix.toFixed(1)} calm`);
    else if (data.vix < 20) parts.push(`VIX ${data.vix.toFixed(1)} normal`);
    else if (data.vix < 28) parts.push(`VIX ${data.vix.toFixed(1)} elevated`);
    else parts.push(`VIX ${data.vix.toFixed(1)} stressed`);
  }
  if (data.vixTerm) {
    if (data.vixTerm.toLowerCase().includes("contango")) parts.push("contango healthy");
    else if (data.vixTerm.toLowerCase().includes("backward")) parts.push("backwardation stress");
  }
  if (data.iv != null && data.hv20 != null) {
    const spread = data.iv - data.hv20;
    if (spread < -1) parts.push(`IV cheap vs HV (Δ${spread.toFixed(1)})`);
    else if (spread > 2) parts.push(`IV rich vs HV (+${spread.toFixed(1)})`);
    else parts.push("IV ≈ HV");
  }
  if (data.pcOi != null) {
    if (data.pcOi > 2.0) parts.push(`P/C ${data.pcOi.toFixed(2)} extreme bearish`);
    else if (data.pcOi > 1.3) parts.push(`P/C ${data.pcOi.toFixed(2)} bearish`);
    else if (data.pcOi < 0.6) parts.push(`P/C ${data.pcOi.toFixed(2)} extreme bullish`);
    else if (data.pcOi < 0.85) parts.push(`P/C ${data.pcOi.toFixed(2)} bullish`);
  }
  if (data.fearGreed != null) {
    if (data.fearGreed >= 75) parts.push(`F&G ${data.fearGreed} extreme greed`);
    else if (data.fearGreed >= 55) parts.push(`F&G ${data.fearGreed} greed`);
    else if (data.fearGreed >= 45) parts.push(`F&G ${data.fearGreed} neutral`);
    else if (data.fearGreed >= 25) parts.push(`F&G ${data.fearGreed} fear`);
    else parts.push(`F&G ${data.fearGreed} extreme fear`);
  }
  return parts.join(" · ");
}

async function writeAirtable(base: string, key: string, fields: Record<string, unknown>) {
  const clean: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v !== null && v !== undefined) clean[k] = v;
  }
  const url = `https://api.airtable.com/v0/${base}/${AIRTABLE_TABLE}/${SPY_RECORD_ID}`;
  const resp = await fetch(url, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${key}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields: clean, typecast: true }),
  });
  if (!resp.ok) {
    const body = await resp.text();
    return { ok: false, error: `Airtable ${resp.status}: ${body.substring(0, 500)}` };
  }
  return { ok: true };
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  const startedAt = new Date().toISOString();
  const url = new URL(req.url);
  const dryRun = url.searchParams.get("dry_run") === "1";
  const runSource = url.searchParams.get("source") || "manual";

  const AIRTABLE_KEY   = Deno.env.get("AIRTABLE_KEY");
  const AIRTABLE_BASE  = Deno.env.get("AIRTABLE_BASE");
  const FLASHALPHA_KEY = Deno.env.get("FLASHALPHA_KEY");
  const SUPABASE_URL   = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY    = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");

  if (!AIRTABLE_KEY || !AIRTABLE_BASE || !FLASHALPHA_KEY) {
    return new Response(JSON.stringify({ error: "Missing required secrets" }), { status: 500, headers: CORS });
  }

  let usageToday = 0;
  if (SUPABASE_URL && SERVICE_KEY) {
    usageToday = await getUsageToday(SUPABASE_URL, SERVICE_KEY);
    if (usageToday >= USAGE_LIMIT_HARD) {
      return new Response(JSON.stringify({
        ok: false,
        circuit_breaker: "TRIPPED",
        usage_today: usageToday,
        limit: USAGE_LIMIT_HARD,
        message: `Aborted — today's FlashAlpha usage ${usageToday}/${USAGE_LIMIT_HARD}. Protecting daily quota of 100.`,
      }, null, 2), { status: 429, headers: CORS });
    }
  }

  try {
    const fa = await fetchFlashAlpha(FLASHALPHA_KEY);

    if (SUPABASE_URL && SERVICE_KEY && !dryRun) {
      await incrementUsage(SUPABASE_URL, SERVICE_KEY, 1);
    }

    const vol   = fa.volatility ?? {};
    const flow  = fa.options_flow ?? {};
    const macro = fa.macro ?? {};
    const expo  = fa.exposure ?? {};

    const iv        = typeof vol.atm_iv === "number" ? vol.atm_iv : null;
    const hv20      = typeof vol.hv_20  === "number" ? vol.hv_20  : null;
    const hv60      = typeof vol.hv_60  === "number" ? vol.hv_60  : null;
    const pcOi      = typeof flow.pc_ratio_oi     === "number" ? flow.pc_ratio_oi     : null;
    const pcVol     = typeof flow.pc_ratio_volume === "number" ? flow.pc_ratio_volume : null;
    const oiDTE     = typeof flow.oi_weighted_dte === "number" ? flow.oi_weighted_dte : null;
    const vix       = typeof macro.vix?.value   === "number" ? macro.vix.value   : null;
    const vixChg    = typeof macro.vix?.change_percent === "number" ? macro.vix.change_percent : null;
    const vvix      = typeof macro.vvix?.value  === "number" ? macro.vvix.value  : null;
    const fearGreed = typeof macro.fear_and_greed?.score === "number" ? macro.fear_and_greed.score : null;
    const vixTerm   = macro.vix_term_structure?.structure ?? null;
    const netGex    = typeof expo.net_gex === "number" ? expo.net_gex / 1e9 : null;

    const ivHvSpread = iv != null && hv20 != null ? iv - hv20 : null;
    const marketContext = buildMarketContext({ iv, hv20, vix, vixTerm, pcOi, fearGreed });

    const writePayload: Record<string, unknown> = {
      "ATM IV": iv,
      "HV 20": hv20,
      "HV 60": hv60,
      "IV-HV Spread": ivHvSpread,
      "VIX": vix,
      "VIX Change %": vixChg,
      "VVIX": vvix,
      "VIX Term Structure": vixTerm,
      "Fear Greed Score": fearGreed,
      "P/C Ratio OI": pcOi,
      "P/C Ratio Volume": pcVol,
      "Net GEX": netGex,
      "OI Weighted DTE": oiDTE,
      "Market Context": marketContext || null,
      "SPY Intelligence Updated": new Date().toISOString(),
    };

    let writeResult: { ok: boolean; error?: string } = { ok: true };
    if (!dryRun) {
      writeResult = await writeAirtable(AIRTABLE_BASE, AIRTABLE_KEY, writePayload);
    }

    return new Response(JSON.stringify({
      ok: writeResult.ok,
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      dry_run: dryRun,
      run_source: runSource,
      usage_before_call: usageToday,
      usage_after_call: usageToday + 1,
      limit: USAGE_LIMIT_HARD,
      parsed: {
        atm_iv: iv, hv_20: hv20, hv_60: hv60, iv_hv_spread: ivHvSpread,
        vix, vix_change_pct: vixChg, vvix, vix_term: vixTerm,
        fear_greed: fearGreed,
        pc_oi: pcOi, pc_vol: pcVol, oi_weighted_dte: oiDTE,
        net_gex_billions: netGex,
        market_context: marketContext,
      },
      write_result: writeResult,
    }, null, 2), { status: 200, headers: CORS });
  } catch (err) {
    return new Response(JSON.stringify({ ok: false, error: String(err) }), { status: 500, headers: CORS });
  }
});
