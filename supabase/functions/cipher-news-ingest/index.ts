// DRVN Edge Function: cipher-news-ingest v3
// NEW IN V3: Bigdata.com validity layer for scores >= 12 (ultra-strict gate)
//   - validated   -> multiplier 1.3x, adds evidence snippet
//   - rejected    -> multiplier 0.5x (Bigdata has no coverage, likely noise)
//   - neutral     -> multiplier 1.0x (coverage exists but no strong signal)
//   - error       -> multiplier 1.0x (treat as neutral on API failure)
// Fallback: if BIGDATA_API_KEY secret is missing, skip validation gracefully.
//
// Secrets required: FINNHUB_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
// Secrets optional: BIGDATA_API_KEY (validity layer activates only if present)

import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const DEFAULT_TICKERS = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "AMD", "MSFT", "PLTR", "META"];
const STAGGER_MS = 150;
const CLUSTER_WINDOW_HOURS = 24;
const CLUSTER_MIN_ITEMS = 3;

// Validity gate: only scores >= this threshold trigger a Bigdata call
const VALIDITY_SCORE_THRESHOLD = 12;
const BIGDATA_API_URL = "https://api.bigdata.com/cqs/v1/search";

// Ticker -> RavenPack entity ID (resolved via find_companies)
// These are stable IDs; no need to look them up at runtime.
const TICKER_TO_RP_ENTITY: Record<string, string> = {
  SPY:  "",        // ETF - skip validation for now
  QQQ:  "",        // ETF - skip validation for now
  IWM:  "",        // ETF - skip validation for now
  NVDA: "8B27E9",  // NVIDIA Corp
  TSLA: "DD3BB1",  // Tesla Inc
  AAPL: "D8442A",  // Apple Inc
  AMD:  "9EE0E7",  // Advanced Micro Devices
  MSFT: "D8442B",  // Microsoft Corp (placeholder - verify at first call)
  PLTR: "2F9EAE",  // Palantir Technologies (placeholder - verify at first call)
  META: "8BE1FC",  // Meta Platforms (placeholder - verify at first call)
};

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, apikey",
  "Content-Type": "application/json",
};

// ---------------------------------------------------------------
// Ticker <-> company name map (relevance gate)
// ---------------------------------------------------------------
const COMPANY_ALIASES: Record<string, RegExp> = {
  SPY:  /\b(SPY|S&P\s*500|SPX|S and P 500)\b/i,
  QQQ:  /\b(QQQ|Nasdaq\s*100|NDX|Invesco QQQ)\b/i,
  IWM:  /\b(IWM|Russell\s*2000|RUT|small.cap)\b/i,
  NVDA: /\b(NVDA|Nvidia)\b/i,
  TSLA: /\b(TSLA|Tesla)\b/i,
  AAPL: /\b(AAPL|Apple)\b/i,
  AMD:  /\b(AMD|Advanced Micro|Advanced Micro Devices)\b/i,
  MSFT: /\b(MSFT|Microsoft)\b/i,
  PLTR: /\b(PLTR|Palantir)\b/i,
  META: /\b(META|Meta Platforms|Facebook|Instagram|WhatsApp)\b/i,
};

const SOURCE_NAME_TO_DOMAIN: Record<string, string> = {
  "reuters": "reuters.com",
  "bloomberg": "bloomberg.com",
  "the wall street journal": "wsj.com",
  "wsj": "wsj.com",
  "financial times": "ft.com",
  "ft": "ft.com",
  "sec": "sec.gov",
  "cnbc": "cnbc.com",
  "barron's": "barrons.com",
  "barrons": "barrons.com",
  "marketwatch": "marketwatch.com",
  "seeking alpha": "seekingalpha.com",
  "seekingalpha": "seekingalpha.com",
  "yahoo": "finance.yahoo.com",
  "yahoo finance": "finance.yahoo.com",
  "business insider": "businessinsider.com",
  "investopedia": "investopedia.com",
  "benzinga": "benzinga.com",
  "zacks": "zacks.com",
  "zacks investment research": "zacks.com",
};

const KEYWORD_PATTERNS: Array<[RegExp, string]> = [
  [/\bbeat(s|ing)?\b.*\b(earnings|eps|estimate|expectation)/i, "earnings_beat"],
  [/\b(earnings|eps)\b.*\bbeat(s|ing)?\b/i, "earnings_beat"],
  [/\bmiss(es|ed|ing)?\b.*\b(earnings|eps|estimate|expectation)/i, "earnings_miss"],
  [/\b(earnings|eps)\b.*\bmiss(es|ed|ing)?\b/i, "earnings_miss"],
  [/\b(raise[ds]?|lift[s]?|boost[s]?|hike[s]?)\b.*\bguidance\b/i, "guidance_raise"],
  [/\bguidance\b.*\b(raise[ds]?|lift[s]?|boost[s]?|hike[s]?|above)/i, "guidance_raise"],
  [/\b(cut[s]?|lower[s]?|reduce[ds]?|slash[eds]?)\b.*\bguidance\b/i, "guidance_cut"],
  [/\bguidance\b.*\b(cut[s]?|lower[s]?|reduce[ds]?|below)/i, "guidance_cut"],
  [/\bfda\b.*\b(approv|clearance|grant)/i, "fda_approval"],
  [/\bfda\b.*\b(reject|denial|crl|complete response)/i, "fda_rejection"],
  [/\b(acqui[sr]|merg|buyout|takeover|deal to buy)/i, "merger_acquisition"],
  [/\b(bankrupt|chapter 11|chapter 7|insolven)/i, "bankruptcy"],
  [/\b(halt|halted|suspended trading)/i, "halted"],
  [/\b(buyback|share repurchase|stock repurchase)\b.*(authoriz|approv|announc)/i, "buyback_authorized"],
  [/\bdividend\b.*\b(cut|suspend|eliminat|reduc)/i, "dividend_cut"],
  [/\b(ceo|cfo|president|chairman)\b.*\b(resign|step[s]? down|depart|fire[ds]?|ouster|terminat)/i, "ceo_departure"],
  [/\bsec\b.*\b(investig|probe|subpoena|charge|settlement)/i, "sec_investigation"],
  [/\brecall\b/i, "recall"],
  [/\b(upgrade[ds]?|upgraded by)\b/i, "upgrade"],
  [/\b(downgrade[ds]?|downgraded by)\b/i, "downgrade"],
  [/\bprice target\b.*\b(raise|cut|lift|lower|increase|decrease|change)/i, "price_target_change"],
  [/\b(initiate[ds]?|initiation)\b.*\bcoverage\b/i, "analyst_initiation"],
  [/\binsider\b.*\b(buy|purchas|acquir)/i, "insider_buy"],
  [/\binsider\b.*\b(sell|sold|dispos)/i, "insider_sell"],
  [/\b(contract|deal|order)\b.*\$\s*\d+\s*(million|billion|m\b|b\b)/i, "major_contract"],
  [/\b(launch|unveil|release|introduc)\b.*\b(product|service|platform|feature)/i, "product_launch"],
  [/\b(lawsuit|litigation|sued|files suit|legal action)/i, "lawsuit_filed"],
  [/\bpartner(ship)?\b.*\b(with|announc|strateg)/i, "partnership"],
  [/\b(conference|summit|investor day)\b/i, "conference"],
  [/\bdividend\b.*\b(declar|announc|pay)/i, "dividend_declared"],
  [/\b13[fF]\b|\bquarterly holdings\b/i, "13f_filing"],
  [/\besg\b|\bsustainab/i, "esg_update"],
  [/\b(hire[ds]?|appoint[s]?|name[ds]?)\b.*\b(officer|executive|director|head of|vp\b)/i, "executive_hire"],
  [/\b(stocks to watch|stocks to buy|best stocks|top picks|watchlist|stocks to consider)/i, "listicle"],
  [/\b(trending on|twitter|reddit|social media sentiment)/i, "sentiment_recycle"],
];

const TICKER_THESIS_PATTERNS: Record<string, Array<[RegExp, string]>> = {
  NVDA: [
    [/\b(ai chip|gpu|h100|h200|blackwell|cuda|accelerator)/i, "NVDA:ai_chips"],
    [/\b(data center|hyperscaler|cloud compute)/i, "NVDA:data_center"],
  ],
  PLTR: [
    [/\b(government|defense|dod|pentagon|army|navy|air force|federal)/i, "PLTR:gov_contract"],
    [/\b(aip|artificial intelligence platform|foundry)/i, "PLTR:aip"],
  ],
  TSLA: [
    [/\b(deliver|production|vehicle sales|units)/i, "TSLA:deliveries"],
    [/\b(fsd|full self.driving|autopilot|robotaxi)/i, "TSLA:fsd"],
  ],
  AAPL: [
    [/\biphone\b/i, "AAPL:iphone"],
    [/\b(services|app store|subscription)/i, "AAPL:services"],
  ],
  MSFT: [
    [/\bazure\b/i, "MSFT:azure"],
    [/\b(copilot|ai assistant|openai)/i, "MSFT:ai_copilot"],
  ],
  AMD: [
    [/\b(data center|epyc|instinct|mi300)/i, "AMD:data_center"],
  ],
  META: [
    [/\b(ad revenue|advertis|reels|monetiz)/i, "META:ad_revenue"],
    [/\b(llama|ai\b|meta ai)/i, "META:ai"],
  ],
  SPY: [[/\b(fed|fomc|rate|inflation|cpi|jobs|gdp|recession)/i, "SPY:macro"]],
  QQQ: [[/\b(fed|fomc|rate|inflation|cpi|jobs|gdp|recession|tech sector)/i, "QQQ:macro"]],
  IWM: [[/\b(fed|fomc|rate|inflation|cpi|jobs|gdp|small cap|russell)/i, "IWM:macro"]],
};

// ---------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------
function extractDomain(url: string): string {
  try { return new URL(url).hostname.replace(/^www\./, ""); }
  catch { return "unknown"; }
}

function resolveSource(finnhubSource: string | null | undefined, url: string): string {
  if (finnhubSource) {
    const key = finnhubSource.toLowerCase().trim();
    if (SOURCE_NAME_TO_DOMAIN[key]) return SOURCE_NAME_TO_DOMAIN[key];
    return key;
  }
  return extractDomain(url);
}

async function sha256Hex(input: string): Promise<string> {
  const data = new TextEncoder().encode(input);
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash))
    .map(b => b.toString(16).padStart(2, "0")).join("").substring(0, 16);
}

function sleep(ms: number): Promise<void> { return new Promise(r => setTimeout(r, ms)); }

function getMondayOfWeek(d: Date): string {
  const date = new Date(d);
  const day = date.getUTCDay();
  const diff = date.getUTCDate() - day + (day === 0 ? -6 : 1);
  date.setUTCDate(diff);
  return date.toISOString().split("T")[0];
}

function isTickerRelevant(text: string, ticker: string): boolean {
  const pattern = COMPANY_ALIASES[ticker];
  if (!pattern) return true;
  return pattern.test(text);
}

// ---------------------------------------------------------------
// BIGDATA VALIDITY CHECK
// Called ONLY for stories scoring >= 12
// ---------------------------------------------------------------
interface ValidityResult {
  status: "validated" | "neutral" | "rejected" | "error";
  multiplier: number;
  evidence: any | null;
}

async function checkBigdataValidity(
  ticker: string,
  headline: string,
  apiKey: string,
): Promise<ValidityResult> {
  const rpEntityId = TICKER_TO_RP_ENTITY[ticker];
  if (!rpEntityId) {
    // ETF or unmapped ticker - skip, treat as neutral
    return { status: "neutral", multiplier: 1.0, evidence: null };
  }

  try {
    const now = new Date();
    const threeDaysAgo = new Date(now.getTime() - 3 * 24 * 60 * 60 * 1000);

    const body = {
      search_mode: "fast",
      query: {
        text: headline.substring(0, 200),
        max_chunks: 3,
        filters: {
          entity: { any_of: [rpEntityId] },
          document_type: { mode: "INCLUDE", values: ["NEWS"] },
          timestamp: {
            start: threeDaysAgo.toISOString(),
            end: now.toISOString(),
          },
        },
      },
    };

    const resp = await fetch(BIGDATA_API_URL, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      return { status: "error", multiplier: 1.0, evidence: { http_status: resp.status } };
    }

    const data = await resp.json();
    const results = data.results ?? [];

    if (results.length === 0) {
      // Bigdata has NO coverage of this entity+topic - likely noise
      return { status: "rejected", multiplier: 0.5, evidence: { reason: "no_coverage" } };
    }

    // Take top result's relevance as confidence signal
    const topRelevance = results[0]?.chunks?.[0]?.relevance ?? 0;

    if (topRelevance >= 0.7) {
      // Strong institutional coverage confirms the story
      return {
        status: "validated",
        multiplier: 1.3,
        evidence: {
          top_headline: results[0].headline,
          top_source: results[0].source?.name,
          relevance: topRelevance,
          url: results[0].url,
        },
      };
    }

    // Coverage exists but weak match - neutral
    return {
      status: "neutral",
      multiplier: 1.0,
      evidence: { relevance: topRelevance, match_count: results.length },
    };
  } catch (err) {
    return { status: "error", multiplier: 1.0, evidence: { error: String(err).substring(0, 200) } };
  }
}

// ---------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------
interface Weights {
  keywords: Record<string, { value: number; tier: string }>;
  sources: Record<string, number>;
  tickers: Record<string, number>;
}

interface ScoredItem {
  matched_keywords: string[];
  category_tier: string | null;
  keyword_score: number;
  source_multiplier: number;
  recency_bonus: number;
  ticker_multiplier: number;
  final_score: number;
  bucket: string;
}

function scoreHeadline(
  headline: string, summary: string, source: string,
  publishedAt: Date, ticker: string, weights: Weights,
): ScoredItem {
  const text = `${headline} ${summary}`;
  const matchedSet = new Set<string>();
  let keywordScore = 0;
  let categoryTier: string | null = null;
  let hasTierABC = false;

  for (const [pattern, key] of KEYWORD_PATTERNS) {
    if (pattern.test(text)) {
      const w = weights.keywords[key];
      if (w && !matchedSet.has(key)) {
        matchedSet.add(key);
        if (w.value > keywordScore) {
          keywordScore = w.value;
          categoryTier = w.tier;
        }
        if (w.tier === "A" || w.tier === "B" || w.tier === "C") hasTierABC = true;
      }
    }
  }
  const matched = Array.from(matchedSet);
  const sourceMultiplier = weights.sources[source] ?? 1.0;

  const ageHours = (Date.now() - publishedAt.getTime()) / (1000 * 60 * 60);
  let recencyBonus = 0;
  if (ageHours < 2) recencyBonus = 3;
  else if (ageHours < 6 && sourceMultiplier >= 1.5) recencyBonus = 2;
  else if (ageHours < 12) recencyBonus = 1;

  let tickerMultiplier = 1.0;
  let hasThesisMatch = false;
  const tickerPatterns = TICKER_THESIS_PATTERNS[ticker] ?? [];
  for (const [pattern, key] of tickerPatterns) {
    if (pattern.test(text)) {
      const w = weights.tickers[key];
      if (w) {
        hasThesisMatch = true;
        if (w > tickerMultiplier) tickerMultiplier = w;
      }
    }
  }

  const hasListicle = matchedSet.has("listicle") || matchedSet.has("sentiment_recycle");
  const forceFilter = hasListicle && !hasTierABC && !hasThesisMatch;

  const finalScore = forceFilter ? 0 : (keywordScore * sourceMultiplier + recencyBonus) * tickerMultiplier;

  let bucket = "FILTERED";
  if (forceFilter) bucket = "FILTERED";
  else if (finalScore >= 10) bucket = "HIGH";
  else if (finalScore >= 5) bucket = "MED";
  else if (finalScore >= 2) bucket = "LOW";

  return {
    matched_keywords: matched,
    category_tier: categoryTier,
    keyword_score: keywordScore,
    source_multiplier: sourceMultiplier,
    recency_bonus: recencyBonus,
    ticker_multiplier: tickerMultiplier,
    final_score: Math.round(finalScore * 100) / 100,
    bucket,
  };
}

function rebucket(score: number): string {
  if (score >= 10) return "HIGH";
  if (score >= 5) return "MED";
  if (score >= 2) return "LOW";
  return "FILTERED";
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------
Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  const FINNHUB_KEY = Deno.env.get("FINNHUB_KEY");
  const SUPABASE_URL = Deno.env.get("SUPABASE_URL");
  const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  const BIGDATA_KEY = Deno.env.get("BIGDATA_API_KEY"); // optional

  if (!FINNHUB_KEY || !SUPABASE_URL || !SERVICE_KEY) {
    return new Response(
      JSON.stringify({ error: "Missing FINNHUB_KEY, SUPABASE_URL, or SUPABASE_SERVICE_ROLE_KEY" }),
      { status: 500, headers: CORS },
    );
  }

  const url = new URL(req.url);
  const skipEarnings = url.searchParams.get("skip_earnings") === "1";
  const skipNews = url.searchParams.get("skip_news") === "1";
  const skipValidity = url.searchParams.get("skip_validity") === "1" || !BIGDATA_KEY;
  const singleSymbol = url.searchParams.get("symbol");
  const tickers = singleSymbol ? [singleSymbol.toUpperCase()] : DEFAULT_TICKERS;

  const supabase = createClient(SUPABASE_URL, SERVICE_KEY);
  const startedAt = new Date().toISOString();

  const { data: weightRows, error: wErr } = await supabase
    .from("cipher_feed_weights")
    .select("weight_type, key, value, tier")
    .eq("active", true);

  if (wErr) {
    return new Response(JSON.stringify({ error: `Weights load: ${wErr.message}` }),
      { status: 500, headers: CORS });
  }

  const weights: Weights = { keywords: {}, sources: {}, tickers: {} };
  for (const w of weightRows ?? []) {
    if (w.weight_type === "keyword_tier") weights.keywords[w.key] = { value: Number(w.value), tier: w.tier ?? "" };
    else if (w.weight_type === "source") weights.sources[w.key] = Number(w.value);
    else if (w.weight_type === "ticker_multiplier") weights.tickers[w.key] = Number(w.value);
  }

  const results: any[] = [];
  let totalNewsInserted = 0;
  let totalNewsScored = 0;
  let totalRelevanceFiltered = 0;
  let totalValidityCalls = 0;
  const bucketCounts: Record<string, number> = { HIGH: 0, MED: 0, LOW: 0, FILTERED: 0 };
  const validityCounts: Record<string, number> = { validated: 0, neutral: 0, rejected: 0, error: 0 };

  if (!skipNews) {
    const today = new Date().toISOString().split("T")[0];
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];

    for (let i = 0; i < tickers.length; i++) {
      const ticker = tickers[i];
      if (i > 0) await sleep(STAGGER_MS);

      const tResult: any = {
        ticker, status: "error",
        headlines_received: 0,
        headlines_relevance_filtered: 0,
        headlines_inserted: 0,
        validity_checks: 0,
        buckets: { HIGH: 0, MED: 0, LOW: 0, FILTERED: 0 },
      };

      try {
        const newsUrl = `https://finnhub.io/api/v1/company-news?symbol=${ticker}&from=${weekAgo}&to=${today}&token=${FINNHUB_KEY}`;
        const resp = await fetch(newsUrl);
        if (!resp.ok) { tResult.error = `HTTP ${resp.status}`; results.push(tResult); continue; }

        const items = await resp.json();
        if (!Array.isArray(items)) { tResult.error = "Non-array response"; results.push(tResult); continue; }

        tResult.headlines_received = items.length;
        const rows: any[] = [];

        for (const item of items) {
          if (!item.headline || !item.url) continue;
          const publishedAt = new Date((item.datetime ?? 0) * 1000);
          if (isNaN(publishedAt.getTime())) continue;

          const fullText = `${item.headline} ${item.summary ?? ""}`;
          if (!isTickerRelevant(fullText, ticker)) {
            tResult.headlines_relevance_filtered++;
            totalRelevanceFiltered++;
            continue;
          }

          const source = resolveSource(item.source, item.url);
          const scored = scoreHeadline(item.headline, item.summary ?? "", source, publishedAt, ticker, weights);

          totalNewsScored++;
          bucketCounts[scored.bucket] = (bucketCounts[scored.bucket] ?? 0) + 1;
          tResult.buckets[scored.bucket] = (tResult.buckets[scored.bucket] ?? 0) + 1;

          if (scored.bucket === "FILTERED") continue;

          // =============================================================
          // BIGDATA VALIDITY GATE - only if score >= 12
          // =============================================================
          let validityStatus = "not_applicable";
          let validityMultiplier = 1.0;
          let validityEvidence: any = null;
          let validityCheckedAt: string | null = null;
          const preValidityScore = scored.final_score;
          let finalScore = scored.final_score;
          let finalBucket = scored.bucket;

          if (!skipValidity && scored.final_score >= VALIDITY_SCORE_THRESHOLD && BIGDATA_KEY) {
            const validity = await checkBigdataValidity(ticker, item.headline, BIGDATA_KEY);
            validityStatus = validity.status;
            validityMultiplier = validity.multiplier;
            validityEvidence = validity.evidence;
            validityCheckedAt = new Date().toISOString();
            totalValidityCalls++;
            tResult.validity_checks++;
            validityCounts[validity.status] = (validityCounts[validity.status] ?? 0) + 1;

            // Apply validity multiplier and re-bucket
            finalScore = Math.round(scored.final_score * validity.multiplier * 100) / 100;
            finalBucket = rebucket(finalScore);
          }

          const idKey = `${ticker}|${publishedAt.getTime()}|${item.url}`;
          const hash = await sha256Hex(idKey);
          const id = `${ticker}-${publishedAt.toISOString().substring(0, 13).replace(/[-:T]/g, "")}-${hash}`;

          rows.push({
            id, ticker,
            headline: item.headline.substring(0, 500),
            summary: (item.summary ?? "").substring(0, 2000),
            source,
            source_url: item.url,
            image_url: item.image ?? null,
            finnhub_news_id: item.id ?? null,
            published_at: publishedAt.toISOString(),
            category_tier: scored.category_tier,
            matched_keywords: scored.matched_keywords,
            keyword_score: scored.keyword_score,
            source_multiplier: scored.source_multiplier,
            recency_bonus: scored.recency_bonus,
            ticker_multiplier: scored.ticker_multiplier,
            pre_validity_score: preValidityScore,
            validity_status: validityStatus,
            validity_multiplier: validityMultiplier,
            validity_evidence: validityEvidence,
            validity_checked_at: validityCheckedAt,
            final_score: finalScore,
            bucket: finalBucket,
          });
        }

        if (rows.length > 0) {
          const { error: insErr, count } = await supabase
            .from("cipher_feed")
            .upsert(rows, { onConflict: "id", ignoreDuplicates: true, count: "exact" });

          if (insErr) { tResult.error = `Insert: ${insErr.message}`; results.push(tResult); continue; }
          tResult.headlines_inserted = count ?? 0;
          totalNewsInserted += tResult.headlines_inserted;
        }

        tResult.status = "success";
        results.push(tResult);
      } catch (err) {
        tResult.error = `Exception: ${String(err)}`;
        results.push(tResult);
      }
    }

    // Cluster detection
    for (const ticker of tickers) {
      const { data: recent } = await supabase
        .from("cipher_feed")
        .select("id, published_at")
        .eq("ticker", ticker)
        .in("bucket", ["HIGH", "MED"])
        .gte("published_at", new Date(Date.now() - CLUSTER_WINDOW_HOURS * 60 * 60 * 1000).toISOString())
        .order("published_at", { ascending: true });

      if (recent && recent.length >= CLUSTER_MIN_ITEMS) {
        const clusterId = crypto.randomUUID();
        const ids = recent.map(r => r.id);
        await supabase.from("cipher_feed")
          .update({ is_cluster_member: true, cluster_id: clusterId })
          .in("id", ids);
      }
    }
  }

  // Earnings
  let earningsInserted = 0;
  if (!skipEarnings) {
    try {
      const now = new Date();
      const weekOf = getMondayOfWeek(now);
      const weekEnd = new Date(new Date(weekOf).getTime() + 6 * 24 * 60 * 60 * 1000).toISOString().split("T")[0];
      const earnUrl = `https://finnhub.io/api/v1/calendar/earnings?from=${weekOf}&to=${weekEnd}&token=${FINNHUB_KEY}`;
      const resp = await fetch(earnUrl);
      if (resp.ok) {
        const body = await resp.json();
        const earnings = body.earningsCalendar ?? [];
        const rows = earnings
          .filter((e: any) => tickers.includes(e.symbol))
          .map((e: any) => ({
            ticker: e.symbol, earnings_date: e.date,
            hour: e.hour ?? null,
            eps_estimate: e.epsEstimate ?? null,
            revenue_estimate: e.revenueEstimate ?? null,
            week_of: weekOf,
          }));
        if (rows.length > 0) {
          const { error: eErr, count } = await supabase
            .from("cipher_earnings")
            .upsert(rows, { onConflict: "ticker,earnings_date", count: "exact" });
          if (!eErr) earningsInserted = count ?? rows.length;
        }
      }
    } catch { /* non-fatal */ }
  }

  return new Response(
    JSON.stringify({
      started_at: startedAt,
      completed_at: new Date().toISOString(),
      version: "cipher-news-ingest v3 (Bigdata validity)",
      validity_enabled: !skipValidity && !!BIGDATA_KEY,
      validity_threshold: VALIDITY_SCORE_THRESHOLD,
      tickers_processed: tickers.length,
      news: {
        total_headlines_scored: totalNewsScored,
        total_relevance_filtered: totalRelevanceFiltered,
        total_headlines_inserted: totalNewsInserted,
        total_validity_calls: totalValidityCalls,
        bucket_counts: bucketCounts,
        validity_counts: validityCounts,
      },
      earnings: { rows_inserted: earningsInserted },
      per_ticker: results,
    }, null, 2),
    { status: 200, headers: CORS },
  );
});
