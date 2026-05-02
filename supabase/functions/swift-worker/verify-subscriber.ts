// ════════════════════════════════════════════════════════════════════
// swift-worker (verify-subscriber) — CORS-HARDENED REDEPLOY
// ────────────────────────────────────────────────────────────────────
// Endpoint: https://fyecjswjojftbouadfvp.supabase.co/functions/v1/swift-worker
//
// Purpose: Verify a DRVN community sign-in email by checking Beehiiv's
// subscription API server-side. The Beehiiv API key stays in Supabase
// secrets — never exposed to the browser.
//
// REQUIRED SECRETS (already set in Supabase):
//   BEEHIIV_PUB       - e.g. pub_f5f7b2b3-51d1-4252-b6d5-6e3762ce2950
//   BEEHIIV_KEY       - the rotated Beehiiv API key
//   PUBLISHER_EMAIL   - blueduckservices@gmail.com
//
// REQUEST:  POST { email: "user@example.com" }
// RESPONSE: 200 { verified: true|false, tier: "free"|"paid", isPublisher: bool, reason?: string }
//           400 { error: "invalid_email" | "invalid_body" }
//           429 { error: "rate_limited" }
//           502 { error: "upstream_error", detail?: string }
// ════════════════════════════════════════════════════════════════════

// ─── CORS HEADERS (applied to every single response, including errors) ───
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Max-Age": "86400",
};

// ─── Rate limiter (in-memory, per-IP, resets on cold start) ───
const rateLimitMap = new Map<string, number[]>();
const WINDOW_MS = 60_000;
const MAX_HITS = 10;

function isRateLimited(ip: string): boolean {
  const now = Date.now();
  const hits = (rateLimitMap.get(ip) || []).filter((t) => now - t < WINDOW_MS);
  if (hits.length >= MAX_HITS) return true;
  hits.push(now);
  rateLimitMap.set(ip, hits);
  return false;
}

// ─── Helpers ───
function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) && email.length <= 254;
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS, "Content-Type": "application/json" },
  });
}

// ─── Main handler ───
Deno.serve(async (req: Request): Promise<Response> => {
  // 1. CORS preflight — ALWAYS return 200 with CORS headers
  if (req.method === "OPTIONS") {
    return new Response("ok", { status: 200, headers: CORS });
  }

  // 2. Only POST is allowed for the actual verification
  if (req.method !== "POST") {
    return jsonResponse({ error: "method_not_allowed" }, 405);
  }

  // 3. Rate limit by IP
  const ip = req.headers.get("x-forwarded-for")?.split(",")[0]?.trim() || "unknown";
  if (isRateLimited(ip)) {
    return jsonResponse({ error: "rate_limited" }, 429);
  }

  // 4. Parse body
  let body: { email?: string };
  try {
    body = await req.json();
  } catch {
    return jsonResponse({ error: "invalid_body" }, 400);
  }

  const email = (body.email || "").trim().toLowerCase();
  if (!isValidEmail(email)) {
    return jsonResponse({ error: "invalid_email" }, 400);
  }

  // 5. Load secrets
  const BEEHIIV_PUB = Deno.env.get("BEEHIIV_PUB");
  const BEEHIIV_KEY = Deno.env.get("BEEHIIV_KEY");
  const PUBLISHER_EMAIL = (Deno.env.get("PUBLISHER_EMAIL") || "").toLowerCase();

  if (!BEEHIIV_PUB || !BEEHIIV_KEY) {
    console.error("Missing BEEHIIV_PUB or BEEHIIV_KEY secret");
    return jsonResponse({ error: "config_error" }, 502);
  }

  // 6. Publisher bypass (server-side, not spoofable from client)
  if (PUBLISHER_EMAIL && email === PUBLISHER_EMAIL) {
    return jsonResponse({ verified: true, tier: "paid", isPublisher: true });
  }

  // 7. Query Beehiiv
  try {
    const url =
      `https://api.beehiiv.com/v2/publications/${BEEHIIV_PUB}/subscriptions` +
      `?email=${encodeURIComponent(email)}&limit=1`;

    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${BEEHIIV_KEY}`,
        "Content-Type": "application/json",
      },
    });

    if (!res.ok) {
      const errText = await res.text();
      console.error(`Beehiiv API returned ${res.status}:`, errText.substring(0, 200));
      return jsonResponse(
        { error: "upstream_error", detail: `beehiiv_${res.status}` },
        502,
      );
    }

    const data = await res.json();
    const sub = data?.data?.[0];

    if (!sub) {
      return jsonResponse({ verified: false, reason: "not_subscribed" });
    }
    if (sub.status !== "active") {
      return jsonResponse({ verified: false, reason: "not_active" });
    }

    // Normalize tier: Beehiiv returns "free" for non-premium, or a premium
    // tier name for paid subscribers. Expose only the binary to the client.
    const tier =
      sub.subscription_tier && sub.subscription_tier !== "free"
        ? "paid"
        : "free";

    return jsonResponse({ verified: true, tier, isPublisher: false });
  } catch (err) {
    console.error("verify-subscriber error:", err);
    return jsonResponse(
      { error: "upstream_error", detail: (err as Error).message },
      502,
    );
  }
});