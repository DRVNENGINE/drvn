// DEPRECATED - Replaced by market-data-candles
// This function is no longer used. Kept as a safe no-op for backwards compatibility.
import "jsr:@supabase/functions-js/edge-runtime.d.ts";
Deno.serve(() => {
  return new Response(
    JSON.stringify({
      message: "verify-finnhub-candles is deprecated. Use market-data-candles instead.",
      deprecated: true,
      replacement: "/functions/v1/market-data-candles"
    }),
    { headers: { "Content-Type": "application/json" } }
  );
});
