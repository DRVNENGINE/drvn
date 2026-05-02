// DEPRECATED - debug-airtable-key served its purpose and is now retired.
import "jsr:@supabase/functions-js/edge-runtime.d.ts";
Deno.serve(() => new Response(
  JSON.stringify({ message: "This diagnostic is retired. Key issue was resolved Apr 17, 2026.", deprecated: true }),
  { headers: { "Content-Type": "application/json" } }
));
