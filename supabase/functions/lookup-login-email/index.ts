import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const baseCorsHeaders = {
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function getAllowedOrigins(): Set<string> {
  const raw = Deno.env.get("CORS_ALLOWED_ORIGINS") ?? "";
  const envOrigins = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const defaults = ["http://localhost:3000", "http://127.0.0.1:3000"];
  return new Set([...defaults, ...envOrigins]);
}

const allowedOrigins = getAllowedOrigins();

function isOriginAllowed(origin: string | null): boolean {
  if (!origin) return true;
  return allowedOrigins.has(origin);
}

function corsHeadersFor(origin: string | null): Record<string, string> {
  const headers: Record<string, string> = { ...baseCorsHeaders };
  if (origin && allowedOrigins.has(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers["Vary"] = "Origin";
  }
  return headers;
}

function json(status: number, body: unknown, origin: string | null) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeadersFor(origin), "Content-Type": "application/json" },
  });
}

/** Aligné sur l’écran BenAI (normalizeId). */
function normalizeId(s: string): string {
  return (s || "")
    .toLowerCase()
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, "_");
}

/** Aligné sur create-user (sanitizeAppUid). */
function sanitizeAppUid(seed: string): string {
  const cleaned = String(seed || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return cleaned || "user";
}

function loginLookupKey(raw: string): string {
  return sanitizeAppUid(normalizeId(raw));
}

Deno.serve(async (req) => {
  const origin = req.headers.get("Origin");

  if (req.method === "OPTIONS") {
    if (!isOriginAllowed(origin)) {
      return new Response("CORS origin denied", {
        status: 403,
        headers: { ...baseCorsHeaders, "Content-Type": "text/plain" },
      });
    }
    return new Response("ok", { headers: corsHeadersFor(origin) });
  }

  if (!isOriginAllowed(origin)) {
    return json(403, { error: "Origin non autorisee" }, origin);
  }

  if (req.method !== "POST") {
    return json(405, { error: "Method not allowed" }, origin);
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY") ?? "";

    if (!supabaseUrl || !serviceRoleKey || !anonKey) {
      return json(500, { error: "Variables Supabase manquantes" }, origin);
    }

    const apikey = req.headers.get("apikey") ?? "";
    const auth = req.headers.get("Authorization")?.replace("Bearer ", "").trim() ?? "";
    if (apikey !== anonKey && auth !== anonKey) {
      return json(401, { error: "Cle anon invalide" }, origin);
    }

    const body = (await req.json()) as { login?: string };
    const raw = String(body.login ?? "").trim();
    if (!raw) {
      return json(400, { error: "login_manquant" }, origin);
    }
    if (raw.includes("@")) {
      return json(400, { error: "utiliser_email_directement" }, origin);
    }
    if (raw.length > 120) {
      return json(400, { error: "login_trop_long" }, origin);
    }

    const key = loginLookupKey(raw);
    if (!key || key === "user") {
      return json(404, { error: "not_found" }, origin);
    }

    const adminClient = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const { data, error } = await adminClient
      .from("profiles")
      .select("email")
      .eq("app_uid", key)
      .maybeSingle();

    if (error || !data?.email) {
      return json(404, { error: "not_found" }, origin);
    }

    const email = String(data.email).trim().toLowerCase();
    if (!email.includes("@")) {
      return json(404, { error: "not_found" }, origin);
    }

    return json(200, { email }, origin);
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    }, origin);
  }
});
