import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const baseCorsHeaders = {
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

type DeleteUserPayload = {
  app_uid?: string;
  email?: string;
  user_id?: string;
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

function normalizeAppUid(v: string): string {
  return String(v || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
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

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY") ?? "";

    if (!supabaseUrl || !serviceRoleKey || !anonKey) {
      return json(500, { error: "Variables Supabase manquantes" }, origin);
    }

    const authHeader = req.headers.get("Authorization");
    if (!authHeader) return json(401, { error: "Authorization manquante" }, origin);
    const token = authHeader.replace("Bearer ", "").trim();
    if (!token) return json(401, { error: "Authorization invalide" }, origin);

    const callerClient = createClient(supabaseUrl, anonKey, {
      auth: { persistSession: false, autoRefreshToken: false },
      global: { headers: { Authorization: `Bearer ${token}` } },
    });
    const {
      data: { user: caller },
      error: callerError,
    } = await callerClient.auth.getUser();

    if (callerError || !caller) return json(401, { error: "Session invalide" }, origin);

    const adminClient = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const { data: callerProfile, error: callerProfileError } = await adminClient
      .from("profiles")
      .select("role")
      .eq("id", caller.id)
      .maybeSingle();

    if (callerProfileError || callerProfile?.role !== "admin") {
      return json(403, { error: "Accès réservé à l'administrateur" }, origin);
    }

    const payload = (await req.json()) as DeleteUserPayload;
    const appUid = normalizeAppUid(payload.app_uid || "");
    const email = String(payload.email || "").trim().toLowerCase();
    const userId = String(payload.user_id || "").trim();

    let query = adminClient
      .from("profiles")
      .select("id,email,app_uid,role")
      .limit(1);

    if (userId) query = query.eq("id", userId);
    else if (appUid) query = query.eq("app_uid", appUid);
    else if (email) query = query.eq("email", email);
    else return json(400, { error: "Identifiant utilisateur manquant" }, origin);

    const { data: profile, error: profileErr } = await query.maybeSingle();
    if (profileErr) return json(400, { error: profileErr.message || "Recherche profil impossible" }, origin);
    if (!profile) return json(404, { error: "Utilisateur introuvable" }, origin);
    if (profile.id === caller.id) return json(400, { error: "Vous ne pouvez pas supprimer votre propre compte" }, origin);

    const stateKey = `user_state_${profile.app_uid || email.split("@")[0] || ""}`;
    if (stateKey !== "user_state_") {
      await adminClient.from("app_settings").delete().eq("key", stateKey);
    }

    const { error: authDeleteError } = await adminClient.auth.admin.deleteUser(profile.id);
    if (authDeleteError) {
      return json(400, { error: authDeleteError.message || "Suppression auth impossible" }, origin);
    }

    // Safety cleanup if profile row remains for any reason.
    await adminClient.from("profiles").delete().eq("id", profile.id);

    return json(200, { ok: true, deleted_id: profile.id, deleted_email: profile.email || "" }, origin);
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    }, origin);
  }
});

