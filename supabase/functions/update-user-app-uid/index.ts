import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const baseCorsHeaders = {
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

type Body = {
  target_user_id?: string;
  current_app_uid?: string;
  new_app_uid: string;
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

function isUuid(v: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(v);
}

async function ensureUniqueAppUid(
  adminClient: ReturnType<typeof createClient>,
  seed: string,
  userId: string,
): Promise<string> {
  const base = normalizeAppUid(seed);
  let candidate = base || "user";
  for (let i = 0; i < 20; i++) {
    const { data, error } = await adminClient
      .from("profiles")
      .select("id")
      .eq("app_uid", candidate)
      .maybeSingle();
    if (error) return candidate;
    if (!data || data.id === userId) return candidate;
    const suffix = userId.replaceAll("-", "").slice(0, 6 + i);
    candidate = `${base}_${suffix}`;
  }
  return `${base}_${crypto.randomUUID().replaceAll("-", "").slice(0, 8)}`;
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

    const payload = (await req.json()) as Body;
    const newRaw = String(payload.new_app_uid || "").trim();
    const newBase = normalizeAppUid(newRaw);
    if (!newBase || newBase === "user") {
      return json(400, { error: "Identifiant invalide" }, origin);
    }
    const reserved = new Set(["benjamin", "benai", "admin"]);
    if (reserved.has(newBase)) {
      return json(400, { error: "Identifiant réservé" }, origin);
    }

    let targetId = String(payload.target_user_id || "").trim();
    const curUid = normalizeAppUid(String(payload.current_app_uid || ""));

    let query = adminClient.from("profiles").select("id,email,app_uid,full_name,role,company").limit(1);
    if (targetId && isUuid(targetId)) {
      query = query.eq("id", targetId);
    } else if (curUid) {
      query = query.eq("app_uid", curUid);
    } else {
      return json(400, { error: "Cible manquante (target_user_id ou current_app_uid)" }, origin);
    }

    const { data: profile, error: profileErr } = await query.maybeSingle();
    if (profileErr) return json(400, { error: profileErr.message || "Profil introuvable" }, origin);
    if (!profile?.id) return json(404, { error: "Utilisateur introuvable" }, origin);

    targetId = profile.id;
    const oldUid = normalizeAppUid(String(profile.app_uid || ""));
    if (oldUid === "benjamin") {
      return json(400, { error: "Impossible de renommer ce compte" }, origin);
    }

    const resolved = await ensureUniqueAppUid(adminClient, newBase, targetId);
    if (resolved === oldUid) {
      return json(200, { ok: true, app_uid: resolved, unchanged: true }, origin);
    }

    const { error: upProf } = await adminClient
      .from("profiles")
      .update({ app_uid: resolved, updated_at: new Date().toISOString() })
      .eq("id", targetId);
    if (upProf) return json(400, { error: upProf.message || "Mise à jour profil impossible" }, origin);

    const { error: metaErr } = await adminClient.auth.admin.updateUserById(targetId, {
      user_metadata: {
        full_name: profile.full_name,
        role: profile.role,
        company: profile.company,
        app_uid: resolved,
      },
    });
    if (metaErr) {
      return json(400, { error: metaErr.message || "Mise à jour auth impossible" }, origin);
    }

    return json(200, { ok: true, user_id: targetId, app_uid: resolved, previous_app_uid: oldUid }, origin);
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    }, origin);
  }
});
