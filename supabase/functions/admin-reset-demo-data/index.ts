import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const baseCorsHeaders = {
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

type Body = { password?: string };

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

async function verifyPasswordGrant(
  supabaseUrl: string,
  anonKey: string,
  email: string,
  password: string,
): Promise<boolean> {
  const res = await fetch(`${supabaseUrl}/auth/v1/token?grant_type=password`, {
    method: "POST",
    headers: {
      apikey: anonKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ email, password }),
  });
  return res.ok;
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
      .select("role,email")
      .eq("id", caller.id)
      .maybeSingle();

    if (callerProfileError || callerProfile?.role !== "admin") {
      return json(403, { error: "Accès réservé à l'administrateur" }, origin);
    }

    let body: Body = {};
    try {
      body = (await req.json()) as Body;
    } catch {
      body = {};
    }
    const password = String(body.password ?? "");
    if (!password || password.length < 6) {
      return json(400, { error: "Mot de passe requis (compte cloud)" }, origin);
    }

    const { data: authUserWrap, error: authUserErr } = await adminClient.auth.admin.getUserById(
      caller.id,
    );
    if (authUserErr || !authUserWrap?.user) {
      return json(400, { error: "Compte auth introuvable" }, origin);
    }
    const email = String(authUserWrap.user.email || callerProfile?.email || caller.email || "")
      .trim()
      .toLowerCase();
    if (!email) {
      return json(400, { error: "Email du compte introuvable pour vérification" }, origin);
    }

    const pwdOk = await verifyPasswordGrant(supabaseUrl, anonKey, email, password);
    if (!pwdOk) {
      return json(401, { error: "Mot de passe administrateur incorrect" }, origin);
    }

    const { error: rpcError } = await adminClient.rpc("admin_reset_demo_data_truncate");
    if (rpcError) {
      return json(400, {
        error: rpcError.message ||
          "RPC admin_reset_demo_data_truncate : exécutez supabase/patch_admin_reset_demo_data_rpc.sql sur le projet.",
      }, origin);
    }

    const perPage = 200;
    let deleted = 0;
    let guard = 0;
    while (true) {
      if (++guard > 500) {
        return json(500, { error: "Arrêt sécurité : trop d'itérations sur la liste utilisateurs" }, origin);
      }
      const { data: listData, error: listErr } = await adminClient.auth.admin.listUsers({
        page: 1,
        perPage,
      });
      if (listErr) {
        return json(400, { error: listErr.message || "Liste utilisateurs impossible" }, origin);
      }
      const users = listData?.users ?? [];
      const victims = users.filter((u) => u.id !== caller.id);
      if (!victims.length) break;
      for (const u of victims) {
        const { error: delErr } = await adminClient.auth.admin.deleteUser(u.id);
        if (delErr) {
          return json(400, { error: delErr.message || "Suppression utilisateur impossible" }, origin);
        }
        deleted++;
      }
    }

    return json(200, { ok: true, deleted_users: deleted }, origin);
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    }, origin);
  }
});
