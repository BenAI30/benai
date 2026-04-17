import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const baseCorsHeaders = {
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

type CreateUserPayload = {
  email: string;
  password: string;
  full_name: string;
  role: "admin" | "directeur_co" | "commercial" | "assistante" | "metreur";
  company: "nemausus" | "lambert" | "les-deux";
  app_uid?: string;
};

function sanitizeAppUid(seed: string): string {
  const cleaned = String(seed || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return cleaned || "user";
}

async function ensureUniqueAppUid(
  adminClient: ReturnType<typeof createClient>,
  seed: string,
  userId: string,
): Promise<string> {
  const base = sanitizeAppUid(seed);
  let candidate = base;
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

function getAllowedOrigins(): Set<string> {
  const raw = Deno.env.get("CORS_ALLOWED_ORIGINS") ?? "";
  const envOrigins = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  // Local dev defaults only; production domains must be listed in env.
  const defaults = ["http://localhost:3000", "http://127.0.0.1:3000"];
  return new Set([...defaults, ...envOrigins]);
}

const allowedOrigins = getAllowedOrigins();

function isOriginAllowed(origin: string | null): boolean {
  // Non-browser clients usually send no Origin header.
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
    if (!authHeader) {
      return json(401, { error: "Authorization manquante" }, origin);
    }

    const token = authHeader.replace("Bearer ", "").trim();
    if (!token) {
      return json(401, { error: "Authorization invalide" }, origin);
    }

    // Verify caller session with anon client (compatible with asymmetric JWT algs, e.g. ES256).
    const callerClient = createClient(supabaseUrl, anonKey, {
      auth: { persistSession: false, autoRefreshToken: false },
      global: { headers: { Authorization: `Bearer ${token}` } },
    });
    const {
      data: { user: caller },
      error: callerError,
    } = await callerClient.auth.getUser();

    if (callerError || !caller) {
      return json(401, { error: "Session invalide" }, origin);
    }

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

    const payload = (await req.json()) as CreateUserPayload;
    const email = String(payload.email || "").trim().toLowerCase();
    const password = String(payload.password || "");
    const fullName = String(payload.full_name || "").trim();
    const role = payload.role;
    const company = payload.company;
    const requestedAppUid = sanitizeAppUid(String(payload.app_uid || "").trim().toLowerCase() || email.split("@")[0] || "user");

    if (!email || !email.includes("@")) {
      return json(400, { error: "Email invalide" }, origin);
    }
    if (!password || password.length < 3) {
      return json(400, { error: "Mot de passe trop court" }, origin);
    }
    if (!fullName) {
      return json(400, { error: "Nom complet manquant" }, origin);
    }

    const allowedRoles = ["admin", "directeur_co", "commercial", "assistante", "metreur"];
    const allowedCompanies = ["nemausus", "lambert", "les-deux"];
    if (!allowedRoles.includes(role)) {
      return json(400, { error: "Rôle invalide" }, origin);
    }
    if (!allowedCompanies.includes(company)) {
      return json(400, { error: "Société invalide" }, origin);
    }

    const existingAuth = await adminClient.auth.admin.listUsers({ page: 1, perPage: 1000 });
    const existingUser = existingAuth.data.users.find((u) => (u.email || "").toLowerCase() === email);

    let userId = existingUser?.id || "";

    if (!userId) {
      const { data: created, error: createError } = await adminClient.auth.admin.createUser({
        email,
        password,
        email_confirm: true,
        user_metadata: {
          full_name: fullName,
          role,
          company,
          app_uid: requestedAppUid,
        },
      });

      if (createError || !created.user) {
        return json(400, { error: createError?.message || "Impossible de créer l'utilisateur" }, origin);
      }

      userId = created.user.id;
    } else {
      const { error: updateError } = await adminClient.auth.admin.updateUserById(userId, {
        password,
        user_metadata: {
          full_name: fullName,
          role,
          company,
          app_uid: requestedAppUid,
        },
      });

      if (updateError) {
        return json(400, { error: updateError.message || "Impossible de mettre à jour l'utilisateur" }, origin);
      }
    }

    const resolvedAppUid = await ensureUniqueAppUid(adminClient, requestedAppUid, userId);
    const { error: metadataUpdateError } = await adminClient.auth.admin.updateUserById(userId, {
      user_metadata: {
        full_name: fullName,
        role,
        company,
        app_uid: resolvedAppUid,
      },
    });
    if (metadataUpdateError) {
      return json(400, { error: metadataUpdateError.message || "Impossible de finaliser le profil utilisateur" }, origin);
    }

    const { error: profileError } = await adminClient.from("profiles").upsert(
      {
        id: userId,
        email,
        app_uid: resolvedAppUid,
        full_name: fullName,
        role,
        company,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "id" },
    );

    if (profileError) {
      return json(400, { error: profileError.message || "Impossible de créer le profil" }, origin);
    }

    return json(200, {
      ok: true,
      user_id: userId,
      email,
      full_name: fullName,
      role,
      company,
    }, origin);
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    }, origin);
  }
});
