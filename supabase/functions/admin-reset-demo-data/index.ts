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

function isUuid(s: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(s || "").trim(),
  );
}

function randomPassword(): string {
  const upper = "ABCDEFGHJKLMNPQRSTUVWXYZ";
  const lower = "abcdefghijkmnpqrstuvwxyz";
  const num = "23456789";
  const spe = "!@#$%&*";
  const pick = (set: string) => set[Math.floor(Math.random() * set.length)];
  const all = upper + lower + num + spe;
  let out = pick(upper) + pick(lower) + pick(num) + pick(spe);
  for (let i = 0; i < 10; i++) out += all[Math.floor(Math.random() * all.length)];
  return out;
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

type AuthUserRow = { id: string; email: string };

async function listAllAuthUsers(
  adminClient: ReturnType<typeof createClient>,
): Promise<AuthUserRow[]> {
  const rows: AuthUserRow[] = [];
  let page = 1;
  const perPage = 200;
  while (true) {
    const { data, error } = await adminClient.auth.admin.listUsers({ page, perPage });
    if (error) throw new Error(error.message || "listUsers");
    const users = data?.users ?? [];
    for (const u of users) {
      rows.push({ id: String(u.id), email: String(u.email ?? "").trim() });
    }
    if (users.length < perPage) break;
    page++;
  }
  return rows;
}

function cleanSharedCoreData(
  data: Record<string, unknown>,
  authIdSet: Set<string>,
  extraAllowedFrom: Set<string>,
): Record<string, unknown> {
  const allowedFrom = new Set([...authIdSet, ...extraAllowedFrom]);

  const messagesRaw = data.messages;
  const messages: Record<string, unknown[]> =
    messagesRaw && typeof messagesRaw === "object" && !Array.isArray(messagesRaw)
      ? (messagesRaw as Record<string, unknown[]>)
      : {};

  const cleanedMessages: Record<string, unknown[]> = {};
  for (const [cid, arr] of Object.entries(messages)) {
    const list = Array.isArray(arr) ? arr : [];
    const kept = list.filter((msg) => {
      if (!msg || typeof msg !== "object") return false;
      const from = String((msg as Record<string, unknown>).from ?? "").trim();
      if (!from) return true;
      if (isUuid(from) && !allowedFrom.has(from)) return false;
      return true;
    });
    if (kept.length) cleanedMessages[cid] = kept;
  }

  const readRaw = data.msg_read_cursor;
  const read: Record<string, Record<string, unknown>> =
    readRaw && typeof readRaw === "object" && !Array.isArray(readRaw)
      ? (readRaw as Record<string, Record<string, unknown>>)
      : {};

  const cleanedRead: Record<string, Record<string, unknown>> = {};
  for (const [cid, cursors] of Object.entries(read)) {
    if (!cursors || typeof cursors !== "object" || Array.isArray(cursors)) continue;
    const inner: Record<string, unknown> = {};
    for (const [uid, v] of Object.entries(cursors)) {
      if (isUuid(uid) && !authIdSet.has(uid)) continue;
      inner[uid] = v;
    }
    if (Object.keys(inner).length) cleanedRead[cid] = inner;
  }

  const feedRaw = data.notif_feed;
  const feed = Array.isArray(feedRaw) ? feedRaw : [];
  const cleanedFeed = feed.filter((item) => {
    if (!item || typeof item !== "object") return false;
    const t = String((item as Record<string, unknown>).target_uid ?? "").trim();
    if (!t || t === "all") return true;
    if (isUuid(t) && !authIdSet.has(t)) return false;
    return true;
  });

  return {
    ...data,
    messages: cleanedMessages,
    msg_read_cursor: cleanedRead,
    notif_feed: cleanedFeed,
  };
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

    const { error: rpcError } = await adminClient.rpc("admin_cleanup_orphan_refs", {
      p_caller: caller.id,
    });
    if (rpcError) {
      return json(400, {
        error: rpcError.message ||
          "RPC admin_cleanup_orphan_refs : exécutez supabase/patch_admin_reset_demo_data_rpc.sql sur le projet.",
      }, origin);
    }

    const authUsers = await listAllAuthUsers(adminClient);
    const authIds = authUsers.map((u) => u.id);
    const authIdSet = new Set(authIds);

    const { data: profRows } = await adminClient.from("profiles").select("app_uid");
    const extraFrom = new Set<string>();
    for (const row of profRows ?? []) {
      const u = String((row as { app_uid?: string }).app_uid ?? "").trim().toLowerCase();
      if (u) extraFrom.add(u);
    }
    extraFrom.add("benjamin");

    const { data: settingsRow, error: setErr } = await adminClient
      .from("app_settings")
      .select("value")
      .eq("key", "shared_core_data_v1")
      .maybeSingle();

    if (setErr) {
      return json(400, { error: setErr.message || "Lecture app_settings impossible" }, origin);
    }

    const root = (settingsRow?.value ?? {}) as Record<string, unknown>;
    const innerData =
      root && typeof root === "object" && "data" in root && root.data && typeof root.data === "object"
        ? ({ ...(root.data as Record<string, unknown>) } as Record<string, unknown>)
        : {};

    const cleanedData = cleanSharedCoreData(innerData, authIdSet, extraFrom);
    const newValue = {
      ...root,
      version: typeof root.version === "number" ? root.version : 1,
      updated_at: new Date().toISOString(),
      data: cleanedData,
    };

    const { error: upErr } = await adminClient.from("app_settings").upsert(
      {
        key: "shared_core_data_v1",
        value: newValue,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "key" },
    );
    if (upErr) {
      return json(400, { error: upErr.message || "Mise à jour shared_core_data_v1 impossible" }, origin);
    }

    const passwords: { user_id: string; email: string; new_password: string }[] = [];
    for (const u of authUsers) {
      if (u.id === caller.id) continue;
      const newPwd = randomPassword();
      const { error: pwErr } = await adminClient.auth.admin.updateUserById(u.id, {
        password: newPwd,
      });
      if (pwErr) {
        return json(400, {
          error: pwErr.message || `Mot de passe impossible pour ${u.email || u.id}`,
        }, origin);
      }
      passwords.push({ user_id: u.id, email: u.email, new_password: newPwd });
    }

    return json(200, {
      ok: true,
      passwords,
      message:
        "Références orphelines nettoyées, annuaire et comptes conservés, mots de passe régénérés (sauf le vôtre). Copiez la liste renvoyée une seule fois.",
    }, origin);
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    }, origin);
  }
});
