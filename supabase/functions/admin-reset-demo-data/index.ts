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

/** Repasse une ligne SQL `annuaire` vers la forme attendue par l’app (shared_core / localStorage). */
function clientAnnuaireFromDbRow(row: Record<string, unknown>, syncBase: number, idx: number): Record<string, unknown> {
  const pl =
    row.payload && typeof row.payload === "object" && !Array.isArray(row.payload)
      ? (row.payload as Record<string, unknown>)
      : {};
  const idNum = Number(pl.id ?? row.id);
  const id = Number.isFinite(idNum) && idNum > 0 ? idNum : Number(row.id) || syncBase + idx;
  return {
    ...pl,
    id,
    prenom: String(row.prenom ?? pl.prenom ?? ""),
    nom: String(row.nom ?? pl.nom ?? ""),
    email: String(row.email ?? pl.email ?? ""),
    emailPro: String(row.email_pro ?? pl.emailPro ?? ""),
    tel: String(row.tel ?? pl.tel ?? ""),
    naissance: row.naissance ?? pl.naissance ?? "",
    fonction: String(row.fonction ?? pl.fonction ?? "Autre"),
    societe: String(row.societe ?? pl.societe ?? "nemausus"),
    sync_ts: syncBase + idx,
  };
}

/** Retire des snapshots cloud `user_state_*` tout ce qui alimente pilotage / ventes / stats (hors annuaire métier en table). */
async function stripCrmStatsFromAllUserStateSnapshots(
  adminClient: ReturnType<typeof createClient>,
): Promise<number> {
  const exactKeys = [
    "benai_obj_comm",
    "benai_obj_comm_mois",
    "benai_lead_obj",
    "benai_lead_obj_soc_mois",
    "benai_ventes_mois",
    "benai_connexions",
    "benai_commercial_archives",
    "benai_projets_sugg",
    "benai_pending_user_deletes",
    "benai_pending_user_creates",
  ];
  const prefixStrip = [
    "benai_notifs_",
    "benai_weekly_",
    "benai_briefing_",
    "benai_rr_idx_",
    "benai_last_motiv_",
    "benai_tuto_done_",
    "benai_anniv_seen_",
  ];
  let updatedRows = 0;
  let from = 0;
  const pageSize = 300;
  while (true) {
    const { data: rows, error } = await adminClient
      .from("app_settings")
      .select("key, value")
      .like("key", "user_state_%")
      .order("key", { ascending: true })
      .range(from, from + pageSize - 1);
    if (error) throw new Error(error.message || "app_settings user_state list");
    const chunk = rows ?? [];
    for (const row of chunk) {
      const rowKey = String((row as { key?: string }).key ?? "");
      const val = (row as { value?: unknown }).value;
      if (!rowKey.startsWith("user_state_")) continue;
      if (!val || typeof val !== "object" || Array.isArray(val)) continue;
      const next: Record<string, unknown> = { ...(val as Record<string, unknown>) };
      let touched = false;
      for (const ek of exactKeys) {
        if (Object.prototype.hasOwnProperty.call(next, ek)) {
          delete next[ek];
          touched = true;
        }
      }
      for (const pk of Object.keys(next)) {
        if (prefixStrip.some((p) => pk.startsWith(p))) {
          delete next[pk];
          touched = true;
        }
      }
      if (!touched) continue;
      const { error: upErr } = await adminClient.from("app_settings").upsert(
        {
          key: rowKey,
          value: next,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "key" },
      );
      if (upErr) throw new Error(upErr.message || `upsert ${rowKey}`);
      updatedRows++;
    }
    if (chunk.length < pageSize) break;
    from += pageSize;
  }
  return updatedRows;
}

async function fetchAllAnnuaireRows(
  adminClient: ReturnType<typeof createClient>,
): Promise<Record<string, unknown>[]> {
  const out: Record<string, unknown>[] = [];
  const pageSize = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await adminClient
      .from("annuaire")
      .select("*")
      .order("id", { ascending: true })
      .range(from, from + pageSize - 1);
    if (error) throw new Error(error.message || "annuaire select");
    const chunk = data ?? [];
    out.push(...chunk);
    if (chunk.length < pageSize) break;
    from += pageSize;
  }
  return out;
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

    const { error: rpcError } = await adminClient.rpc("admin_wipe_benai_keep_annuaire", {
      p_caller: caller.id,
    });
    if (rpcError) {
      return json(400, {
        error: rpcError.message ||
          "RPC admin_wipe_benai_keep_annuaire : exécutez supabase/patch_admin_reset_demo_data_rpc.sql sur le projet.",
      }, origin);
    }

    const dbRows = await fetchAllAnnuaireRows(adminClient);
    const syncBase = Date.now();
    const annuaire = dbRows.map((r, i) => clientAnnuaireFromDbRow(r, syncBase, i));

    const newValue = {
      version: 1,
      updated_at: new Date().toISOString(),
      data: {
        sav: [],
        notes: [],
        absences: [],
        annuaire,
        leads: [],
        notif_feed: [],
        messages: {},
        msg_deletions: {},
        msg_read_cursor: {},
      },
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

    let userStatesScrubbed = 0;
    try {
      userStatesScrubbed = await stripCrmStatsFromAllUserStateSnapshots(adminClient);
    } catch (e) {
      return json(400, {
        error: e instanceof Error ? e.message : "Nettoyage user_state impossible",
      }, origin);
    }

    return json(200, {
      ok: true,
      annuaire,
      annuaire_count: annuaire.length,
      user_states_scrubbed: userStatesScrubbed,
      message:
        "Leads, SAV, notes, absences, messages, snapshots benai_state, miroir partagé et stats/ventes/objectifs dans user_state_* ont été effacés ; comptes et table annuaire conservés.",
    }, origin);
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    }, origin);
  }
});
