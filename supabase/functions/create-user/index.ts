import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
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

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

    if (!supabaseUrl || !serviceRoleKey) {
      return json(500, { error: "Variables Supabase manquantes" });
    }

    const authHeader = req.headers.get("Authorization");
    if (!authHeader) {
      return json(401, { error: "Authorization manquante" });
    }

    const adminClient = createClient(supabaseUrl, serviceRoleKey, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const token = authHeader.replace("Bearer ", "").trim();
    const {
      data: { user: caller },
      error: callerError,
    } = await adminClient.auth.getUser(token);

    if (callerError || !caller) {
      return json(401, { error: "Session invalide" });
    }

    const { data: callerProfile, error: callerProfileError } = await adminClient
      .from("profiles")
      .select("role")
      .eq("id", caller.id)
      .maybeSingle();

    if (callerProfileError || callerProfile?.role !== "admin") {
      return json(403, { error: "Accès réservé à l'administrateur" });
    }

    const payload = (await req.json()) as CreateUserPayload;
    const email = String(payload.email || "").trim().toLowerCase();
    const password = String(payload.password || "");
    const fullName = String(payload.full_name || "").trim();
    const role = payload.role;
    const company = payload.company;
    const appUid = String(payload.app_uid || "").trim().toLowerCase();

    if (!email || !email.includes("@")) {
      return json(400, { error: "Email invalide" });
    }
    if (!password || password.length < 3) {
      return json(400, { error: "Mot de passe trop court" });
    }
    if (!fullName) {
      return json(400, { error: "Nom complet manquant" });
    }

    const allowedRoles = ["admin", "directeur_co", "commercial", "assistante", "metreur"];
    const allowedCompanies = ["nemausus", "lambert", "les-deux"];
    if (!allowedRoles.includes(role)) {
      return json(400, { error: "Rôle invalide" });
    }
    if (!allowedCompanies.includes(company)) {
      return json(400, { error: "Société invalide" });
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
          app_uid: appUid,
        },
      });

      if (createError || !created.user) {
        return json(400, { error: createError?.message || "Impossible de créer l'utilisateur" });
      }

      userId = created.user.id;
    } else {
      const { error: updateError } = await adminClient.auth.admin.updateUserById(userId, {
        password,
        user_metadata: {
          full_name: fullName,
          role,
          company,
          app_uid: appUid,
        },
      });

      if (updateError) {
        return json(400, { error: updateError.message || "Impossible de mettre à jour l'utilisateur" });
      }
    }

    const { error: profileError } = await adminClient.from("profiles").upsert(
      {
        id: userId,
        email,
        full_name: fullName,
        role,
        company,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "id" },
    );

    if (profileError) {
      return json(400, { error: profileError.message || "Impossible de créer le profil" });
    }

    return json(200, {
      ok: true,
      user_id: userId,
      email,
      full_name: fullName,
      role,
      company,
    });
  } catch (error) {
    return json(500, {
      error: error instanceof Error ? error.message : "Erreur inconnue",
    });
  }
});
