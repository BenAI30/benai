-- =============================================================================
-- AVANT DE CLIQUER SUR « RUN » : LIRE (30 secondes)
-- =============================================================================
-- Supabase peut afficher une fenêtre « Problème potentiel… » sur public.profiles.
-- Ce n’est PAS un bug de BenAI : c’est un garde-fou du tableau de bord.
--
-- Si cette fenêtre s’affiche, vous devez cliquer UNIQUEMENT sur le bouton VERT :
--   « Exécuter et activer RLS »  (ou équivalent en anglais).
-- Ne cliquez PAS sur « Courir sans RLS » (marron) — la sécurité resterait désactivée.
-- « Annuler » = le script ne s’applique pas : dans ce cas, recollez le script et Run → vert.
--
-- Ce script ne supprime rien. Il corrige :
--   1) erreur 500 sur /profiles (récursion RLS dans les helpers),
--   2) erreur 404 sur rpc/ensure_my_profile (fonction absente ou cache API),
-- puis recharge le cache PostgREST.
-- =============================================================================

-- ========= Helpers (évite récursion RLS sur public.profiles) =========
create or replace function public.current_profile_role()
returns text language sql stable security definer set search_path = public as $$
  select role from public.profiles where id = auth.uid() limit 1
$$;

create or replace function public.current_profile_company()
returns text language sql stable security definer set search_path = public as $$
  select company from public.profiles where id = auth.uid() limit 1
$$;

create or replace function public.current_auth_uid_slug()
returns text language sql stable security definer set search_path = public, auth as $$
  select lower(split_part(coalesce((select email from auth.users where id = auth.uid()),''),'@',1))
$$;

create or replace function public.current_profile_app_uid()
returns text language sql stable security definer set search_path = public, auth as $$
  select coalesce(
    nullif((select app_uid from public.profiles where id = auth.uid() limit 1),''),
    nullif((select lower(split_part(email,'@',1)) from public.profiles where id = auth.uid() limit 1),''),
    lower(split_part(coalesce((select email from auth.users where id = auth.uid()),''),'@',1))
  )
$$;

alter table if exists public.profiles enable row level security;

create or replace function public.ensure_my_profile()
returns void
language plpgsql
security definer
set search_path = public, auth
as $$
declare
  meta jsonb;
  full_name_val text;
  role_val text;
  company_val text;
  base_uid text;
  safe_uid text;
  uemail text;
begin
  -- PostgREST appelle la RPC avec le JWT utilisateur ; sans ceci, l’INSERT peut être bloqué par la RLS selon le propriétaire de la fonction.
  set local row_security = off;
  for au in select * from auth.users where id = auth.uid() loop
    uemail := lower(coalesce(au.email, ''));
    meta := coalesce(au.raw_user_meta_data, '{}'::jsonb);

    full_name_val := nullif(trim(coalesce(meta->>'full_name', '')), '');
    if full_name_val is null then
      full_name_val := split_part(coalesce(uemail, ''), '@', 1);
    end if;
    if full_name_val is null or full_name_val = '' then
      full_name_val := 'Utilisateur';
    end if;

    role_val := lower(coalesce(meta->>'role', 'assistante'));
    if role_val not in ('admin','directeur_co','directeur_general','commercial','assistante','metreur') then
      role_val := 'assistante';
    end if;

    company_val := lower(coalesce(meta->>'company', 'nemausus'));
    if company_val not in ('nemausus','lambert','les-deux') then
      company_val := 'nemausus';
    end if;

    base_uid := lower(coalesce(nullif(trim(coalesce(meta->>'app_uid', '')), ''), split_part(coalesce(uemail, ''), '@', 1), 'user'));
    safe_uid := regexp_replace(base_uid, '[^a-z0-9_]+', '_', 'g');
    if safe_uid = '' then
      safe_uid := 'user';
    end if;
    if exists (select 1 from public.profiles p where p.app_uid = safe_uid and p.id <> au.id) then
      safe_uid := safe_uid || '_' || substr(replace(au.id::text, '-', ''), 1, 6);
    end if;

    insert into public.profiles (id, email, app_uid, full_name, role, company)
    values (au.id, uemail, safe_uid, full_name_val, role_val, company_val)
    on conflict (id) do update set
      email = excluded.email,
      app_uid = excluded.app_uid,
      full_name = excluded.full_name,
      role = excluded.role,
      company = excluded.company,
      updated_at = now();

    return;
  end loop;
end;
$$;

grant execute on function public.ensure_my_profile() to authenticated;

-- Recharge le cache schéma PostgREST (sinon erreur « function … not in schema cache » quelques secondes).
notify pgrst, 'reload schema';
