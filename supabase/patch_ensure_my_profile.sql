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
--
-- Exécutez ce script en entier (Ctrl+A puis Run). La fonction ensure_my_profile est en
-- LANGUAGE sql (une seule commande INSERT) : plus de variables PL/pgSQL mal interprétées.
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
language sql
security definer
set search_path = public, auth
as $fn$
insert into public.profiles (id, email, app_uid, full_name, role, company)
with au as (
  select id, email, raw_user_meta_data
  from auth.users
  where id = auth.uid()
),
s1 as (
  select
    au.id,
    lower(coalesce(au.email, '')) as email_l,
    coalesce(au.raw_user_meta_data, '{}'::jsonb) as ju
  from au
),
s2 as (
  select
    s1.id,
    s1.email_l,
    case
      when nullif(trim(s1.ju->>'full_name'), '') is not null then nullif(trim(s1.ju->>'full_name'), '')
      when nullif(split_part(s1.email_l, '@', 1), '') is not null then split_part(s1.email_l, '@', 1)
      else 'Utilisateur'
    end as full_name_out,
    case
      when lower(coalesce(s1.ju->>'role', 'assistante')) in (
        'admin','directeur_co','directeur_general','commercial','assistante','metreur'
      ) then lower(coalesce(s1.ju->>'role', 'assistante'))
      else 'assistante'
    end as role_out,
    case
      when lower(coalesce(s1.ju->>'company', 'nemausus')) in ('nemausus','lambert','les-deux')
      then lower(coalesce(s1.ju->>'company', 'nemausus'))
      else 'nemausus'
    end as company_out,
    lower(coalesce(
      nullif(trim(s1.ju->>'app_uid'), ''),
      nullif(split_part(s1.email_l, '@', 1), ''),
      'user'
    )) as base_uid_raw
  from s1
),
s3 as (
  select
    s2.id,
    s2.email_l,
    s2.full_name_out,
    s2.role_out,
    s2.company_out,
    coalesce(nullif(regexp_replace(s2.base_uid_raw, '[^a-z0-9_]+', '_', 'g'), ''), 'user') as slug_base
  from s2
),
s4 as (
  select
    s3.id,
    s3.email_l,
    s3.full_name_out,
    s3.role_out,
    s3.company_out,
    case
      when exists (
        select 1 from public.profiles p
        where p.app_uid = s3.slug_base and p.id <> s3.id
      )
      then s3.slug_base || '_' || substr(replace(s3.id::text, '-', ''), 1, 6)
      else s3.slug_base
    end as app_uid_out
  from s3
)
select id, email_l, app_uid_out, full_name_out, role_out, company_out from s4
on conflict (id) do update set
  email = excluded.email,
  app_uid = excluded.app_uid,
  full_name = excluded.full_name,
  role = excluded.role,
  company = excluded.company,
  updated_at = now();
$fn$;

grant execute on function public.ensure_my_profile() to authenticated;

-- Recharge le cache schéma PostgREST (sinon erreur « function … not in schema cache » quelques secondes).
notify pgrst, 'reload schema';
