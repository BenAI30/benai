-- BenAI - Base security model for Supabase (RLS)
-- Execute in Supabase SQL Editor.

create extension if not exists pgcrypto;

-- =========
-- PROFILES
-- =========
create table if not exists public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text unique,
  app_uid text unique,
  full_name text not null,
  role text not null check (role in ('admin','directeur_co','directeur_general','commercial','assistante','metreur')),
  company text not null check (company in ('nemausus','lambert','les-deux')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
alter table public.profiles add column if not exists app_uid text;

-- Bases déjà déployées : élargir le CHECK role (idempotent).
alter table public.profiles drop constraint if exists profiles_role_check;
alter table public.profiles add constraint profiles_role_check
  check (role in ('admin','directeur_co','directeur_general','commercial','assistante','metreur'));

-- =========
-- LEADS
-- =========
create table if not exists public.leads (
  id bigserial primary key,
  societe_crm text not null check (societe_crm in ('nemausus','lambert')),
  nom text not null,
  telephone text,
  ville text,
  cp text,
  type_projet text,
  statut text not null default 'gris' check (statut in ('gris','rdv','jaune','vert','rouge')),
  raison_mort text,
  commercial_user_id uuid references public.profiles(id) on delete set null,
  created_by uuid not null references public.profiles(id) on delete restrict,
  archive boolean not null default false,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- =========
-- SAV
-- =========
create table if not exists public.sav (
  id bigserial primary key,
  legacy_id text unique,
  societe text not null check (societe in ('nemausus','lambert')),
  client text not null,
  probleme text not null,
  statut text not null default 'nouveau' check (statut in ('nouveau','en_cours','regle')),
  urgent boolean not null default false,
  archive boolean not null default false,
  mute_reminder boolean not null default false,
  commentaire text,
  rappel_date date,
  payload jsonb not null default '{}'::jsonb,
  created_by uuid references public.profiles(id) on delete restrict default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- =========
-- NOTES
-- =========
create table if not exists public.notes (
  id bigserial primary key,
  legacy_id text unique,
  text text not null default '',
  author_uid text,
  target_uid text default 'all',
  ts bigint,
  payload jsonb not null default '{}'::jsonb,
  created_by uuid references public.profiles(id) on delete set null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.notes add column if not exists legacy_id text;
alter table public.notes add column if not exists text text default '';
alter table public.notes add column if not exists author_uid text;
alter table public.notes add column if not exists target_uid text default 'all';
alter table public.notes add column if not exists ts bigint;
alter table public.notes add column if not exists payload jsonb default '{}'::jsonb;
alter table public.notes add column if not exists created_by uuid references public.profiles(id) on delete set null;
alter table public.notes add column if not exists created_at timestamptz default now();
alter table public.notes add column if not exists updated_at timestamptz default now();
create unique index if not exists idx_notes_legacy_id on public.notes(legacy_id) where legacy_id is not null;

-- =========
-- ABSENCES
-- =========
create table if not exists public.absences (
  id bigserial primary key,
  legacy_id text unique,
  employe text not null,
  debut date not null,
  fin date not null,
  type text not null default 'Congé',
  note text,
  heure_debut text,
  heure_fin text,
  notifs jsonb not null default '[]'::jsonb,
  payload jsonb not null default '{}'::jsonb,
  created_by uuid references public.profiles(id) on delete set null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.absences add column if not exists legacy_id text;
alter table public.absences add column if not exists employe text;
alter table public.absences add column if not exists debut date;
alter table public.absences add column if not exists fin date;
alter table public.absences add column if not exists type text default 'Congé';
alter table public.absences add column if not exists note text;
alter table public.absences add column if not exists heure_debut text;
alter table public.absences add column if not exists heure_fin text;
alter table public.absences add column if not exists notifs jsonb default '[]'::jsonb;
alter table public.absences add column if not exists payload jsonb default '{}'::jsonb;
alter table public.absences add column if not exists created_by uuid references public.profiles(id) on delete set null;
alter table public.absences add column if not exists created_at timestamptz default now();
alter table public.absences add column if not exists updated_at timestamptz default now();
create unique index if not exists idx_absences_legacy_id on public.absences(legacy_id) where legacy_id is not null;

-- =========
-- ANNUAIRE
-- =========
create table if not exists public.annuaire (
  id bigserial primary key,
  legacy_id text unique,
  prenom text not null,
  nom text not null,
  email text,
  email_pro text,
  tel text,
  naissance date,
  fonction text,
  societe text not null check (societe in ('nemausus','lambert')),
  payload jsonb not null default '{}'::jsonb,
  created_by uuid references public.profiles(id) on delete set null default auth.uid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.annuaire add column if not exists legacy_id text;
alter table public.annuaire add column if not exists prenom text;
alter table public.annuaire add column if not exists nom text;
alter table public.annuaire add column if not exists email text;
alter table public.annuaire add column if not exists email_pro text;
alter table public.annuaire add column if not exists tel text;
alter table public.annuaire add column if not exists naissance date;
alter table public.annuaire add column if not exists fonction text;
alter table public.annuaire add column if not exists societe text;
alter table public.annuaire add column if not exists payload jsonb default '{}'::jsonb;
alter table public.annuaire add column if not exists created_by uuid references public.profiles(id) on delete set null;
alter table public.annuaire add column if not exists created_at timestamptz default now();
alter table public.annuaire add column if not exists updated_at timestamptz default now();
create unique index if not exists idx_annuaire_legacy_id on public.annuaire(legacy_id) where legacy_id is not null;

-- =========
-- SETTINGS (shared app keys, admin only write)
-- =========
create table if not exists public.app_settings (
  key text primary key,
  value jsonb not null,
  updated_by uuid references public.profiles(id) on delete set null,
  updated_at timestamptz not null default now()
);

insert into public.app_settings(key, value)
values ('shared_ai_api', jsonb_build_object('provider','anthropic','key',''))
on conflict (key) do nothing;

-- =========
-- Helpers
-- =========
-- SECURITY DEFINER : lecture profiles sans repasser par la RLS de profiles (sinon récursion → HTTP 500 sur GET /profiles).
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

create or replace function public.set_updated_at()
returns trigger language plpgsql set search_path = public as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create or replace function public.handle_new_auth_user()
returns trigger
language plpgsql
security definer
set search_path = public, auth
as $$
declare
  meta jsonb := coalesce(new.raw_user_meta_data, '{}'::jsonb);
  full_name_val text;
  role_val text;
  company_val text;
  base_uid text;
  safe_uid text;
begin
  full_name_val := nullif(trim(coalesce(meta->>'full_name', '')), '');
  if full_name_val is null then
    full_name_val := split_part(coalesce(new.email, ''), '@', 1);
  end if;

  role_val := lower(coalesce(meta->>'role', 'assistante'));
  if role_val not in ('admin','directeur_co','directeur_general','commercial','assistante','metreur') then
    role_val := 'assistante';
  end if;

  company_val := lower(coalesce(meta->>'company', 'nemausus'));
  if company_val not in ('nemausus','lambert','les-deux') then
    company_val := 'nemausus';
  end if;

  base_uid := lower(coalesce(nullif(trim(coalesce(meta->>'app_uid', '')), ''), split_part(coalesce(new.email, ''), '@', 1), 'user'));
  safe_uid := regexp_replace(base_uid, '[^a-z0-9_]+', '_', 'g');
  if safe_uid = '' then
    safe_uid := 'user';
  end if;
  if exists (select 1 from public.profiles p where p.app_uid = safe_uid and p.id <> new.id) then
    safe_uid := safe_uid || '_' || substr(replace(new.id::text, '-', ''), 1, 6);
  end if;

  insert into public.profiles(id, email, app_uid, full_name, role, company)
  values (new.id, lower(coalesce(new.email, '')), safe_uid, full_name_val, role_val, company_val)
  on conflict (id) do update set
    email = excluded.email,
    app_uid = excluded.app_uid,
    full_name = excluded.full_name,
    role = excluded.role,
    company = excluded.company,
    updated_at = now();

  return new;
end;
$$;

-- =========
-- Enable RLS
-- =========
alter table public.profiles enable row level security;
alter table public.leads enable row level security;
alter table public.sav enable row level security;
alter table public.notes enable row level security;
alter table public.absences enable row level security;
alter table public.annuaire enable row level security;
alter table public.app_settings enable row level security;

-- =========
-- PROFILES policies
-- =========
drop policy if exists "profiles_self_or_admin_select" on public.profiles;
create policy "profiles_self_or_admin_select"
on public.profiles for select
using (
  id = auth.uid()
  or public.current_profile_role() = 'admin'
  -- Même périmètre entreprise : pilotage CRM + terrain + assistantes + métreur (messagerie, listes, attribution).
  or (
    public.current_profile_role() in ('directeur_co','directeur_general','commercial','assistante','metreur')
    and (
      public.current_profile_company() = 'les-deux'
      or public.profiles.company = public.current_profile_company()
      or (
        public.current_profile_company() = 'nemausus'
        and public.profiles.company in ('nemausus','les-deux')
      )
      or (
        public.current_profile_company() = 'lambert'
        and public.profiles.company in ('lambert','les-deux')
      )
    )
  )
);

drop policy if exists "profiles_admin_write" on public.profiles;
create policy "profiles_admin_write"
on public.profiles for all
using (public.current_profile_role() = 'admin')
with check (public.current_profile_role() = 'admin');

-- =========
-- LEADS policies
-- =========
drop policy if exists "leads_select_policy" on public.leads;
create policy "leads_select_policy"
on public.leads for select
using (
  -- admin sees all
  public.current_profile_role() = 'admin'
  -- directeur_co / directeur_general sees only own company
  or (
    public.current_profile_role() in ('directeur_co','directeur_general')
    and (
      public.current_profile_company() = 'les-deux'
      or societe_crm = public.current_profile_company()
    )
  )
  -- commercial sees own assigned leads
  or (
    public.current_profile_role() = 'commercial'
    and commercial_user_id = auth.uid()
  )
  -- assistante/metreur limited to own created leads
  or (
    public.current_profile_role() in ('assistante','metreur')
    and created_by = auth.uid()
  )
);

drop policy if exists "leads_insert_policy" on public.leads;
create policy "leads_insert_policy"
on public.leads for insert
with check (
  public.current_profile_role() in ('admin','directeur_co','directeur_general','commercial','assistante')
  and created_by = auth.uid()
  and (
    public.current_profile_role() = 'admin'
    or public.current_profile_company() = 'les-deux'
    or societe_crm = public.current_profile_company()
  )
);

drop policy if exists "leads_update_policy" on public.leads;
create policy "leads_update_policy"
on public.leads for update
using (
  public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general')
    and (
      public.current_profile_company() = 'les-deux'
      or societe_crm = public.current_profile_company()
    )
  )
  or (
    public.current_profile_role() = 'commercial'
    and commercial_user_id = auth.uid()
  )
  or (
    public.current_profile_role() = 'assistante'
    and created_by = auth.uid()
  )
)
with check (
  public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general')
    and (
      public.current_profile_company() = 'les-deux'
      or societe_crm = public.current_profile_company()
    )
  )
  or (
    public.current_profile_role() = 'commercial'
    and commercial_user_id = auth.uid()
  )
  or (
    public.current_profile_role() = 'assistante'
    and created_by = auth.uid()
  )
);

drop policy if exists "leads_delete_admin_only" on public.leads;
create policy "leads_delete_admin_only"
on public.leads for delete
using (public.current_profile_role() = 'admin');

-- =========
-- SAV policies
-- =========
drop policy if exists "sav_select_policy" on public.sav;
create policy "sav_select_policy"
on public.sav for select
using (
  public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general')
    and (
      public.current_profile_company() = 'les-deux'
      or societe = public.current_profile_company()
    )
  )
  or (
    public.current_profile_role() in ('assistante','commercial','metreur')
    and (
      public.current_profile_company() = 'les-deux'
      or societe = public.current_profile_company()
    )
  )
);

drop policy if exists "sav_write_policy" on public.sav;
create policy "sav_write_policy"
on public.sav for all
using (
  public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general','assistante')
    and (
      public.current_profile_company() = 'les-deux'
      or societe = public.current_profile_company()
    )
  )
)
with check (
  public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general','assistante')
    and (
      public.current_profile_company() = 'les-deux'
      or societe = public.current_profile_company()
    )
  )
);

-- =========
-- NOTES policies
-- =========
drop policy if exists "notes_select_policy" on public.notes;
create policy "notes_select_policy"
on public.notes for select
using (
  public.current_profile_role() = 'admin'
  or author_uid = auth.email()
  or author_uid = (select lower(split_part(email,'@',1)) from auth.users where id = auth.uid())
  or target_uid = 'all'
);

drop policy if exists "notes_write_policy" on public.notes;
create policy "notes_write_policy"
on public.notes for all
using (
  public.current_profile_role() = 'admin'
  or created_by = auth.uid()
)
with check (
  public.current_profile_role() = 'admin'
  or created_by = auth.uid()
);

-- =========
-- ABSENCES policies
-- =========
drop policy if exists "absences_select_policy" on public.absences;
create policy "absences_select_policy"
on public.absences for select
using (
  public.current_profile_role() = 'admin'
  or created_by = auth.uid()
  or exists (
    select 1
    from jsonb_array_elements_text(coalesce(notifs,'[]'::jsonb)) as notif(uid_slug)
    where lower(notif.uid_slug) = public.current_profile_app_uid()
  )
);

drop policy if exists "absences_write_policy" on public.absences;
create policy "absences_write_policy"
on public.absences for all
using (
  public.current_profile_role() in ('admin','assistante')
)
with check (
  public.current_profile_role() in ('admin','assistante')
);

-- =========
-- ANNUAIRE policies
-- =========
drop policy if exists "annuaire_select_policy" on public.annuaire;
create policy "annuaire_select_policy"
on public.annuaire for select
using (
  public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general','assistante','commercial','metreur')
    and (
      public.current_profile_company() = 'les-deux'
      or societe = public.current_profile_company()
    )
  )
);

drop policy if exists "annuaire_write_policy" on public.annuaire;
create policy "annuaire_write_policy"
on public.annuaire for all
using (
  public.current_profile_role() in ('admin','assistante')
)
with check (
  public.current_profile_role() in ('admin','assistante')
);

-- =========
-- APP SETTINGS policies
-- =========
drop policy if exists "settings_admin_read" on public.app_settings;
create policy "settings_admin_read"
on public.app_settings for select
using (
  public.current_profile_role() = 'admin'
  or key = ('user_state_' || public.current_profile_app_uid())
);

drop policy if exists "settings_admin_write" on public.app_settings;
create policy "settings_admin_write"
on public.app_settings for all
using (
  public.current_profile_role() = 'admin'
  or key = ('user_state_' || public.current_profile_app_uid())
)
with check (
  public.current_profile_role() = 'admin'
  or key = ('user_state_' || public.current_profile_app_uid())
);

alter table public.absences
  drop constraint if exists absences_dates_valid;
alter table public.absences
  add constraint absences_dates_valid check (fin >= debut);

alter table public.absences
  drop constraint if exists absences_notifs_is_array;
alter table public.absences
  add constraint absences_notifs_is_array check (jsonb_typeof(notifs) = 'array');

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
after insert on auth.users
for each row execute function public.handle_new_auth_user();

with missing_users as (
  select
    au.id,
    lower(coalesce(au.email, '')) as email,
    lower(regexp_replace(split_part(coalesce(au.email, ''), '@', 1), '[^a-z0-9_]+', '_', 'g')) as base_uid,
    coalesce(nullif(trim(coalesce(au.raw_user_meta_data->>'full_name', '')), ''), split_part(coalesce(au.email, ''), '@', 1), 'Utilisateur') as full_name,
    case
      when lower(coalesce(au.raw_user_meta_data->>'role', 'assistante')) in ('admin','directeur_co','directeur_general','commercial','assistante','metreur')
        then lower(au.raw_user_meta_data->>'role')
      else 'assistante'
    end as role,
    case
      when lower(coalesce(au.raw_user_meta_data->>'company', 'nemausus')) in ('nemausus','lambert','les-deux')
        then lower(au.raw_user_meta_data->>'company')
      else 'nemausus'
    end as company
  from auth.users au
  left join public.profiles p on p.id = au.id
  where p.id is null
),
ranked as (
  select
    m.*,
    row_number() over (partition by m.base_uid order by m.id) as rn
  from missing_users m
),
prepared as (
  select
    r.id,
    r.email,
    case
      when r.rn = 1 and not exists (select 1 from public.profiles p2 where p2.app_uid = r.base_uid)
        then r.base_uid
      else r.base_uid || '_' || substr(replace(r.id::text, '-', ''), 1, 6)
    end as app_uid,
    r.full_name,
    r.role,
    r.company
  from ranked r
)
insert into public.profiles (id, email, app_uid, full_name, role, company)
select id, email, app_uid, full_name, role, company
from prepared
on conflict (id) do nothing;

drop trigger if exists trg_profiles_updated_at on public.profiles;
create trigger trg_profiles_updated_at
before update on public.profiles
for each row execute function public.set_updated_at();

drop trigger if exists trg_leads_updated_at on public.leads;
create trigger trg_leads_updated_at
before update on public.leads
for each row execute function public.set_updated_at();

drop trigger if exists trg_sav_updated_at on public.sav;
create trigger trg_sav_updated_at
before update on public.sav
for each row execute function public.set_updated_at();

drop trigger if exists trg_notes_updated_at on public.notes;
create trigger trg_notes_updated_at
before update on public.notes
for each row execute function public.set_updated_at();

drop trigger if exists trg_absences_updated_at on public.absences;
create trigger trg_absences_updated_at
before update on public.absences
for each row execute function public.set_updated_at();

drop trigger if exists trg_annuaire_updated_at on public.annuaire;
create trigger trg_annuaire_updated_at
before update on public.annuaire
for each row execute function public.set_updated_at();

drop trigger if exists trg_app_settings_updated_at on public.app_settings;
create trigger trg_app_settings_updated_at
before update on public.app_settings
for each row execute function public.set_updated_at();

create index if not exists idx_sav_societe on public.sav(societe);
create index if not exists idx_sav_statut on public.sav(statut);
create index if not exists idx_notes_author_uid on public.notes(author_uid);
create index if not exists idx_absences_debut_fin on public.absences(debut, fin);
create index if not exists idx_annuaire_societe on public.annuaire(societe);

-- Optional: create a secure view for non-admin users if needed later
-- create view public.shared_runtime_flags as
-- select key, (value - 'key') as value
-- from public.app_settings
-- where key <> 'shared_ai_api';

-- =========
-- RPC : créer / mettre à jour profiles pour auth.uid() (comptes sans trigger)
-- =========
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

