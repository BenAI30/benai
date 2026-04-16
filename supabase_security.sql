-- BenAI - Base security model for Supabase (RLS)
-- Execute in Supabase SQL Editor.

create extension if not exists pgcrypto;

-- =========
-- PROFILES
-- =========
create table if not exists public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text unique,
  full_name text not null,
  role text not null check (role in ('admin','directeur_co','commercial','assistante','metreur')),
  company text not null check (company in ('nemausus','lambert','les-deux')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

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
create or replace function public.current_profile_role()
returns text language sql stable as $$
  select role from public.profiles where id = auth.uid()
$$;

create or replace function public.current_profile_company()
returns text language sql stable as $$
  select company from public.profiles where id = auth.uid()
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
  -- directeur_co sees only own company
  or (
    public.current_profile_role() = 'directeur_co'
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
  public.current_profile_role() in ('admin','directeur_co','commercial','assistante')
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
    public.current_profile_role() = 'directeur_co'
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
    public.current_profile_role() = 'directeur_co'
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
    public.current_profile_role() = 'directeur_co'
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
    public.current_profile_role() in ('directeur_co','assistante')
    and (
      public.current_profile_company() = 'les-deux'
      or societe = public.current_profile_company()
    )
  )
)
with check (
  public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','assistante')
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
  or created_by is null
);

-- =========
-- ABSENCES policies
-- =========
drop policy if exists "absences_select_policy" on public.absences;
create policy "absences_select_policy"
on public.absences for select
using (
  public.current_profile_role() = 'admin'
  or public.current_profile_role() in ('directeur_co','assistante','commercial','metreur')
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
    public.current_profile_role() in ('directeur_co','assistante','commercial','metreur')
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
using (public.current_profile_role() = 'admin');

drop policy if exists "settings_admin_write" on public.app_settings;
create policy "settings_admin_write"
on public.app_settings for all
using (public.current_profile_role() = 'admin')
with check (public.current_profile_role() = 'admin');

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

