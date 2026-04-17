-- BenAI - Réparation synchro Supabase (RLS + upsert)
-- Exécuter dans Supabase SQL Editor (projet BenAI)
-- Objectif: fiabiliser la synchro multi-appareils pour tables CRM + snapshots.

begin;

-- 0) Créer les tables manquantes (safe idempotent)
create table if not exists public.sav (
  id bigserial primary key,
  legacy_id text,
  societe text,
  client text,
  probleme text,
  rappel_date text,
  commentaire text,
  urgent boolean default false,
  statut text,
  archive boolean default false,
  mute_reminder boolean default false,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.notes (
  id bigserial primary key,
  legacy_id text,
  text text,
  author_uid text,
  target_uid text,
  ts bigint,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.absences (
  id bigserial primary key,
  legacy_id text,
  employe text,
  debut text,
  fin text,
  type text,
  note text,
  notifs jsonb default '[]'::jsonb,
  heure_debut text,
  heure_fin text,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.annuaire (
  id bigserial primary key,
  legacy_id text,
  prenom text,
  nom text,
  email text,
  email_pro text,
  tel text,
  naissance text,
  fonction text,
  societe text,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.leads (
  id bigint primary key,
  societe_crm text,
  nom text,
  telephone text,
  ville text,
  cp text,
  type_projet text,
  statut text,
  raison_mort text,
  created_by text,
  commercial_user_id text,
  archive boolean default false,
  payload jsonb default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create table if not exists public.app_settings (
  id bigserial primary key,
  key text,
  value jsonb default '{}'::jsonb,
  updated_at timestamptz default now()
);

create table if not exists public.benai_state (
  id bigserial primary key,
  uid text,
  payload jsonb default '{}'::jsonb,
  updated_at timestamptz default now()
);

-- 1) Index uniques requis pour les upserts REST (on_conflict)
create unique index if not exists sav_legacy_id_uidx on public.sav(legacy_id);
create unique index if not exists notes_legacy_id_uidx on public.notes(legacy_id);
create unique index if not exists absences_legacy_id_uidx on public.absences(legacy_id);
create unique index if not exists annuaire_legacy_id_uidx on public.annuaire(legacy_id);
create unique index if not exists leads_id_uidx on public.leads(id);
create unique index if not exists app_settings_key_uidx on public.app_settings(key);
create unique index if not exists benai_state_uid_uidx on public.benai_state(uid);

-- 2) Permissions de base role authenticated
grant usage on schema public to authenticated;
grant select, insert, update, delete on table
  public.sav,
  public.notes,
  public.absences,
  public.annuaire,
  public.leads,
  public.app_settings,
  public.benai_state
to authenticated;

-- 3) RLS activé (et politiques explicites)
alter table public.sav enable row level security;
alter table public.notes enable row level security;
alter table public.absences enable row level security;
alter table public.annuaire enable row level security;
alter table public.leads enable row level security;
alter table public.app_settings enable row level security;
alter table public.benai_state enable row level security;

-- Supprimer anciennes policies ambiguës si présentes
drop policy if exists sav_rw_auth on public.sav;
drop policy if exists notes_rw_auth on public.notes;
drop policy if exists absences_rw_auth on public.absences;
drop policy if exists annuaire_rw_auth on public.annuaire;
drop policy if exists leads_rw_auth on public.leads;
drop policy if exists app_settings_rw_auth on public.app_settings;
drop policy if exists benai_state_rw_auth on public.benai_state;

-- Policies simples et stables pour utilisateurs connectés
create policy sav_rw_auth on public.sav
for all to authenticated
using (true)
with check (true);

create policy notes_rw_auth on public.notes
for all to authenticated
using (true)
with check (true);

create policy absences_rw_auth on public.absences
for all to authenticated
using (true)
with check (true);

create policy annuaire_rw_auth on public.annuaire
for all to authenticated
using (true)
with check (true);

create policy leads_rw_auth on public.leads
for all to authenticated
using (true)
with check (true);

create policy app_settings_rw_auth on public.app_settings
for all to authenticated
using (true)
with check (true);

create policy benai_state_rw_auth on public.benai_state
for all to authenticated
using (true)
with check (true);

commit;

-- Vérifications rapides (optionnel):
-- select key from public.app_settings where key in ('shared_core_data_v1','shared_ai_api');
-- select uid, updated_at from public.benai_state order by updated_at desc nulls last limit 5;
