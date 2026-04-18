-- Patch ponctuel : rôle directeur_general (profils + trigger).
-- À exécuter dans Supabase → SQL Editor si la création DG échouait avec une erreur CHECK sur profiles.role.
-- Pour les politiques RLS (leads, SAV, annuaire), ré-exécutez plutôt tout supabase_security.sql depuis la racine du dépôt.

alter table public.profiles drop constraint if exists profiles_role_check;
alter table public.profiles add constraint profiles_role_check
  check (role in ('admin','directeur_co','directeur_general','commercial','assistante','metreur'));

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
