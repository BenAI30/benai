-- Répare les comptes où auth.users existe mais public.profiles n’a pas été créé (trigger absent, erreur passée, etc.).
-- Exécuter une fois dans Supabase → SQL Editor.
-- BenAI appelle ensuite POST /rest/v1/rpc/ensure_my_profile avec le JWT utilisateur après connexion.

create or replace function public.ensure_my_profile()
returns void
language plpgsql
security definer
set search_path = public, auth
as $$
declare
  uid uuid;
  uemail text;
  meta jsonb;
  full_name_val text;
  role_val text;
  company_val text;
  base_uid text;
  safe_uid text;
begin
  -- Pas de « SELECT * INTO u » : certains analyseurs SQL le confondent avec une CREATE TABLE u.
  select id, lower(coalesce(email, '')), coalesce(raw_user_meta_data, '{}'::jsonb)
  into uid, uemail, meta
  from auth.users
  where id = auth.uid();

  if uid is null then
    return;
  end if;

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
  if exists (select 1 from public.profiles p where p.app_uid = safe_uid and p.id <> uid) then
    safe_uid := safe_uid || '_' || substr(replace(uid::text, '-', ''), 1, 6);
  end if;

  insert into public.profiles (id, email, app_uid, full_name, role, company)
  values (uid, uemail, safe_uid, full_name_val, role_val, company_val)
  on conflict (id) do update set
    email = excluded.email,
    app_uid = excluded.app_uid,
    full_name = excluded.full_name,
    role = excluded.role,
    company = excluded.company,
    updated_at = now();
end;
$$;

grant execute on function public.ensure_my_profile() to authenticated;
