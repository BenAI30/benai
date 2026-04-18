-- Répare les comptes où auth.users existe mais public.profiles n’a pas été créé (trigger absent, erreur passée, etc.).
-- Exécuter une fois dans Supabase → SQL Editor.
-- BenAI appelle ensuite POST /rest/v1/rpc/ensure_my_profile avec le JWT utilisateur après connexion.
--
-- L’éditeur SQL peut afficher une alerte « public.profiles sans RLS » : c’est souvent un faux positif.
-- Si une fenêtre s’ouvre : choisissez « Exécuter et activer RLS » (bouton vert). Ne choisissez pas « sans RLS ».
-- La ligne suivante ne change rien si la RLS est déjà activée (idempotent).

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
