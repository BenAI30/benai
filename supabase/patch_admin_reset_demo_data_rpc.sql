-- BenAI — Nettoyage des références vers des comptes Auth déjà supprimés (RPC pour Edge admin-reset-demo-data).
-- Exécuter dans le SQL Editor Supabase. Si l’ancienne fonction « truncate » existait, elle est supprimée ici.

drop function if exists public.admin_reset_demo_data_truncate();

create or replace function public.admin_cleanup_orphan_refs(p_caller uuid)
returns void
language plpgsql
security definer
set search_path = public, auth
as $$
begin
  if p_caller is null or not exists (select 1 from auth.users au where au.id = p_caller) then
    raise exception 'admin_cleanup_orphan_refs: p_caller invalide';
  end if;

  update public.leads l
  set commercial_user_id = null
  where l.commercial_user_id is not null
    and not exists (select 1 from auth.users au where au.id = l.commercial_user_id);

  update public.leads l
  set created_by = p_caller
  where not exists (select 1 from auth.users au where au.id = l.created_by);

  update public.sav s
  set created_by = null
  where s.created_by is not null
    and not exists (select 1 from auth.users au where au.id = s.created_by);

  update public.notes n
  set created_by = null
  where n.created_by is not null
    and not exists (select 1 from auth.users au where au.id = n.created_by);

  update public.notes n
  set author_uid = null
  where n.author_uid is not null
    and n.author_uid ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and not exists (select 1 from auth.users au where au.id::text = n.author_uid);

  update public.absences a
  set created_by = null
  where a.created_by is not null
    and not exists (select 1 from auth.users au where au.id = a.created_by);

  update public.annuaire an
  set created_by = null
  where an.created_by is not null
    and not exists (select 1 from auth.users au where au.id = an.created_by);

  delete from public.benai_state b
  where b.uid is not null
    and trim(b.uid) <> ''
    and not exists (select 1 from auth.users au where au.id::text = trim(b.uid));
end;
$$;

revoke all on function public.admin_cleanup_orphan_refs(uuid) from public;
grant execute on function public.admin_cleanup_orphan_refs(uuid) to service_role;
