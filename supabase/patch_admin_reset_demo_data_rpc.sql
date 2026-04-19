-- BenAI — Remise à zéro des données métier, en conservant auth + profiles + annuaire (RPC pour Edge admin-reset-demo-data).
-- Exécuter dans le SQL Editor Supabase.

drop function if exists public.admin_cleanup_orphan_refs(uuid);
drop function if exists public.admin_reset_demo_data_truncate();

create or replace function public.admin_wipe_benai_keep_annuaire(p_caller uuid)
returns void
language plpgsql
security definer
set search_path = public, auth
as $$
begin
  if p_caller is null or not exists (select 1 from auth.users au where au.id = p_caller) then
    raise exception 'admin_wipe_benai_keep_annuaire: p_caller invalide';
  end if;

  truncate table
    public.leads,
    public.sav,
    public.notes,
    public.absences,
    public.benai_state
  restart identity cascade;
end;
$$;

revoke all on function public.admin_wipe_benai_keep_annuaire(uuid) from public;
grant execute on function public.admin_wipe_benai_keep_annuaire(uuid) to service_role;
