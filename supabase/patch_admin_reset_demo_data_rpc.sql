-- BenAI — RPC appelée par la Edge Function admin-reset-demo-data (service_role uniquement).
-- Après déploiement de la fonction, exécuter ce script dans le SQL Editor Supabase.

create or replace function public.admin_reset_demo_data_truncate()
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  truncate table
    public.leads,
    public.sav,
    public.notes,
    public.absences,
    public.annuaire,
    public.benai_state
  restart identity cascade;

  insert into public.app_settings (key, value, updated_at)
  values (
    'shared_core_data_v1',
    jsonb_build_object(
      'version', 1,
      'updated_at', to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
      'data', jsonb_build_object(
        'sav', '[]'::jsonb,
        'notes', '[]'::jsonb,
        'absences', '[]'::jsonb,
        'annuaire', '[]'::jsonb,
        'leads', '[]'::jsonb,
        'notif_feed', '[]'::jsonb,
        'messages', '{}'::jsonb,
        'msg_deletions', '{}'::jsonb,
        'msg_read_cursor', '{}'::jsonb
      )
    ),
    now()
  )
  on conflict (key) do update set
    value = excluded.value,
    updated_at = excluded.updated_at;
end;
$$;

revoke all on function public.admin_reset_demo_data_truncate() from public;
grant execute on function public.admin_reset_demo_data_truncate() to service_role;
