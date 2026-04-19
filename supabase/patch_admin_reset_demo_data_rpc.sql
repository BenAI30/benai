-- ═══════════════════════════════════════════════════════════════════════════
-- BenAI — « Repartir de zéro » : RPC appelée par le bouton Pilotage (1 clic)
-- ═══════════════════════════════════════════════════════════════════════════
-- À exécuter UNE FOIS dans Supabase → SQL → Run (installe la fonction).
-- Ensuite dans BenAI : Pilotage → Fichiers & outils → « Repartir de zéro (1 clic) ».
--
-- Réservé au rôle admin (vérifie auth.uid() dans public.profiles).
-- SECURITY DEFINER : contourne la RLS pour truncate / app_settings.
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.admin_wipe_benai_reset()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
begin
  if auth.uid() is null then
    raise exception 'Connexion requise';
  end if;
  if (select lower(trim(coalesce(p.role, ''))) from public.profiles p where p.id = auth.uid() limit 1)
     is distinct from 'admin' then
    raise exception 'Action reservee au role admin (dans Supabase : public.profiles → colonne role = admin pour votre utilisateur).';
  end if;

  truncate table
    public.leads,
    public.sav,
    public.notes,
    public.absences
  restart identity cascade;

  if exists (
    select 1 from information_schema.tables
    where table_schema = 'public' and table_name = 'benai_state'
  ) then
    execute 'truncate table public.benai_state restart identity cascade';
  end if;

  delete from public.app_settings
  where key like 'user_state\_%' escape '\';

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
        'annuaire', coalesce(
          (
            select jsonb_agg(x.row_json order by x.sort_id)
            from (
              select
                a.id as sort_id,
                (coalesce(a.payload, '{}'::jsonb) || jsonb_build_object(
                  'id', a.id,
                  'prenom', coalesce(a.prenom, ''),
                  'nom', coalesce(a.nom, ''),
                  'email', coalesce(a.email, ''),
                  'emailPro', coalesce(a.email_pro, ''),
                  'tel', coalesce(a.tel, ''),
                  'naissance', coalesce(a.naissance::text, ''),
                  'fonction', coalesce(a.fonction, 'Autre'),
                  'societe', a.societe,
                  'sync_ts', (floor(extract(epoch from clock_timestamp()) * 1000))::bigint
                )) as row_json
              from public.annuaire a
            ) x
          ),
          '[]'::jsonb
        ),
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

  return jsonb_build_object(
    'ok', true,
    'annuaire_count', (select count(*)::int from public.annuaire)
  );
end;
$$;

revoke all on function public.admin_wipe_benai_reset() from public;
grant execute on function public.admin_wipe_benai_reset() to authenticated;

-- PostgREST met parfois plusieurs secondes à voir la nouvelle RPC sans ce signal.
notify pgrst, 'reload schema';
