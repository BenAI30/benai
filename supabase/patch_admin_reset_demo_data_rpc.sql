-- ═══════════════════════════════════════════════════════════════════════════
-- BenAI — REPARTIR DE ZÉRO (Supabase → SQL → New query → Run une fois)
-- ═══════════════════════════════════════════════════════════════════════════
-- Si ça échoue : copiez le message d’erreur rouge en entier (souvent : table inexistante).
--
-- Efface : leads, SAV, notes, absences, benai_state (si elle existe), user_state_*,
--          puis remplace shared_core_data_v1 (CRM + messages vides, annuaire = table).
-- Conserve : Auth, profiles, table annuaire.
-- ═══════════════════════════════════════════════════════════════════════════

begin;

truncate table
  public.leads,
  public.sav,
  public.notes,
  public.absences
restart identity cascade;

-- benai_state n’est pas dans supabase_security.sql : elle existe seulement si vous l’avez créée ailleurs.
do $blk$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public'
      and table_name = 'benai_state'
  ) then
    execute 'truncate table public.benai_state restart identity cascade';
  end if;
end
$blk$;

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
          select jsonb_agg(x.row_json)
          from (
            select
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
            order by a.id
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

commit;
