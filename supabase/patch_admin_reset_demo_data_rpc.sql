-- ═══════════════════════════════════════════════════════════════════════════
-- BenAI — REPARTIR DE ZÉRO (une seule exécution dans Supabase → SQL → New query)
-- ═══════════════════════════════════════════════════════════════════════════
-- Efface : leads, SAV, notes, absences, snapshots benai_state, miroir partagé
--          + toutes les lignes app_settings dont la clé commence par user_state_
-- Conserve : comptes Auth, table profiles, table annuaire (fiches employés)
-- Ne change pas : clé shared_ai_api ni les autres clés app_settings hors liste ci-dessus
--
-- Avant : faites une sauvegarde si besoin. En PRODUCTION, vérifiez deux fois.
-- Puis : bouton RUN (ou Ctrl+Enter). Pas besoin de Edge Function ni de déploiement.
-- ═══════════════════════════════════════════════════════════════════════════

begin;

truncate table
  public.leads,
  public.sav,
  public.notes,
  public.absences,
  public.benai_state
restart identity cascade;

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
          select jsonb_agg(
            (coalesce(a.payload, '{}'::jsonb) || jsonb_build_object(
              'id', a.id,
              'prenom', a.prenom,
              'nom', a.nom,
              'email', coalesce(a.email, ''),
              'emailPro', coalesce(a.email_pro, ''),
              'tel', coalesce(a.tel, ''),
              'naissance', coalesce(a.naissance::text, ''),
              'fonction', coalesce(a.fonction, 'Autre'),
              'societe', a.societe,
              'sync_ts', (floor(extract(epoch from clock_timestamp()) * 1000))::bigint
            ))
            order by a.id
          )
          from public.annuaire a
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

-- Vérifications (optionnel, dans une nouvelle requête) :
-- select count(*) from public.leads;
-- select count(*) from public.annuaire;
-- select key from public.app_settings where key like 'user_state%' limit 5;
