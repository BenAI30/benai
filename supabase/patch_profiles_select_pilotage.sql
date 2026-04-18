-- Lecture des profils « même entreprise » : direction + commercial + assistante + métreur (CRM + messagerie).
-- Sans cette politique, les commerciaux / assistantes ne reçoivent qu’UNE ligne (eux-mêmes) depuis l’API → équipe invisible.
-- À exécuter dans Supabase SQL Editor (valider l’avertissement « opérations destructrices » : normal, c’est le DROP POLICY).
-- Si vous ne réexécutez pas tout supabase_security.sql, exécuter UNIQUEMENT ce fichier après mise à jour du code BenAI.
--
-- Évite l’avertissement « public.profiles sans RLS » de l’éditeur (idempotent).

alter table if exists public.profiles enable row level security;

drop policy if exists "profiles_self_or_admin_select" on public.profiles;
create policy "profiles_self_or_admin_select"
on public.profiles for select
using (
  id = auth.uid()
  or public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general','commercial','assistante','metreur')
    and (
      public.current_profile_company() = 'les-deux'
      or public.profiles.company = public.current_profile_company()
      or (
        public.current_profile_company() = 'nemausus'
        and public.profiles.company in ('nemausus','les-deux')
      )
      or (
        public.current_profile_company() = 'lambert'
        and public.profiles.company in ('lambert','les-deux')
      )
    )
  )
);
