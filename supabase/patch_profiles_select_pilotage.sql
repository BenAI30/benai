-- Permet au dir. commercial / DG de lire les profils de leur périmètre (liste commerciaux CRM, attribution).
-- À exécuter dans Supabase SQL Editor si vous ne réexécutez pas tout supabase_security.sql.

drop policy if exists "profiles_self_or_admin_select" on public.profiles;
create policy "profiles_self_or_admin_select"
on public.profiles for select
using (
  id = auth.uid()
  or public.current_profile_role() = 'admin'
  or (
    public.current_profile_role() in ('directeur_co','directeur_general')
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
