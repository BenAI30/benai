# BenAI — Modèle d’accès (Supabase + app)

Document de référence : **qui est qui**, **qui peut quoi** côté base (RLS) et côté interface, **où ça diverge**, et **pourquoi il y a eu des bugs**.

---

## 1. Identité et source de vérité

| Élément | Rôle |
|--------|------|
| **Authentification** | `auth.users` (Supabase Auth), identifiant stable = `auth.uid()` (UUID). |
| **Profil métier** | `public.profiles` : `id` = `auth.uid()`, `role`, `company`, `app_uid`, etc. |
| **JWT** | PostgREST envoie le JWT ; `auth.uid()` et les politiques RLS s’appuient dessus. |
| **Helpers SQL** | `current_profile_role()`, `current_profile_company()`, `current_profile_app_uid()` : en **`SECURITY DEFINER`** + `search_path` fixe — **obligatoire** pour éviter la récursion RLS sur `profiles`. |

Toute incohérence (ligne absente dans `profiles`, mauvais `role` en metadata, trigger `handle_new_auth_user` en échec) se traduit par des **403/500** ou un UI qui croit un rôle alors que la base refuse.

---

## 2. Rôles reconnus (CHECK SQL + app)

Valeurs autorisées dans `profiles.role` (et metadata à l’inscription) :

`admin`, `directeur_co`, `directeur_general`, `commercial`, `assistante`, `metreur`

**Entreprise** (`profiles.company`) : `nemausus`, `lambert`, `les-deux`

**CRM société sur un lead** (`leads.societe_crm`) : seulement `nemausus`, `lambert` (pas `les-deux` au niveau lead — le périmètre « les deux » est géré au niveau **profil** + logique `company` / `societe_crm`).

---

## 3. Matrice des droits — **base de données** (`supabase_security.sql`)

Légende : **R** = lecture (SELECT), **C** = création, **U** = mise à jour, **D** = suppression.  
Les conditions exactes sont dans le fichier SQL ; ici c’est la **logique métier**.

### `public.profiles`

| Rôle | R | C/U/D |
|------|---|--------|
| **Tous** | Soi (`id = auth.uid()`), via politique `profiles_self_or_admin_select`. | Pas d’auto-écriture REST : seulement **admin** via `profiles_admin_write` (FOR ALL). |
| **admin** | Tous les profils (via `current_profile_role() = 'admin'`). | Oui (toutes lignes). |
| **directeur_co / directeur_general** | Profils de la **même entreprise** (+ règles `les-deux` / croisements Nemausus–Lambert). | Non (pas de policy INSERT/UPDATE non-admin). |

**RPC** `ensure_my_profile()` : `SECURITY DEFINER`, upsert **uniquement la ligne de l’utilisateur connecté** (réparation si trigger absent).

### `public.leads`

| Rôle | R | C | U | D |
|------|---|---|---|---|
| **admin** | Tout | Oui | Oui | Oui |
| **directeur_co / directeur_general** | Leads dont `societe_crm` est dans leur périmètre (ou `company = les-deux`) | Oui (WITH CHECK société) | Oui | Non (delete admin only) |
| **commercial** | Leads où `commercial_user_id = auth.uid()` | Oui | Oui (souvent les siens) | Non |
| **assistante** | Leads où `created_by = auth.uid()` | Oui | Oui | Non |
| **metreur** | Idem assistante sur **created_by** | ? | ? | Non |

> **Point de vigilance** : en base, **metreur** est traité comme **assistante** pour les leads (SELECT/INSERT/UPDATE sur `created_by = auth.uid()`). Vérifier que c’est bien le métier voulu (sinon ajuster les policies).

### `public.sav`

| Rôle | R | Écriture |
|------|---|----------|
| **admin** | Tout | Tout |
| **directeur_co / directeur_general** | `societe` dans périmètre | Oui (même périmètre) |
| **assistante / commercial / metreur** | `societe` dans périmètre | Oui (policy `sav_write_policy` inclut ces rôles) |

### `public.notes`

| Rôle | Accès |
|------|--------|
| **admin** | Tout |
| **autres** | Policy historique : accès **très large** côté fichier `supabase_security.sql` (à relire ligne par ligne si durcissement nécessaire). |

### `public.absences`

| Rôle | R | Écriture |
|------|---|----------|
| **admin** | Tout | Tout |
| **non-admin** | SELECT si notif les concerne (`current_profile_app_uid()` dans JSON `notifs`) | **admin** + **assistante** seulement (`absences_write_policy`) |

**Conséquence** : commercial / dir. co / DG **voient** des absences (selon notifs) mais **ne modifient** pas via RLS — sauf si le client contourne (voir section 5).

### `public.annuaire`

| Rôle | R | Écriture |
|------|---|----------|
| **admin** | Tout | Tout |
| **directeur_co / directeur_general / commercial / metreur** | `societe` dans périmètre | **Non** (write = admin + assistante) |
| **assistante** | Périmètre société | Oui |

### `public.app_settings`

- **Lecture / écriture** : clé `shared_ai_api` réservée **admin** ; clés `user_state_<app_uid>` pour l’utilisateur courant (via `current_profile_app_uid()`).

---

## 4. Matrice des droits — **application** (`BenAI_v3 15-04.html`)

L’UI utilise `ROLE_PAGES` : **pages visibles** (pas forcément égal aux droits SQL).

| Rôle | Pages (aperçu) |
|------|------------------|
| **admin** | benai, notes, messages, sav, leads, absences, annuaire, paie, admin, evolution, guide, **bugs** (écran Tickets / liste des signalements) |
| **assistante** | benai, notes, messages, sav, leads, absences, evolution, guide (**pas** bugs / annuaire / paie / admin) |
| **metreur** | benai, notes, messages, absences, evolution, guide (**pas** bugs / sav / leads dans la liste) |
| **directeur_co / directeur_general / commercial** | benai, notes, messages, sav, leads, absences, evolution, guide (**pas** bugs ; aligné RLS société sur SAV / leads). |

**Signalement** : entrée **« Signaler »** (overlay) pour tous les comptes connectés — envoi d’un ticket **sans** accès à la liste ; la consultation / résolution reste **admin** (page `bugs`).

Le rôle affiché après login Supabase passe par **`normalizeProfileRole()`** (minuscules, espaces/tirets → `_`, alias dg / dirco) pour coller au CHECK SQL et éviter une vue « commercial » (2 onglets CRM) si la base renvoie une chaîne mal formée.

Fonctions utiles :

- `isCRMScopePilotageRole` → `directeur_co` **ou** `directeur_general` (même logique pilotage / société que le dir. co).
- `isCrmSalesActorRole` → commercial + dir. co + DG (création terrain, attribution, etc.).
- `canAccessLeadByCompany` → filtre **côté client** sur les leads déjà chargés (doit **rester aligné** avec `leads_select_policy`).

**Risque classique** : l’app affiche une page ou une action alors que le **SELECT REST** renvoie vide ou 403 → impression de « bug » alors que c’est la RLS qui est stricte.

---

## 5. Conflit majeur à connaître : `supabase_sync_repair.sql`

Ce fichier définit des politiques du type :

`using (true) with check (true)` sur **sav, notes, absences, annuaire, leads, app_settings, benai_state** pour le rôle `authenticated`.

**Si ce script a été exécuté sur le projet**, en complément de `supabase_security.sql` :

- Les politiques permissives s’**additionnent** (OR) aux politiques métier → **n’importe quel utilisateur connecté peut tout lire/écrire** sur ces tables (d’où les alertes *« RLS toujours vraie »* du Security Advisor).
- Vous avez alors **deux modèles** en même temps : le vôtre (métier) et le « repair » (tout ouvert).

**Recommandation** : traiter **`supabase_security.sql`** (+ patches ciblés) comme **référence sécurité**. Ne pas réappliquer `supabase_sync_repair.sql` sur un projet déjà sécurisé ; si besoin de réparation, **DROP** les policies `*_rw_auth` puis réappliquer uniquement les policies de `supabase_security.sql`.

---

## 6. Fichiers SQL « patches » (ordre logique)

1. **`supabase_security.sql`** — schéma + helpers + RLS + trigger `handle_new_auth_user` + RPC `ensure_my_profile` (référence complète).
2. **`patch_ensure_my_profile.sql`** — helpers + RLS + fonction SQL + `GRANT` + `NOTIFY pgrst` (déploiement rapide sans rejouer tout le fichier).
3. **`patch_profiles_select_pilotage.sql`** — politique SELECT `profiles` pour dir. co / DG (déjà dans `supabase_security.sql` si déploiement complet).
4. **`patch_directeur_general_profiles.sql`** — contrainte rôle + trigger (si base créée avant l’ajout DG).

---

## 7. Analyse des bugs rencontrés (racines)

| Symptôme | Cause racine |
|----------|----------------|
| HTTP **500** sur `GET /profiles` | RLS sur `profiles` appelait `current_profile_*` **sans** `SECURITY DEFINER` → récursion. |
| HTTP **404** sur `rpc/ensure_my_profile` | Fonction absente ou cache PostgREST pas rechargé ; ou script non exécuté jusqu’au bout. |
| « Relation **v_auth_id** / **rec** n’existe pas » | Exécution **partielle** de PL/pgSQL (morceau de fonction hors `CREATE FUNCTION`). |
| Fenêtre **« sans RLS »** Supabase | Heuristique du dashboard sur tout script touchant `profiles` — choisir **Exécuter et activer RLS** (vert). |
| **Profil manquant** | Trigger insert échoué, metadata invalide, ou compte créé avant trigger ; la RPC `ensure_my_profile` corrige **après** login. |
| **Commercial introuvable** / pas de liste | SELECT `profiles` refusé pour dir. co avant patch ; côté app, sync roster pilotage à aligner avec les rôles CRM. |

---

## 8. Recommandations (priorisées)

1. **Une seule source RLS** : finaliser le projet sur `supabase_security.sql` ; retirer ou neutraliser les policies `*_rw_auth` de `supabase_sync_repair.sql` si elles sont présentes.
2. **Checklist déploiement** : après tout changement de fonction exposée à PostgREST, exécuter `NOTIFY pgrst, 'reload schema';` (déjà dans le patch ensure).
3. **Aligner metreur** : décider si metreur = assistante pour les leads en base **et** dans l’UI (`ROLE_PAGES` vs `leads_*_policy`).
4. **Tests par rôle** : pour chaque rôle, vérifier `GET profiles`, `GET leads`, `POST leads`, `PATCH leads` avec le JWT réel (pas seulement l’éditeur SQL qui bypass RLS en superuser).
5. **Documentation métier** : remplir une colonne « intention métier » pour DG vs dir. co (même CRM aujourd’hui — documenter si la divergence doit apparaître plus tard).

---

*Dernière mise à jour : alignée sur `supabase_security.sql` et `BenAI_v3 15-04.html` du dépôt BenAI.*
