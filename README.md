# BenAI

Projet local minimal pour continuer le developpement de `BenAI_v3 15-04.html` avec la partie `Supabase` du dossier `supabase/`.

## Fichiers utiles

- `index.html` : point d'entree local
- `BenAI_v3 15-04.html` : application principale
- `server.js` : petit serveur statique local
- `supabase/` : configuration et fonctions Edge
- `supabase_security.sql` : base SQL pour la migration

## Lancer le projet

```bash
npm start
```

Puis ouvrir :

`http://localhost:3000`

## Reprise de la migration Supabase

- URL Supabase deja configuree dans le HTML
- cle publishable a renseigner dans les parametres de l'application si besoin
- fonction Edge presente : `supabase/functions/create-user/index.ts`
- SQL de base present : `supabase_security.sql`

## Notes

Le projet n'avait pas de structure Node classique. Ce socle sert surtout a :

- rouvrir facilement le bon dossier dans Cursor
- lancer l'application localement
- separer plus clairement le HTML principal et la couche Supabase
