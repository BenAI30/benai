# BenAI

Application principale : `BenAI_v3 15-04.html`.

## Point d’entrée

- `index.html` : redirection vers `BenAI_v3 15-04.html` (utile pour GitHub Pages / racine du dépôt).

## Fichiers utiles

- `BenAI_v3 15-04.html` : application (shell + chargement de `benai-app.js`, version affichée lue depuis `version.json`)
- `benai-app.js` : logique BenAI (constante `BENAI_VERSION` alignée sur `version.json`)
- `supabase_security.sql` : schéma + RLS
- `Supabase.txt` : URL + clé publishable (à jour manuellement si besoin)
- `supabase/` : fonctions Edge et config CLI (si utilisé)

## Lancer en local

Si tu utilises le petit serveur du dépôt :

```bash
npm start
```

Puis ouvre `http://localhost:3000` (ou le port indiqué par le script).

**Important :** n’ouvre pas l’app en `file://` si tu utilises Supabase (connexion bloquée par le navigateur). Passe par `http://localhost`.
