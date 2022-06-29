# Refiners

## Description


## Utilisation

1. Aller dans site d’administration dans Hublot. Accéder à la page d’accueil d’Hublot. Va dans dynamic tables. 
2. Crée une table.
    1. Exemple: Créer un raffineur qui va créer un comptoir qui va calculer le nombre de communiqués publiés par partis par semaine. 
3. Nommer la table
    1. Nom: mart_ : table de comptoir
    2. Table, verbose name and verbose name plural: pareil
4. Créer métadonnées de comptoir
    1. Type: table
    2. Format: dataframe
    3. Pillier: décideurs, médias, citoyens
    4. Hashtag: permet tracabilité du lac vers entrepot/comptoir
    5. Description: free text. Expliquer le comptoir. 
    6. Content type: façon de mettre sous forme python case ce qu’il y a dans la table
    7. Storage class: toujours mart. A aussi lake et un autre (entrepôt?).
5. Populer la table basé sur le repo retl
    1. Cloner repository clessn/retl
    2. Copier contenu retl et coller dans cleessn-blend/pipeline/refiners, créer nouveau dossier selon format r_nom_du_refiner. Préfixe r_ est pour refiner.
6. Ouvrir RStudio
7. Ouvrir Rprojet dans dossier de ton raffineur
8. Supprimer README.md
9. Modifier template_README.md pour décrire le raffineur dans le langage pas technique. Renommer README.md
10. Push dans CLESSN-blend
11. Mettre à coder. À part du README et de dossier de code, pas besoin de toucher le reste. 
12. Code.R: ouvrir pour coder. C’est le gabarit de code. Coder à l’intérieur de ce gabarit. Contenu est relié à l’automatisation de ton rafineur.
13. Créer le rafineur
    1. Changer la ligne 111 et mettre le nom de notre rafineur en pythoncase.
    2. Dans MAIN: c’est là que votre code va et que vous allez pouvoir le tester.
    3. Retourner dans Hublot, regarder table pour identifier l’intrant. A clé unique et timestamp. Voit champ qui s’appelle body.
    4. Prendre le nom de la table et 

```r
{
warehouse_table_name <- "political_parties_press_releases"
datamart_press_release_frequency <- "[nom de la dynamic table, enlever préfixe]"

warehouse_df <- clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```

1. Exécuter.
2. Inspecter les variables. Tout ce qui est préfixé par Data. sont colonnes dans la table d’entrepôt.
3. Enrichir la table pour créer un comptoir qui est utile pour la recherche. Ajouter colonne.
4. Doit toujours avoir une key dplyr::mutate(key = paste(political_party, week_num, format(Sys.Date(), “%Y”), sep = “”))
5. Écrire dataframe dans Hublot via R

```markdown
clessnverse::commit_mart_table(df, datamart_df, key_column = “key”, mode = “refresh”, credentials)}
```

1. Tu peux mettre ton ggplot dedans. Le rafineur peut créer ton graphique. Tu peux mettre ton ggplot dans le lac avec clessn::commit_lake_item()

**R enviro**

R enviro: aller dans clessn-blend. Suivre les instructions dans repo Renviro-tutorial: tout est expliqué. Ça crée un fichier dans votre R intentory.
