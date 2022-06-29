# Refiners

## Description

Hublot: https://clhub.clessn.cloud/admin/

## Utilisation

1. On the admin site of Hublot, open Dynamic table. 
2. Create a table
    1. Exemple: Créer un raffineur qui va créer un comptoir qui va calculer le nombre de communiqués publiés par partis par semaine. 
3. Name the table
    1. Name: mart_ (prefix for datamart table)
    2. Table, verbose name and verbose name plural: same
4. Create datamart metadata
    1. Type: table
    2. Format: dataframe
    3. Pillar: décideurs, médias, citoyens
    4. Hashtag: permet tracabilité du lac vers entrepot/comptoir
    5. Description: free text. Expliquer le comptoir. 
    6. Content type: façon de mettre sous forme python case ce qu’il y a dans la table
    7. Storage class: toujours mart. A aussi lake et un autre (entrepôt?).
5. Populate the table based on the RETL repo
    1. Clone repository clessn/retl
    2. Copy content of RETL into cleessn-blend/pipeline/refiners, create new folder with name format r_nom_du_refiner. Prefix r_ stands for refiner.
6. Open RStudio
7. Open Rprojet in the refiner folder.
8. Delete README.md
9. Modify template_README.md to describe the refinor in non-technical language. Rename README.md
10. Push in CLESSN-blend
11. Start coding. À part du README et de dossier de code, pas besoin de toucher le reste. 
12. Code.R: open to code as it's the code template. Content is related to automating your refinor.
13. Create the refinor
    1. Change line 111 and put the refinor name in snake_case 
    2. In MAIN: that's where your code will go and where you'll be able to test it.
    3. Go back into Hublot, look at table to identify the intrant. Look for unique key, timestamp, and body. 
    4. Take the variable name and:

```r
{
warehouse_table_name <- "political_parties_press_releases"
datamart_press_release_frequency <- "[nom de la dynamic table, enlever préfixe]"

warehouse_df <- clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```

1. Execute
2. Inspect the variables. Everything with Data. prefix are columns in the storage table.
3. Enrichir la table pour créer un comptoir qui est utile pour la recherche. Ajouter colonne.
4. Doit toujours avoir une key dplyr::mutate(key = paste(political_party, week_num, format(Sys.Date(), “%Y”), sep = “”))
5. Écrire dataframe dans Hublot via R

```r
clessnverse::commit_mart_table(df, datamart_df, key_column = “key”, mode = “refresh”, credentials)}
```

1. Tu peux mettre ton ggplot dedans. Le rafineur peut créer ton graphique. Tu peux mettre ton ggplot dans le lac avec clessn::commit_lake_item()

## R enviro

R enviro: aller dans clessn-blend. Suivre les instructions dans repo Renviro-tutorial: tout est expliqué. Ça crée un fichier dans votre R intentory.

## Aide-mémoire

```r
clessnverse::commit_mart_table(df, datamart_df, key_column = “key”, mode = “refresh”, credentials)
clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```
