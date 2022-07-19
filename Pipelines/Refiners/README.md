# Refiners

## Description

A data refiner takes the information from the data warehouse and prepares it for a data mart.

## Create a refiner

### Prerequisites

* R
* R environ: it allows you to hide your password and other sensitive information in a project. To configure it, follow the instructions in the repo `clessn/Renviron_tutorial`.
* Access to clessn repositories on GitHub
* Access to [Hublot](https://clhub.clessn.cloud/admin/)
* CLESSN R packages
 	* `hublot` : `devtools::install_github("clessn/hublotr")`
	* `clessnverse` : download it in R using the code `devtools::install_github("clessn/clessnverse")`
	* `clessnhub`(temporary, for access to Hub 2)

### Tutorial

In this example, we create a refiner for a datamart that will calculate the number of press releases published every week by a party.

1. On the admin site of [Hublot](https://clhub.clessn.cloud/admin/), open Dynamic table. 
2. Create a table and fill in the form according to the Dynamic table parameters at the bottom of this document

3. Create datamart metadata
    1. In the Metadata field, change the view from "Tree" to "Code"
    2. Insert the following template and fill in according to the Metadata parameters at the bottom of this document. Press Save.

```
# Template

{
  "type": "observations",
  "format": "table",
  "content_type": <description en_snake_case du contenu d'une observation du comptoir (au singulier = une unité d'observation)>,
  "storage_class": "mart",
  "source_type": <valeurs de la metadonnée content_type de.s table.s d'entrepôt et Files utilisées pour construire ce comptoir>, # provient donc de la méta donnée content_type des tables d'entrepôt et des Files qui constituent cette table.  En règle général, plusieurs tables d'entrepôt + dictionnaires => un comptoir. Si plusieurs, alors séparer les valeurs par une virgule.
	"source": <noms des tables de l'entrepot et Files du lac (dictionnaires) dont ce comptoir est constitué separés par des virgules>,
  "pillars": ["decision_makers"|"citizens"|"medias"],
  "projects": <nom du.des projets en_snake_case separés par des virgules>,
  "tags": <tags cumulatifs des tables d'entrepôt et/ou Files dont les observations de ce comptoir sont constituées>,
  "description": <Description en langage naturel du contenu du comptoir et de son utilité>,
  "output": {
    "webapp": <url de la webapp qui pourrait utiliser les données de ce comptoir (le cac échéant)>,
    "graphics": <Path du lac utilisé pour stocker et rassembler les graphiques issus de ce comptoir pour publication> # si plusieurs, alors csv
  }
}
```

```
# Example

{
  "type": "observations",
  "format": "table",
  "content_type": "political_parties_press_release_freq",
  "storage_class": "mart",
  "source_type": "lexicoder_topic_dictionary,political_party_press_release",
	"source": "dict_sentiment,dict_issues,warehouse_political_parties_press_releases",
  "pillars": "decision_makers",
  "projects": "agoraplus,vitrine_democratique",
  "tags": "elxn-qc2022, vitrine_democratique, polqc",
  "description": "Fréquence de publication des communiqués de presse par partis politiques par semaine pour faire un graph à barres et le stocker dans le lac en mode public pour que le publier inline sur le site http://vitrinedemocratique.com",
  "output": {
    "webapp": "http://agora_plus.ca, http://vitrine-democratique.ca"
    "graphics": "political_press_releases_freq/plots"
  }
}
```

5. Populate the table based on the `RETL` repo
    1. Clone or pull the repository `clessn/retl`
    2. In `clessn-blend/pipeline/refiners`, create a new folder with the name `r_name_of_refiner`. The prefix `r_` stands for refiner.
    3. Copy the ***content*** of `clessn/retl` into your new folder. In the folder `r_name_of_refiner`, delete the files and folders starting with a `.` (a period)  ***Attention DO NOT COPY THE FOLDERS STARTING WITH a `.` (a period)***
    4. In your new folder, delete the `README.md`
    5. Rename `template_README.md` as `README.md`.
    6. Open and modify the `README.md` to describe the refiner in non-technical language.
    7. Push your changes made in CLESSN-blend
6. Start coding
    1. Open the Rprojet in the folder of your refiner.
    2. Open `code/code.R` to code as it's the code template. It's content is related to automating your refiner.
    3. Change line 111 and put the refiner name in `snake_case`
    4. In the MAIN section, that's where your code will go and where you'll be able to test it.
    5. Go back into Hublot, look at the table to identify the intrant. Look for the unique key, timestamp, and body. 
    6. In the section `Functions to get data sources from DataLake Hub 3.0`, take the variable name and:

```r
# Template

warehouse_table_name <- "[name of table without prefix]"
datamart_press_release_frequency <- "[name of dynamic without prefix]"

warehouse_df <- clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```

```r
# Example

warehouse_table_name <- "political_parties_press_releases"
datamart_press_release_frequency <- "[nom de la dynamic table, enlever préfixe]"

warehouse_df <- clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```

    7. Execute
    8. Inspect the variables. Everything with the `Data.` prefix are columns in the storage table.
    9. Enrich the table to create a datamart that's useful for research. Add column.
    10. You always need to have a key `dplyr::mutate(key = paste(political_party, week_num, format(Sys.Date(), “%Y”), sep = “”))`
11. To write your dataframe into Hublot via R, add the code below

```r
clessnverse::commit_mart_table(df, datamart_df, key_column = “key”, mode = “refresh”, credentials)}
```

12. **Optional**: To make the refiner create your graph, add your ggplot to the lake using `clessn::commit_lake_item()`

## Cheatsheet

```r
clessnverse::commit_mart_table(df, datamart_df, key_column = “key”, mode = “refresh”, credentials)
clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```
## Parameters

When in doubt, verify the accuracy of the parameters using the [Notion page](https://www.notion.so/clessn/Metadonn-es-dans-les-pipelines-de-la-CLESSN-f99a8cc4271644b08c4b0ac27ae4d55e). The page on Notion takes priority over information in this README.

### Dynamic table parameters

|Parameter|Description|Options|Example|
|---|---|---|---|
|Database||default||
|Table name|Short description of the table in `snake_case` with the prefix `mart_`||mart_political_parties_press_releases_freq|
|Verbose name|Same as table name||mart_political_parties_press_releases_freq|
|Verbose name plural|Same as table name||mart_political_parties_press_releases_freq|

### Metadata parameters

|Parameter|Description|Options|Example|
|---|---|---|---|
|tags|Allows tracability for lake to storage or datamart. Comma-seperated values within quotation marks.||elxn-qc2022, vitrine_democratique, polqc|
|type||table, observations||
|format||table, dataframe||
|pillars|One or more of the three pillars of the CLESSN|decision_makers, citizens, media||
|description|Describe the datamart in free text||Fréquence de publication des communiqués de presse par partis politiques|
|content_type|Describe what's in the table in `snake_case`||political_parties_press_release_freq|
|storage_class|Storage location|lake, storage, mart||
