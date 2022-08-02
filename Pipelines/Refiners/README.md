# Refiners

## Description

A data refiner takes the information from the data warehouse and prepares it for a data mart.

## 1. Draw your refiner

<img width="752" alt="Capture d’écran, le 2022-07-28 à 11 21 30" src="https://user-images.githubusercontent.com/49455681/181582125-a3d77bb4-88ee-4df3-bcd0-694ee9a38939.png">


You'll find the template in [clessn/diagrams](https://github.com/clessn/diagrams/tree/master/raffineurs). 

## 2. Create your refiner

### Prerequisites

* R
* R environ: it allows you to hide your password and other sensitive information in a project. To configure it, follow the instructions in the repo [`clessn/Renviron_tutorial`](https://github.com/clessn/Renviron_tutorial).
* Access to
	* clessn repositories on GitHub
	* [Hublot](https://clhub.clessn.cloud/admin/)
	* Hub 2.0 (temporary, for access to data in Hub 2.0 not yet in Hublot)
* Install CLESSN R packages
  * `clessnhub`: `devtools::install_github("clessn/clessn-hub-r")` (temporary, for access to data in Hub 2.0 not yet in Hublot) (Contact: Adrien Cloutier, Olivier Banville)
  * `clessnverse` : `devtools::install_github("clessn/clessnverse")` (Contact: Adrien Cloutier, Hubert Cadieux, Olivier Banville)
  * `hublot` : `devtools::install_github("clessn/hublotr")` (Contact: Adrien Cloutier, Olivier Banville)

### Tutorial

In this example, we create a refiner for a datamart that will calculate the number of press releases published every week by a party.

1. On the admin site of [Hublot](https://clhub.clessn.cloud/admin/), open Dynamic table. 
2. Click "Add dynamic table", and fill in the form according to the Dynamic table parameters at the bottom of this document
3. Create datamart metadata
    1. In the Metadata field, change the view from "Tree" to "Code"
    2. Insert the following template and fill in according to the Metadata parameters at the bottom of this document.
    3. Do not check "Is public".

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
    "graphics": nom_du_mart/plot # toujours écrire le nom du mart, suive d'un / et de «plot» 
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
    "webapp": "http://agora_plus.ca, http://vitrine-democratique.ca",
    "graphics": "political_press_releases_freq/plots"
  }
}
```

4. Click "SAVE". So far so good!!
5. Populate the table based on the [RETL](https://github.com/clessn/retl) repo
    1. Clone or pull the repository `clessn/retl`
    2. In `clessn-blend/pipeline/refiners`, create a new folder with the name `r_name_of_refiner`. The prefix `r_` stands for refiner.
    3. Copy the ***content*** of `clessn/retl` into your new folder. Windows users: in the folder `r_name_of_refiner`, delete the files and folders starting with a `.` (a period).
    4. In your new folder, delete the `README.md`.
    5. Rename `template_README.md` as `README.md`.
    6. Open and modify the `README.md` to describe the refiner in non-technical language.
    7. Push your changes made in CLESSN-blend.
    8. Start coding.

## 3. Start coding your refiner

Now that the structure of your refiner is setup, it's time to start writing in the script that tells the refiner *what* to do. Start by following these simple steps:
1. Open the Rprojet in the folder of your refiner.
2. Open `code/code.R` to code. This file is the **code template**. Its additional content and structure are related to **automating** your refiner.
    
The refiner template is divided in 6 main sections:
- Functions
- Functions to get data sources from Hub 3.0
- Functions to get data sources from Hub 2.0 (temporary)
- Functions to get external data sources (e.g. Dropbox)
- Main
- Error handling

Each of these sections and what to write in them are detailed in the following sections. As you read this, you can follow along with the example in the annex (coming soon!).

Since your script is meant to run automatically, exclude `library(package_name)` and `install.packages("package_name")`.

### Functions
The *Functions* section is used to define the functions that will be used in the **analysis**. These functions will be applied on the datasets loaded in the next sections to analyze them. Examples of functions to define in this section are included in the annex.

### Functions to get data sources from Hub 3.0
The *Functions to get data sources from Hub 3.0* section is used to define the functions that will fetch the relevant **warehouse tables** and **files** (eg: dictionaries) in [Hublot](https://clhub.clessn.cloud/admin/).

Examples of relevant functions from `clessnverse` and `hublot` to use in this section are included in the annex as well as common operations.

### Functions to get data sources from Hub 2.0 (temporary)
The *Functions to get data sources from Hub 2.0 (temporary)* section is used to define the functions that will fetch data from Hub 2.0. Grouping these functions in the same section will allow for an easier transition when the migration towards Hublot will be done. Examples of relevant functions from `clessnhub` to use in this section are included in the annex as well as common operations.

Note: The *Data management committee* is currently in the process of migrating data and scrapers from the former Hub (Hub 2.0) to Hublot while sticking to the industry standards. Until all the data is properly stored in Hublot, some refiners will need to access datasets stored in Hub 2.0 using functions from the `clessnhub` package.

### Functions to get external data sources (Dropbox)
If one of the datasets used to create the mart is from an external data source, a ***really*** good practice would be to advise the *Data management committee* (Patrick Poncet, Judith Bourque, Jeremy Gilbert, Adrien Cloutier or Hubert Cadieux) so the process of integrating this dataset to the Data Lake can begin. A ***bad*** practice would be to ignore this suggestion. When the committee is advised, **temporary** functions to retrieve the external files can be included in the *Functions to get external data sources (Dropbox)*. No example of these types of functions have been made yet because this is a ***bad*** practice.

### Main
The fun part!

The *Main* section is where you analyze or refine the data with the functions defined in *Functions*. These data are loaded **in** `main()` using the functions defined in *Functions to get datasources from Hub 3.0*, *Functions to get data sources from Hub 2.0* and *Functions to get external data sources (Dropbox)*.  

```r
main <- function() {
    ###########################################################################
    # Define local objects of your core algorithm here
    # Ex: parties_list <- list("CAQ", "PLQ", "QS", "PCQ", "PQ")


    ###########################################################################
    # Start your main script here using the best practices in activity logging
    #
    # Ex: warehouse_items_list <- clessnverse::get_warehouse_table(warehouse_table, credentials, nbrows = 0)
    #     clessnverse::logit(scriptname, "Getting political parties press releases from the datalake", logger) 
    #     lakes_items_list <- get_lake_press_releases(parties_list)
    #      



}

```

    4. In the MAIN section, that's where your code will go and where you'll be able to test it.
    5. Go back into Hublot, look at the table to identify the intrant. Look for the unique key, timestamp, and body. 
    6. In the section `Functions to get data sources from DataLake Hub 3.0`, take the variable name and:

3. Change line 118 and put the refiner name in `snake_case`
    
    ```r
    if (!exists("scriptname")) scriptname <<- "r_name_of_your_refiner"
    ```

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

### Cheatsheet

```r
clessnverse::commit_mart_table(df, datamart_df, key_column = “key”, mode = “refresh”, credentials)
clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```
### Parameters

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


## Common operations
TO DO LIST
- Functions to get data from Hub 3.0
- Functions to get data from Hub 2.0
- creating hub 3 filter
- get_warehouse_table
- get a dictionary
- upload a mart table
- what to put in main
