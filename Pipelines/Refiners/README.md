# Refiners

## Description

Data refiners take the information from the data warehouse and prepare them for data marts.

## Create a refiner

### Prerequisites

* R
* R environ: it allows you to hide your password and other sensitive information in a project. To configure it, follow the instructions in the repo `clessn/Renviron_tutorial`.
* Access to [Hublot](https://clhub.clessn.cloud/admin/)
* `clessnverse` R package: download it in R using the code `devtools::install_github("clessn/clessnverse")`

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
  "tags": "",
  "type": "table",
  "format": "dataframe",
  "pillars": "",
  "description": "",
  "content_type": "",
  "storage_class": "mart"
}
```

```
# Example

{
  "tags": "elxn-qc2022, vitrine_democratique, polqc",
  "type": "observations",
  "format": "table",
  "pillars": "decision_makers",
  "description": "Fréquence de publication des communiqués de presse par partis politiques",
  "content_type": "political_parties_press_release_freq",
  "storage_class": "mart"
}
```

5. Populate the table based on the `RETL` repo
    1. Clone or pull the repository `clessn/retl`
    2. In `clessn-blend/pipeline/refiners`, create a new folder with the name `r_name_of_refiner`. The prefix `r_` stands for refiner.
    3. Copy the ***content*** of `clessn/retl` into your new folder.
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
    6. Take the variable name and:

```r
# Example

{
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

### Dynamic table

|Parameter|Description|Options|Example|
|---|---|---|---|
|Database||default||
|Table name|Short description of the table in `snake_case` with the prefix `mart_`||mart_political_parties_press_releases_freq|
|Verbose name|Same as table name||mart_political_parties_press_releases_freq|
|Verbose name plural|Same as table name||mart_political_parties_press_releases_freq|

### Metadata
|Parameter|Description|Options|Example|
|---|---|---|---|
|tags|Allows tracability for lake to storage or datamart||elxn-qc2022, vitrine_democratique, polqc|
|type||table, observations||
|format||table, dataframe||
|pillars|One or more of the three pillars of the CLESSN|decision_makers, citizens, media||
|description|Describe the datamart in free text||Fréquence de publication des communiqués de presse par partis politiques|
|content_type|Describe what's in the table in `snake_case`||political_parties_press_release_freq|
|storage_class|Storage location|lake, storage, mart||
