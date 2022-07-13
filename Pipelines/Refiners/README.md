# Refiners

## Description

Hublot: https://clhub.clessn.cloud/admin/

## Create a refiner

Follow the following steps to create a refiner.

Example: Create a refiner for a datamart that will calculate the number of press releases published every week by a party.

1. On the admin site of Hublot, open Dynamic table. 
2. Create a table and fill in the form:
    1. Database: default
    2. For table name, verbose name and verbose name plural: `mart_table_name` (`mart_` is the prefix for datamart table, replace `table_name` by your table name) (same)

3. Create datamart metadata
    1. In the Metadata field, change the view from "Tree" to "Code"
    2. Insert the following template

Template

```
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

Example

```
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

    3. Fill in the metadata elements
    4. Press Save

5. Populate the table based on the `RETL` repo
    1. Clone or pull the repository `clessn/retl`
    2. In `clessn-blend/pipeline/refiners`, create a new folder with the name `r_name_of_refiner`. The prefix `r_` stands for refiner.
    3. Copy the ***content*** of `clessn/retl` into your new folder.
    4. In your new folder, delete the `README.md`
    5. Rename `template_README.md` as `README.md`.
    6. Open and modify the `README.md` to describe the refiner in non-technical language. 
    7. Push in CLESSN-blend
6. Start coding
    1. Open Rprojet in the folder of your refiner.
    2. Open `code/code.R`: open to code as it's the code template. Content is related to automating your refiner.
    3. Change line 111 and put the refiner name in snake_case 
    4. In MAIN: that's where your code will go and where you'll be able to test it.
    5. Go back into Hublot, look at table to identify the intrant. Look for unique key, timestamp, and body. 
    6. Take the variable name and:

```r
{
warehouse_table_name <- "political_parties_press_releases"
datamart_press_release_frequency <- "[nom de la dynamic table, enlever préfixe]"

warehouse_df <- clessnverse::get_warehouse_table(warehouse_table_name, credentials)
```

1. Execute
2. Inspect the variables. Everything with Data. prefix are columns in the storage table.
3. Enrichir la table to create a datamart that's useful for research. Add column.
4. You always need to have a key `dplyr::mutate(key = paste(political_party, week_num, format(Sys.Date(), “%Y”), sep = “”))`
5. Wrie dataframe to Hublot via R

```r
clessnverse::commit_mart_table(df, datamart_df, key_column = “key”, mode = “refresh”, credentials)}
```

1. Optional: add gglopt so that the refiner can create your graph. You can add your ggplot to the lake with `clessn::commit_lake_item()`

## R enviro

R enviro: go in clessn-blend. Follow instructions in repo Renviro-tutorial as everything is explained. It creates a file in your R inventory.

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



