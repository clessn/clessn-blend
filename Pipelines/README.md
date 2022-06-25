# The Pipelines folder in clessn-blend

This folder contains the code of the data pipelines extractors, loader and refiners.  Once all the old scrapers have been converted to this new ETL methodology, this folder will become the root of this repo.
The current ETL methodology used at the CLESSN consists in data pipelines made out of data extractors, data loaders and data refiners.  Each component moves data from one level of refining to the next: 
* Extractors move data from its original source to the data lake or file blob and store it in its original form after applying meta data to it.
* Loaders move data from the data lake or file blob to a data warehouse table.  A data warehouse table contains therefore a tabular representation of the raw data.  A table also has meta data applied to it.
* Refiners are typically created by researchers and move data from one or more data warehouse tables (and exceptionally from the file blobs) to one datamarts.  A datamart is a single table stored in a database, containing all rows and columns necessary to conduct a research or produce a visualization artefact.  The columns of a datamart are the result of calculations and transformations that enrich the data from the data warehouse in order to answer business related questions, solve specific problems, or visualize the information it contains.  A datamart table also has meta data applied to it.

Generally speaking, meta data provides a way to select data in bulk from the lake, warehouse or marts.  It also is a means to provide accurate traceability of the data from its very original source to a datamart.  Finally, meta data allows to precisely identify the nature of the data and its characteristics.

The benefits of this ETL methodology are
* To allow reuse or previously collected data for other purposes without having to rewrite new code to collect it again.
* To facilitate the creation and maintenance of an accurate inventory of the data and the algorithms that harvest it.
* To ease the sharing of datasets issued from datamarts, through a data catalog.
* To prevent having to write multiple R scripts by different people in the CLESSN for the same purposes, by facilitating the reuse of data stored in the data warehouse or consuming existing datamarts.
* To accelerate the combination of data of various formats comming from various sources.

The definition of the data lake, data warehouse and datamarts can be found below

![alt](https://github.com/clessn/diagrams/blob/master/infra/definitiond_lake_warehouse_datamart.drawio.png)
