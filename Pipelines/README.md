# The Pipelines folder in clessn-blend

This folder contains the code of the data pipelines extractors, loader and refiners.

The current ETL methodology used at the CLESSN consists in data pipelines made out of data extractors, data loaders and data refiners.  Each component moves data from one level of refining to the next: 
* Extractors move data from its original source to the data lake or file blob and store it in its original form after applying meta data to it.
* Loaders move data from the data lake or file blob to a data warehouse table.  A data warehouse table contains therefore a tabular representation of the raw data.  A table also has meta data applied to it.
* Refiners are typically created by researchers and move data from one or more data warehouse tables (and exceptionally from the file blobs) to one datamarts.  A datamart is a single table stored in a database, containing all rows and columns necessary to conduct a research or produce a visualization artefact.  The columns of a datamart are the result of calculations and transformations that enrich the data from the data warehouse in order to answer business related questions, solve specific problems, or visualize the information it contains.  A datamart table also has meta data applied to it.

Generally speaking, meta data provides s way to select data in bulk from the lake, warehouse or marts.  It also is a means to provide accurate traceability of the data from its very original source to a datamart.

The definition of the data lake, data warehouse and datamarts can be found below

![alt](https://github.com/clessn/diagrams/blob/master/infra/definitiond_lake_warehouse_datamart.drawio.png)
