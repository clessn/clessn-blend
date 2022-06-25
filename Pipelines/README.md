# The Pipelines folder in clessn-blend

This folder contains the code of the data pipelines extractors, loader and refiners.

The current methodology consists in data pipelines made out of data extractors, data loaders and data refiners.  Each component moves data in turn 
* Extractors move data from its original source to the data lake or file blob and store it in its original form in the data lake after applying meta data to it.
* Loaders move data from the data lake or file blob to a data warehouse table.  A table also has meta data applied to it.
* Refiners are typically created by researchers and move from one or more data warehouse tables to one datamarts.

The definition of the data lake, data warehouse and datamarts can be found below

![alt](https://github.com/clessn/diagrams/blob/master/infra/definitiond_lake_warehouse_datamart.drawio.png)
