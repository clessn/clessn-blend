# cless-blend

**As of June 2020, only create/change code in the [Pipelines folder](https://github.com/clessn/clessn-blend/tree/main/Pipelines) of this repo**

This repo contains code used to construct the data ETL pipelines between the various data sources required for the CLESSN and the datamarts providing datasets needed for research or visualization.

The data platform of the CLESSN is composed of scripts that move the data across file storage space and databases to make it analytics ready.

The current methodology consists in data pipelines made out of data extractors, data loaders and data refiners.  Each component moves data in turn 
* from its original source to the data lake or file blob
* from the data lake or file  to the data warehouse 
* from the data warehouse to datamarts 

Note that this methodology had been recently implemented at the CLESSN.  Prior to that, web scrapers were developped and often combined all three steps in one (extraction, storage and refining).

The data lake contains items in their raw format.  The datawarehouse and datamarts contain only tabular representations of those items.

To conduct their research or data visualization projects, researchers will consume data only from the datamarts they produce.  They produce datamarts by writing refiners that feed from the data warehouse.

Researchers will eventually be able to make some datamarts public in order to be shared them with the community.

Here a view of the CLESSN data platform
![Alt text](https://github.com/clessn/diagrams/blob/master/infra/data_platform_clessn.drawio.png)

