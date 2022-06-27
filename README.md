# cless-blend

**WARNING: As of June 2022, only create/change code in the [Pipelines folder](https://github.com/clessn/clessn-blend/tree/main/Pipelines) of this repo.**

This repo contains code used to construct the data ETL pipelines between the various data sources required for the CLESSN and the datamarts providing datasets needed for research or visualization.

The data platform of the CLESSN is composed of scripts that move the data across the internet, file storage space, and databases, to make it ready for analytics.

The scripts are the active components of the pipelines, and the storage and databases are passives components.

## Extractors, loaders and refiners : the active components of ETL pipelines

The current methodology consists in data pipelines made out of **data extractors**, **data loaders** and **data refiners**. Each component moves data in turn 
* from its original source to the **data lake** or **files blob storage**
* from the data lake or file  to the **data warehouse**
* from the data warehouse to **datamarts**

Side Note: There are exceptions by which an extractor might not be needed in a pipeline. For instance, a researcher could very well obtain raw data in the form of a csv or pdf file, such as a university paper, a political parti election program, the answers of survey questions etc. and would store it manually directly in the **data lake** or **files blob storage**. In that case the researcher plays the role of the extractor.

Note that this ETL methodology has recently been implemented at the CLESSN. Prior to that, web scrapers were developped and often combined all three steps in one (extraction, storage and refining).

## Data Lake, files blob storage, data tables : the passives components of ETL pipelines
The data lake contains items in their raw format.  The datawarehouse and datamarts contain only tabular representations of those items.

To conduct their research or data visualization projects, researchers will consume data only from the datamarts they produce. They produce datamarts by writing refiners that feed from the data warehouse.

As much as possible, a datamart will serve multiple purposes that require data of the same nature. However, it would be an error to squeeze too much data from the warehouse in one datamart, for the sole purpose of avoiding to have to write a refiner.

Researchers will eventually be able to make some datamarts public in order to be shared them with the community.

## Development methodology
It is important to respect the CLESSN development methodology and environment requirements to create actibve **and** passive components of a data pipeline. See the [Pipelines folder](https://github.com/clessn/clessn-blend/tree/main/Pipelines) for more details.

Here's a view of the CLESSN data platform:
![Alt text](https://github.com/clessn/diagrams/blob/master/infra/data_platform_clessn.drawio.png)

