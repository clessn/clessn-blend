# cless-blend
This repo contains code used to construct the data ETL pipelines between the various data sources required for the CLESSN and datamarts providing datasets needed for research or visualization.

The data platform of the CLESSN is composed of file storage space, databases and scripts that move the data amongst them to make it analytics ready.

The current methodology consists in data pipelines made out of data extractors, data loaders and data refiners.  Each component moves data in turn 
* from its original source to the data lake or file blob
* from the data lake or file blob to the data warehouse 
* from the data warehouse to datamarts 

Note that this methodology is recently implemented at the CLESSN.  Prior to that, web scrapers were developped and often souls 

We say that the data lake contains items.  The datawarehouse and datamarts contain only tabular database tables.

To conduct their research or data visualization projects, researchers will consume data only from the datamarts they produce.  They produce datamarts by writing refiners that feed from the data warehouse.

Researchers will eventually be able to make some datamarts public in order to be shared them with the community.
