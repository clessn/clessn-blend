# The Pipelines folder in clessn-blend

This folder contains the code of the data pipelines extractors, loader and refiners.  Those are also referred to as active components of data ETL pipelines.
Once all the old scrapers will been converted to this new ETL methodology, this folder will become the root of this repo.
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

# Development methodology and containerization
At the CLESSN, we designed a methodology for developping extractors, loaders and refiners so that they can be automated and scheduled to run in **Docker containers** on the **VALERIA OpenShift** (k8s) infrastructure.  Therefore, it is important to follow the development methodology of extractors.  

There are a few requirements and steps that you must comply with when writing an active component of a pipeline:
* All extractors, loaders and refiners must be written within the **clessn-blend** repo, inside the *Pipelines* (this) folder.
* Hide the credentials for connecting to **HUBLOT** from your code.
* Use the **retl** repo as a template to your extractor environment within the **clessn-blend** repo.
* Pre-install packages using **renv**.
* Test the container on your machine before deploying.

Note : eventually, the Pipelines folder will become the **root** of the clessn-blend repo, once all the old scrapers have been converted to the new CLESSN data-platform design paradigms.

Also, because we want to develop and test them before we **containerize** them, and because they need to authenticate against **HUBLOT** to store data in it, the environment they run on must either use OS defined environment variables or use the .Renviron system to store credentials in order to connect to HUBLOT.

This is based on the [retl](https://github.com/clessn/retl) repository.
See the [README.md](https://github.com/clessn/retl/blob/master/README.md) of the retl repo for more details.
