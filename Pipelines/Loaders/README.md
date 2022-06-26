# Data pipeline *loaders*
This is the folder containing the **loaders** of the data pipelines of the CLESSN.  Loaders are the second step for collecting and moving data across the CLESSN **data platform**.  They consists in grabing the data from the **data lake** or **files blob** storage and store as structured data in the CLESSN **data warehouse** in the form of a rectangulat tables with rows and columns.

Columns are also known as variables.
Rows ara also called observations.

Data warehouse tables contain observations from the real world, as they have been harvested by the pipelines **extractors**, in their simplest form.  They represent a reality that is not yet combined with another reality or any type of dimension.

Data warehouse tables are tagged with meta data that allow to link them with the data lake items or data files they were constructed with. 

That metadata is also used to retrieve observations in bulk from multiple warehouse tables further in the processing of the data pipeline to create datamarts.

Here is a schema of an data ETL pipeline and the position of loader in it
![Alt](https://github.com/clessn/diagrams/blob/master/infra/pipeline_schema.drawio.png).
