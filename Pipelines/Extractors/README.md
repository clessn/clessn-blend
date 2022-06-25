# Data pipeline *extractors*
This the folder containing the **extractors** of the data pipelines of the CLESSN.  Extractors are the first step for collecting and moving data across the CLESSN **data platform**.  They consists in grabing the data from the platforms they are hosted on and store them in the CLESSN **data lake** or **file blob storage** as they are, without any transformation, while tagging them with meta data.

The meta data applied to the objects stored in the data lake is a way to retrieve them in bulk for further processing.

Here is a schema of an data ETL pipeline and the position of an extractor in it
![Alt](https://github.com/clessn/diagrams/blob/master/infra/pipeline_schema.drawio.png).
