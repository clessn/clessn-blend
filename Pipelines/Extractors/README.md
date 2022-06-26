# Data pipeline *extractors*
This the folder containing the **extractors** of the data pipelines of the CLESSN.  Extractors are the first step for collecting and moving data across the CLESSN **data platform**.  They consists in grabing the data from the platforms they are hosted on and store them in the CLESSN **data lake** or **file blob storage** as they are, without any transformation, while tagging them with meta data.

Amongst other uses, the meta data applied to the objects stored in the data lake and file blob storage is used to retrieve them in bulk for further processing.

# Development methodology specific to data extractors
Here is a schema of an data ETL pipeline and the position of an extractor in it
![Alt](https://github.com/clessn/diagrams/blob/master/infra/pipeline_schema.drawio.png).
