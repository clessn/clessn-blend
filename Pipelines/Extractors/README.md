# Data pipeline *extractors*
This the folder containing the **extractors** of the data pipelines of the CLESSN.  Extractors are the first step for collecting and moving data across the CLESSN **data platform**.  They consists in grabing the data from the platforms they are hosted on and store them in the CLESSN **data lake** or **file blob storage** as they are, without any transformation, while tagging them with meta data.

The meta data applied to the objects stored in the data lake are a way to retrieve them in bulk for further processing.

For more information on the CLESSN data platform, see [this document](https://github.com/clessn/diagrams/blob/master/infra/TargetDataPlatformCLESSN.drawio.pdf).
