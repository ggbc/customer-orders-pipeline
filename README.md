# Beecrowd Technical Assignment

This README file guides you through the solution of the technical assignment provided by Beecrowd.

## Technical Decisions

1) First of, in terms of programing languages, Python was chosen because:
a) The exercice has a 'big data' nature and Python is the most used language in this cases
b) Python offers a broader set of tools for dealing with data ingestion, processing etc
c) Among these 3, Python, is easier to maintain and to find suitable professionals when needed.

### Spark vs other tools
Spark should be used to provide scalability when the data volume grows 100x as the document says.

2) The approach
The exercise shows a typical data ingestion, formating, cleaning and processing for big data applications.
A typical approach is to design an ETL pipeline to ingest the data, handle transformations and load the clean/validated data into the final "repository" tool, like a Data Warehouse, allowing analytical queries.

### Batch vs Streaming
Despite JSONL are typically used for streaming, in this exercise we must understand the trade-off between (the complexity and costs of) keeping a whole production environment to process real time data vs processing historically the orders and events.
For the customers data we do not expect many changes, therefore a batch historical processing of the files is a reasonable solution.

Since there is no requirement saying that the data must be processed in real time, I chose batch processing afterall because of the costs.

### How idempotency and reprocessing would work






## Instructions to run locally
