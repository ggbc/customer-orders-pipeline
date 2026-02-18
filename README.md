# Beecrowd Technical Assignment

This README file guides you through the solution of the technical assignment provided by Beecrowd.

## The approach
The exercise shows a typical data ingestion, transformation and storage for big data applications.
A common approach is to design an ETL pipeline to ingest the data, handle transformations and load the clean/validated data into the final "repository" tool, like a Data Warehouse, allowing analytical queries.
The proposed solution is having 3 layers in the data architecture: raw (for the data ingested as-is), trusted (data transformed with formating, validation etc and/or business rules) and refined (aggregated validated data).

## Architecture & Design
### Data flow from ingestion to output
Among the tools mentioned, Airflow would be responsible for orchestrating the flow in the pipeline.

The pipeline could be represented then, this way:

Spark >> dbt >> Snowflake

meaning...

- (a) Spark is responsible for ingesting the data from the different sources. It is strongly related to the 'raw' layer in the architecture.
- (b) dbt, for transforming and testing the data by applying the many rules informed in the document (removing dupplicates, invalid data, changing inconsistencies etc); Dbt is present in the 'trusted' and 'refind' layers in the architecture;
- (c) Snowflake, as the DW, would be the repository of the data in each layer. It is where the business users would query the DW with OLAP operations.

 
## Technical Decisions

1) In terms of programing languages, Python was chosen because:
- a) The exercise has a 'big data' nature and Python is the most used language in this cases
- b) Python offers a broader set of tools for dealing with data ingestion, processing etc. For example, PyTest and Hashing/PII libs.
- c) Among these 3, Python, is easier to maintain and to find suitable professionals when needed.

2) Spark vs other tools
Spark supports integrating batch and streaming processing in a hybrid approach, without needing to use lots os different libs.
Also, thinking of scalability ad production readiness, when the data volume grows 100x as the document says, Spark would be the best choice since it is ready for distributed processing.

3) Batch vs Streaming
Despite JSONL are typically used for streaming, in this exercise we must understand the trade-off between (the complexity and costs of) keeping a whole production environment to process real time data vs processing historically the orders and events. Besides, since customers.csv arrives daily it could be understood as a "slowly changing dimension". We could process orders and events together to simplify the 'joins' that must be made, ensuring integrity in the DW.

Analysing the file samples, we can say that:
- For the customers data we do not expect many changes, therefore a batch historical processing of the files is a reasonable solution.
- Events are supposed to be used for logging purposes, not to any real time requirement. Therefore, we could enqueue them and batch-process after a while.
- Orders present a similar case. Since the target here is not to build the transactional application but a analytical one, we are not expected to process the orders in real time. So, a batch processing could be perfectly used.

Should consider using streaming for the events, we would have to use a queue in front of the ingestion for that case. As a consequence, the pipeline would have to be on 24/7, raising the infrastructure costs and changing the solution.

Conclusion: since there is no requirement saying that the data must be processed in real time, I chose batch processing afterall because of the costs.

4) How idempotency and reprocessing would work
Since batch ingestion was chosen, processing would be naturally idempotent, which means: every time we reprocess the pipeline with the same data input, we can expect the same data output. This should be guaranteed by overwriting the data in the repository.


## Production Readiness
Considering the production environment, we would prefer to use serverless processing due to the costs involved.
A serverless pay-per-use service is cheaper and more prepared (considering availability) than keeping a virtual machine to run the applications we need.

In this case, Spark would not run locally anymore, but on clusters in the cloud.

That said, Snowflake would be integrated to the pipeline by providing a repository for the data in each layer of the architecture.
Since Snowflake is cloud provider agnostic, we could let the decision of which provider to use for later. What would change is the cloud 'bucket' service to be used. For instance, with GCP we would use Cloud Storage for the output files in the 'raw' layer.
In the upper layers, we would abstract storage to inside Snowflake in order to make better use of its performance with the most 'noble' data.

Also, when in production we expect that such '100x times' raise in data volume but Spark is ready to deal with that.

The bottleneck would be having too many files to process in the JSON and JSONL cases.


## Data Governance

To handle PII questions presented in information such as customers' name and email, the solution must support Data Masking (Anonimization), preferably in both trusted and refined layers.

To handle validation (of schemas) and inconsistent records the solution must log make Validation steps regarding business rules, database referetial integrity checks and schema validation.

Regarding access to the data, Snowflake will handle permissions on the upper layers (Trusted and Refined).

According to the instructions we must log every rejected record for further analysis.

Evolutions in schema must be handled separately. If a new field is asked by the business, for example, this must be treated with version control and backwards compatibility so that we can keep traceability and data lineage always updated.

## Scaling

In order for the data grows 100x times, we must use partitioning. A good strategy would be partitioning by 'date hierarchy' (year/month/date).

The previous decision of using Spark also helps here. Spark is made for distributed processing which helps on scalling, nativelly.


## Instructions to run locally

### Prerequisites
- Python 3.8+
- Java 8 or 11 (set on `JAVA_HOME` environment variable)
- Docker (based in a Hadoop image)

### Installation
1. `python -m venv venv`
2. `source venv/bin/activate` 
3. `pip install -r requirements.txt`

### Execution
- **Run Pipeline:** `python main.py`
- **Run Tests:** `pytest tests/`