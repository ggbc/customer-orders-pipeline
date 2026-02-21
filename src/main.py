from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, when, to_date, to_timestamp, sha2, lit
from abc import ABC, abstractmethod
import os, logging
from ingestion import DataIngestor
from transformation import CustomerTransformer, OrderTransformer, EventTransformer
import utils

# Configuring logs for monitoring and debugging 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#------------------------------------------------------------------
# PIPELINE ORCHESTRATOR 
#------------------------------------------------------------------
class DataPipeline:
    """SOLID: D: Dependency Inversion - decouples pipeline orchestration from specific implementations of ingestion and transformation."""
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self):
        # 1. Ingestion ('Raw' layer)
        raw_customers = DataIngestor(logger, self.spark).read_customers("customers.csv")
        raw_orders = DataIngestor(logger, self.spark).read_orders("orders.json")
        raw_events = DataIngestor(logger, self.spark).read_events("events.jsonl")

        # 2. Transformation ('Trusted' layer)
        dim_customer = CustomerTransformer().transform(logger, raw_customers)
        fact_orders = OrderTransformer().transform(logger, raw_orders)
        trusted_events = EventTransformer().transform(logger, raw_events)

        # 3. Referencial Integrity
        fact_events = trusted_events.join(dim_customer, "customer_id", "inner") \
            .select(trusted_events["*"])
        
        # 4. Output ('Refined' layer)
        self.write_output(dim_customer, "dim_customer")
        self.write_output(fact_orders, "fact_orders")
        self.write_output(fact_events, "fact_events")

    def write_output(self, df: DataFrame, name: str):
        try:
            output_path = f"output/{name}"
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Dataset {name} successfully written in parquet.")
        except Exception as e:
            logger.error(f"Error writing dataset {name}: {e}")


#------------------------------------------------------------------
# PIPELINE EXECUTION
#------------------------------------------------------------------
if __name__ == "__main__":
    spark_session = utils.get_spark_session()

    pipeline = DataPipeline(spark_session)
    pipeline.run()