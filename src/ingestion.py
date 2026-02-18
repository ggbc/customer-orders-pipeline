from pyspark.sql import functions as F
from src.utils import get_logger

class DataIngestor:
    def __init__(self, logger : Logger, spark):
        self.spark = spark
        self.logger = logger
        self.errors_path = "output/ingestion_errors"

    def _isolate_and_log(self, df, condition, source_name, reason):
        """
        Isolates rejected data and logs the occurrence. 
        """
        rejected_df = df.filter(~condition)
        if rejected_df.count() > 0:
            self.logger.warning(f"Inconsistent records detected in {source_name}: {reason}")
            # Adds metadata for debugging
            rejected_df.withColumn("rejection_reason", F.lit(reason)) \
                       .withColumn("ingestion_timestamp", F.current_timestamp()) \
                       .write.mode("append").parquet(f"{self.errors_path}{source_name}")
        return df.filter(condition)

    def read_customers(self, path):
        # read daily sent customers csv
        try:
            self.logger.info(f"Ingesting customers from {path}")            
            df = self.spark.read.csv(path, header=True, sep="|")

            # Validation: customer_id must be present
            valid_condition = F.col("customer_id").isNotNull()
            df_cleaned = self._isolate_and_log(df, valid_condition, "customers", "Null customer_id")
            
            return df_cleaned            
        except Exception as e:
            self.logger.error(f"Error reading customers from path {path}: {e}")
            raise ValueError(f"Error reading customers from path {path}: {e}")


    def read_orders(self, path):

        try:
            self.logger.info(f"Ingesting orders from {path}")
            df = self.spark.read.json(path)

            # Schema Validation: order_id and amount must be valid [cite: 68]
            valid_condition = (F.col("order_id").isNotNull()) & (F.col("amount") >= 0)
            df_validated = self._isolate_and_log(df, valid_condition, "orders", "Invalid Schema or Amount")

            return df_validated
        except Exception as e:
            self.logger.error(f"Error reading orders from path {path}: {e}")
            raise ValueError(f"Error reading orders from path {path}: {e}")


    def read_events(self, path):
        try:
            self.logger.info(f"Ingesting events from {path}")
            df = self.spark.read.json(path)
            
            # Basic schema check
            valid_condition = F.col("event_id").isNotNull()
            return self._isolate_and_log(df, valid_condition, "events", "Missing event_id")
        except Exception as e:
            self.logger.error(f"Error reading events from path {path}: {e}")
            raise ValueError(f"Error reading events from path {path}: {e}")


