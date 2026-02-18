from pyspark.sql import functions as F
import logging
import get_logger

class DataIngestor:
    def __init__(self, logger : Logger, spark):
        self.spark = spark
        self.logger = logger

    def read_customers(self, path):
        # read daily sent customers csv
        df = None
        try:
            df = self.spark.read.csv(path, header=True, sep="|")
        except Exception as e:
            self.logger.error(f"Error reading customers from path {path}: {e}")
            raise ValueError(f"Error reading customers from path {path}: {e}")
        return df

    def read_orders(self, path):
        df = None
        try:
            df = self.spark.read.option("multiline", "true").json(path)
        except Exception as e:
            self.logger.error(f"Error reading orders from path {path}: {e}")
            raise ValueError(f"Error reading orders from path {path}: {e}")
        return df

    def read_events(self, path):
        df = None
        try:
            df = self.spark.read.json(path)
        except Exception as e:
            self.logger.error(f"Error reading events from path {path}: {e}")
            raise ValueError(f"Error reading events from path {path}: {e}")
        return df

