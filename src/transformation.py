from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, when, to_date, to_timestamp, sha2
from logging import Logger


class DataTransformer(ABC):
    """SOLID: S: Single Responsibility & O: Open/Closed."""
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

class CustomerTransformer(DataTransformer):
    """PII, Deduplication & customers clean-up."""
    def transform(self, logger: Logger, df: DataFrame) -> DataFrame:
        try:
            # 1. Deduplication
            # 2. Data masking (emails) by hashing 
            # 3. Nulls and dates handling            
            df = df.dropDuplicates(["customer_id"]) \
                .withColumn("email_masked", sha2(col("email"), 256)) \
                .withColumn("name", when(col("name") == "", "Unknown").otherwise(col("name"))) \
                .withColumn("country", when(col("country").isNull(), "Unknown").otherwise(col("country"))) \
                .withColumn("created_at", to_date(col("created_at")))
        except Exception as e:
            logger.error(f"Error transforming customers: {e}")
            raise ValueError(f"Error transforming customers: {e}")            
        return df

class OrderTransformer(DataTransformer):
    """Normalization and Quality Filtering of Orders."""
    def transform(self, logger: Logger, df: DataFrame) -> DataFrame:
        try:
            # 1. Quality filter: only positive values and non null IDs allowed
            # 2. Statuses normalization
            # 3. Date format handling (multiple formats)
            df = df.filter((col("amount") > 0) & (col("customer_id").isNotNull())) \
                .withColumn("status", lower(trim(col("status")))) \
                .withColumn("order_date", 
                    when(col("order_date").rlike("^\d{2}-"), to_date(col("order_date"), "dd-MM-yyyy"))
                    .otherwise(to_date(col("order_date"), "yyyy-MM-dd")))
        except Exception as e:
            logger.error(f"Error transforming orders: {e}")
            raise ValueError(f"Error transforming orders: {e}")
        return df

class EventTransformer(DataTransformer):
    """Events validation and normalization."""
    def transform(self, logger: Logger,df: DataFrame) -> DataFrame:
        try:
            # 1. Invalid timestamp filtering
            # 2. Event type normalization (lowercase, trim)
            df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
                .filter(col("event_timestamp").isNotNull()) \
                .withColumn("event_type", lower(trim(col("event_type"))))
        except Exception as e:
            logger.error(f"Error transforming events: {e}")
            raise ValueError(f"Error transforming events: {e}")
        return df