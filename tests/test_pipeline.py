import pytest
from src.utils import get_spark_session
from src.transformation import DataTransformer

@pytest.fixture(scope="session")
def spark():
    return get_spark_session("UnitTests")

def test_customer_deduplication(spark):
    data = [("1", "Alice", "a@a.com", "BR", "2023-01-01"),
            ("1", "Alice", "a@a.com", "BR", "2023-01-01")]
    columns = ["customer_id", "name", "email", "country", "created_at"]
    df = spark.createDataFrame(data, columns)
    
    transformed_df = DataTransformer.transform_customers(df)
    assert transformed_df.count() == 1  # Make sure the duplicate is removed

def test_order_status_normalization(spark):
    customer_data = [("1", "Alice")]
    order_data = [("101", "1", 100.0, "BRL", "completed", "2023-07-10")]
    
    df_cust = spark.createDataFrame(customer_data, ["customer_id", "name"])
    df_ord = spark.createDataFrame(order_data, ["order_id", "customer_id", "amount", "currency", "status", "order_date"])
    
    transformed_ord = DataTransformer.transform_orders(df_ord, df_cust)
    assert transformed_ord.collect()[0]["status"] == "COMPLETED" # Guarantees status is normalized to uppercase