import pytest
import os
import uuid
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("DeltaTableWriterTests") \
        .master("local[*]") \
        .getOrCreate()


@pytest.fixture(scope="session")
def scratch_schema():
    schema = os.getenv("SCRATCH_SCHEMA", "default")
    parts = schema.split(".")
    spark = SparkSession.builder.getOrCreate()

    if len(parts) == 2:
        catalog, schema_only = parts
        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        except Exception:
            pass  # Local Spark will fail here — that's fine
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_only}")
    elif len(parts) == 1:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    else:
        raise ValueError(f"Invalid SCRATCH_SCHEMA format: {schema}")

    return schema


@pytest.fixture
def fresh_test_table(spark, scratch_schema):
    """Creates a unique Delta table in the scratch schema and drops it after the test."""
    parts = scratch_schema.split(".")
    table_name = f"test_{uuid.uuid4().hex[:8]}"

    if len(parts) == 2:
        fq_table = f"{parts[0]}.{parts[1]}.{table_name}"
    elif len(parts) == 1:
        fq_table = f"{parts[0]}.{table_name}"
    else:
        raise ValueError(f"Invalid SCRATCH_SCHEMA: {scratch_schema}")

    # Create the table with dummy data
    df = spark.createDataFrame([("init", 0)], ["x", "y"])
    df.write.format("delta").mode("overwrite").saveAsTable(fq_table)

    yield fq_table  # pass fully-qualified table name to test

    # Clean up after test
    spark.sql(f"DROP TABLE IF EXISTS {fq_table}")
