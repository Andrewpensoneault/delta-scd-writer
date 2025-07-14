import pytest
from pyspark.sql import SparkSession
from delta_scd_writer.writer import DeltaTableWriter


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("DeltaTableWriterTest") \
        .master("local[*]") \
        .getOrCreate()


def test_create_or_write_table_invalid_mode_raises_value_error(spark):
    manager = DeltaTableWriter(
        spark,
        target_tbl="forecast_demo.bronze.test",
        primary_keys=["a"]
    )
    df = spark.createDataFrame([("a", 1), ("b", 2)], ["x", "y"])

    with pytest.raises(ValueError, match="Invalid mode: a. Must be 'append' or 'overwrite'."):
        manager._create_or_write_table(df, "a")