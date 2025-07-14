import pytest
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provides a local SparkSession for unit tests."""
    return (
        SparkSession.builder.appName("Tests").master("local[*]").getOrCreate()
    )


@pytest.fixture
def sample_df(spark: SparkSession) -> DataFrame:
    """Creates a simple placeholder DataFrame for unit tests."""
    return spark.createDataFrame([("a", 1), ("b", 2)], ["x", "y"])


@pytest.fixture
def dummy_target_tbl() -> str:
    """Provides a placeholder target table name."""
    return "default.test_table_placeholder"
