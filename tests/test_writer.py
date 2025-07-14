"""Unit tests for DeltaTableWriter."""

import pytest
from pyspark.sql import DataFrame, SparkSession

from delta_scd_writer import DeltaTableWriter


def test_create_or_write_table_invalid_mode_raises_value_error(
    spark: SparkSession,
    sample_df: DataFrame,
    dummy_target_tbl: str,
) -> None:
    """Test that an invalid write mode raises ValueError."""
    manager = DeltaTableWriter(
        spark,
        target_tbl=dummy_target_tbl,
        primary_keys=["a"],
    )

    with pytest.raises(
        ValueError, match="Invalid mode: a. Must be 'append' or 'overwrite'."
    ):
        manager._create_or_write_table(sample_df, "a")
