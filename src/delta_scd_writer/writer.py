"""Delta table writer supporting LND ingestion and SCD1/SCD2 change tracking using PySpark."""

from typing import List, Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException # type: ignore[attr-defined]


class DeltaTableWriter:
    """Utility to manage creation and writing to a Delta table."""

    def __init__(self, spark: SparkSession, target_tbl: str, primary_keys: List[str]) -> None:
        """Initialize the DeltaTableWriter.

        Args:
            spark: SparkSession used for interacting with Spark.
            target_tbl: Fully qualified target Delta table name.
            primary_keys: List of primary key column names.
        """
        self.spark: SparkSession = spark
        self.target: str = target_tbl
        self.pk: List[str] = primary_keys

    def _table_exists(self) -> bool:
        """Check if the target Delta table exists.

        Returns:
            True if the table exists, False otherwise.
        """
        return self.spark.catalog.tableExists(
            self.target
            )

    def _create_table(self, df: DataFrame) -> None:
        """Create the target Delta table using the provided DataFrame.

        Args:
            df: DataFrame to use for inferring schema and writing the table.
        """
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("mergeSchema", "true")
            .saveAsTable(self.target)
        )

    def _create_or_write_table(
        self,
        df: DataFrame,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        """Create the Delta table if it doesn't exist, otherwise write to it.

        Args:
            df: DataFrame to write.
            mode: Save mode ('append' or 'overwrite').
        """
        if mode not in {"append", "overwrite"}:
            raise ValueError(f"Invalid mode: {mode}. Must be 'append' or 'overwrite'.")

        if not self._table_exists():
            self._create_table(df)
        else:
            df.write.format("delta").mode(mode).saveAsTable(self.target)
