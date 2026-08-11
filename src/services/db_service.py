from pathlib import Path

import duckdb


class DatabaseLayer:
    def __init__(self, database_path: str) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = duckdb.connect(database=str(self.database_path))

    def write_df_to_table(self, table_name: str, dataframe) -> None:
        """
        Write a Spark DataFrame into DuckDB as a table.
        """
        pandas_df = dataframe.toPandas()
        temp_view_name = f"temp_{table_name}"

        self.db.register(temp_view_name, pandas_df)
        try:
            self.db.execute(
                f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM "{temp_view_name}"'
            )
        finally:
            self.db.unregister(temp_view_name)

    def close(self) -> None:
        """
        Close the DuckDB connection.
        """
        self.db.close()

