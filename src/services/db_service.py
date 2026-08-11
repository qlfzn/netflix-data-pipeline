import duckdb

class DatabaseLayer:
    def __init__(self) -> None:
        self.db = duckdb.connect(
            database="out/netflix_gold.db",
        )

        