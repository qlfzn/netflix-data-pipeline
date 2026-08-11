import os

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession


class GoldService:
    def __init__(self, spark: SparkSession, logger, db_service=None):
        self.spark = spark
        self.logger = logger
        self.db_service = db_service

        self.aggregations = {
            "gold_user_engagement_profile": "gold_user_engagement_profile.sql",
            "gold_content_performance": "gold_content_performance.sql",
            "gold_subscription_revenue": "gold_subscription_revenue.sql",
        }

    def check_path_exists(self, path: str) -> bool:
        if not os.path.exists(path):
            self.logger.error(f"Silver path not found: {path}")
            return False
        return True

    def register_silver_tables(self, silver_path: str):
        """
        Register silver tables as SQL temporary views.
        """
        for table_name in os.listdir(silver_path):
            table_path = os.path.join(silver_path, table_name)
            try:
                df = self.spark.read.parquet(table_path)
                df.createOrReplaceTempView(table_name)
                self.logger.info(f"Registered silver table '{table_name}' as temp view")
            except AnalysisException as e:
                self.logger.error(f"Failed to register {table_name}: {e}")

    def parse_sql(self, sql_path: str) -> str:
        """
        Read and parse SQL script from file and return as string.
        """
        with open(sql_path, "r") as f:
            query = f.read()
        return query

    def run_gold(self, silver_path: str, sql_dir: str = "src/sql"):
        """
        Orchestrate operations in Gold layer.
        """
        self.logger.info("Starting gold layer")

        if not self.check_path_exists(silver_path):
            self.logger.error("Silver path does not exist. Stopping gold layer.")
            return

        if self.db_service is None:
            self.logger.error("DuckDB service is not initialized. Stopping gold layer.")
            return

        self.register_silver_tables(silver_path)

        for gold_table, sql_file in self.aggregations.items():
            sql_path = os.path.join(sql_dir, sql_file)
            if not os.path.exists(sql_path):
                self.logger.warning(f"SQL file for {gold_table} not found: {sql_path}")
                continue

            query = self.parse_sql(sql_path)
            self.logger.info(f"Running aggregation for {gold_table} using {sql_file}")

            try:
                result_df = self.spark.sql(query)
                self.db_service.write_df_to_table(gold_table, result_df)
                self.logger.info(
                    f"Written gold table '{gold_table}' to DuckDB at {self.db_service.database_path}"
                )
            except AnalysisException as e:
                self.logger.error(f"Failed to generate {gold_table}: {e}")

        self.logger.info("Finished Gold layer")
