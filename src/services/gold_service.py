import os
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

class GoldService:
    def __init__(self, spark: SparkSession, logger):
        self.spark = spark
        self.logger = logger

        self.aggregations = {
            "gold_monthly_user_activity": "gold_monthly_user_activity.sql",
            "gold_content_performance": "gold_content_performance.sql",
            "gold_subscription_revenue": "gold_subscription_revenue.sql"
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

    def run_gold(self, silver_path: str, gold_path: str, sql_dir: str = "sql"):
        """
        Orchestrate operations in Gold layer.
        """
        self.logger.info("Starting gold layer")

        if not self.check_path_exists(silver_path):
            self.logger.error("Silver path does not exist. Stopping gold layer.")
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
                dest_path = os.path.join(gold_path, gold_table)
                result_df.write.mode("overwrite").parquet(dest_path)
                self.logger.info(f"Written gold table '{gold_table}' to {dest_path}")
            except AnalysisException as e:
                self.logger.error(f"Failed to generate {gold_table}: {e}")

        self.logger.info("Finished Gold layer...")
