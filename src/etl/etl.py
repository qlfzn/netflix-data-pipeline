import os
from pathlib import Path

from pyspark.errors import AnalysisException

from services import BronzeService, GoldService, SilverService, SparkService
from utils import Logger


class NetflixETL:
    def __init__(self) -> None:
        self.spark = SparkService().get_spark()
        self.logger = Logger(class_name=__name__).get_logger()

        self.bronze = BronzeService(spark=self.spark, logger=self.logger)
        self.silver = SilverService(spark=self.spark, logger=self.logger)
        self.gold = GoldService(spark=self.spark, logger=self.logger)

        self.bronze_files_check = {
            "total_files": 0,
            "processed": 0,
            "skipped": 0,
            "failed": 0,
        }

        self.silver_columns = ["user_id", "movie_id"]

    def run_bronze(self, source_path: str, dest_path: str):
        """
        Orchestrate operations in Bronze layer.
        """
        if not os.path.exists(source_path):
            self.logger.error(f"Source path does not exist: {source_path}")
            return

        self.logger.info("Starting bronze layer")
        for file in os.listdir(source_path):
            self.bronze_files_check["total_files"] += 1

            full_path = os.path.join(source_path, file)
            table_name = os.path.splitext(file)[0]
            dest_folder = os.path.join(dest_path, table_name)
            row_count = 0

            try:
                df = self.bronze.read_file(path=full_path)
                if df is None:
                    self.bronze_files_check["failed"] += 1
                    self.logger.warning(f"Skipping unreadable file: {full_path}")
                    continue

                row_count = df.count()
                self.logger.info(
                    f"Successfully read file at path {full_path}. Row count: {row_count}"
                )
            except (AnalysisException, OSError) as e:
                self.bronze_files_check["failed"] += 1
                self.logger.error(f"Failed to read file: {e}")
                continue

            try:
                if row_count <= 1:
                    self.logger.warning(f"Skipping empty file: {full_path}")
                    self.bronze_files_check["skipped"] += 1
                    continue

                self.bronze.write_to_processed(dataframe=df, dest_path=dest_folder)
                self.bronze_files_check["processed"] += 1
                self.logger.info(f"Successfully write file to {dest_folder}/")
            except (AnalysisException, OSError) as e:
                self.bronze_files_check["failed"] += 1
                self.logger.error(f"Failed to write file: {e}")

        self.logger.info(f"Bronze result: {self.bronze_files_check}")
        self.logger.info("Finishing bronze layer...")

    def run_silver(self, bronze_path: str, silver_path: str):
        """
        Orchestrate operations in Silver layer.
        """
        self.logger.info("Starting silver layer")

        if not self.silver.check_path_exists(bronze_path):
            self.logger.error("Bronze path does not exist. Aborting silver pipeline.")
            return

        for table_name in os.listdir(bronze_path):
            table_path = os.path.join(bronze_path, table_name)
            self.logger.info(f"Processing table: {table_name}")

            try:
                df = self.silver.read_from_bronze(table_path)
                self.logger.info(
                    f"Read {table_name} successfully. Row count: {df.count()}"
                )
            except AnalysisException as e:
                self.logger.error(f"Failed to read table {table_name}: {e}")
                continue

            # run data quality checks
            for col in df.columns:
                if col in self.silver_columns:
                    self.logger.info(f"Running data quality checks on column: {col}")
                    df = self.silver.deduplicate(df, columns=[col])
                    df = self.silver.drop_null_values(df, columns=[col])
                else:
                    continue

            df = self.silver.log_processed_time(df)

            dest_path = os.path.join(silver_path, table_name)
            self.silver.write_to_silver(
                df=df, silver_path=dest_path, table_name=table_name
            )

        self.logger.info("Finishing silver layer...")

    def run_gold(self, silver_path: str, gold_path: str, sql_dir: str = "src/sql"):
        """
        Orchestrate operations in Gold layer.
        """
        self.logger.info("Starting gold layer")

        if not self.gold.check_path_exists(silver_path):
            self.logger.error("Silver path does not exist. Aborting gold pipeline.")
            return

        self.gold.register_silver_tables(silver_path)

        for gold_table, sql_file in self.gold.aggregations.items():
            sql_path = os.path.join(sql_dir, sql_file)
            if not os.path.exists(sql_path):
                self.logger.warning(f"SQL file for {gold_table} not found: {sql_path}")
                continue

            query = self.gold.parse_sql(sql_path)
            self.logger.info(f"Running aggregation for {gold_table} using {sql_file}")

            try:
                result_df = self.spark.sql(query)
                dest_path = os.path.join(gold_path, gold_table)
                result_df.write.mode("overwrite").parquet(dest_path)
                self.logger.info(f"Written gold table '{gold_table}' to {dest_path}")
            except (AnalysisException, OSError) as e:
                self.logger.error(f"Failed to generate {gold_table}: {e}")

        self.logger.info("Finishing Gold layer...")

    def run(self, source_dir: str, dest_dir: str):
        """
        Run the ETL pipeline.
        """
        src_dir = Path(source_dir)
        out_dir = Path(dest_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        bronze_path = out_dir / "bronze"
        silver_path = out_dir / "silver"
        gold_path = out_dir / "gold"

        self.logger.info(f"Running bronze on directory {src_dir} -> {bronze_path}")
        self.run_bronze(str(src_dir), str(bronze_path))

        self.logger.info(f"Running silver from {bronze_path} -> {silver_path}")
        self.run_silver(str(bronze_path), str(silver_path))

        self.logger.info(f"Running gold from {silver_path} -> {gold_path}")
        self.run_gold(str(silver_path), str(gold_path))
