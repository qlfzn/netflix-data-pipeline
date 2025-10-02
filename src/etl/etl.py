from pathlib import Path
from utils import Logger
import os
from pyspark.errors import AnalysisException

from services import BronzeService, SilverService


class NetflixETL:
    def __init__(self) -> None:
        self.bronze = BronzeService()
        self.silver = SilverService()
        self.logger = Logger(__name__).get_logger()

        self.bronze_files_check = {
            "total_files": 0,
            "processed": 0,
            "skipped": 0,
            "failed": 0
        }

        self.silver_columns = ["user_id", "movie_id"]

    def run_bronze(self, source_path: str, dest_path: str):
        """
        Orchestrate operations in Bronze layer.
        """
        if source_path is None:
            self.logger.error("Source path does not exist")
            return
        
        self.logger.info("Starting bronze layer")
        for file in os.listdir(source_path):
            self.bronze_files_check["total_files"] += 1

            full_path = os.path.join(source_path, file)
            table_name = os.path.splitext(file)[0] 
            dest_folder = os.path.join(dest_path, table_name)

            try:
                df = self.bronze.read_file(path=full_path)
                row_count = df.count()
                self.logger.info(f"Successfully read file at path {full_path}. Row count: {row_count}")
            except AnalysisException as e:
                self.bronze_files_check["failed"] += 1
                self.logger.error(f"Failed to read file: {e}")

            try:
                if row_count <= 1:
                    self.logger.warning(f"Skipping empty file: {full_path}")
                    self.bronze_files_check["skipped"] += 1
                    continue

                self.bronze.write_to_processed(dataframe=df, dest_path=dest_folder)
                self.bronze_files_check["processed"] += 1
                self.logger.info(f"Successfully write file to {dest_folder}/")
            except AnalysisException as e:
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
                self.logger.info(f"Read {table_name} successfully. Row count: {df.count()}")
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
            self.silver.write_to_silver(df=df, silver_path=dest_path, table_name=table_name)

        self.logger.info("Finishing silver layer...")

    def run(self, source_dir: str, dest_dir: str):
        """
        Run the ETL pipeline.
        """
        src_dir = Path(source_dir)
        out_dir = Path(dest_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        bronze_path = out_dir / 'bronze'
        silver_path = out_dir / 'silver'

        self.logger.info(f"Running bronze on directory {src_dir} -> {bronze_path}")
        self.run_bronze(str(src_dir), str(bronze_path))

        self.logger.info(f"Running silver from {bronze_path} -> {silver_path}")
        self.run_silver(str(bronze_path), str(silver_path))