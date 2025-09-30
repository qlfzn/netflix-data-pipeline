from pathlib import Path
from utils import Logger
import os
from pyspark.errors import AnalysisException

from services import BronzeService


class NetflixETL:
    def __init__(self) -> None:
        self.bronze = BronzeService()
        self.logger = Logger(__name__).get_logger()

        self.files_check = {
            "total_files": 0,
            "processed": 0,
            "skipped": 0,
            "failed": 0
        }

    def run_bronze(self, source_path: str, dest_path: str):
        """
        Orchestrate operations in Bronze layer.
        """
        if source_path is None:
            self.logger.error("Source path does not exist")
            return
        
        self.logger.info("Starting bronze layer")
        for file in os.listdir(source_path):
            self.files_check["total_files"] += 1

            full_path = os.path.join(source_path, file)
            table_name = os.path.splitext(file)[0] 
            dest_folder = os.path.join(dest_path, table_name)

            try:
                df = self.bronze.read_file(path=full_path)
                row_count = df.count()
                self.logger.info(f"Successfully read file at path {full_path}. Row count: {row_count}")
            except AnalysisException as e:
                self.files_check["failed"] += 1
                self.logger.error(f"Failed to read file: {e}")

            try:
                if row_count <= 1:
                    self.logger.warning(f"Skipping empty file: {full_path}")
                    self.files_check["skipped"] += 1
                    continue

                self.bronze.write_to_processed(dataframe=df, dest_path=dest_folder)
                self.files_check["processed"] += 1
                self.logger.info(f"Successfully write file to {dest_folder}/")
            except AnalysisException as e:
                self.files_check["failed"] += 1
                self.logger.error(f"Failed to write file: {e}")

        self.logger.info(f"Bronze result: {self.files_check}")
        self.logger.info("Finishing bronze layer...")

    def run(self, source_dir: str, dest_dir: str):
        """
        Run the ETL pipeline.
        """
        src_dir = Path(source_dir)
        out_dir = Path(dest_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / 'bronze'
        self.logger.info(f"Running bronze on directory {src_dir} -> {out_path}")
        self.run_bronze(str(src_dir), str(out_path))