from pathlib import Path
from utils import Logger

from services import BronzeService


class NetflixETL:
    def __init__(self) -> None:
        self.bronze = BronzeService()
        self.logger = Logger(__name__).get_logger()

    def run_bronze(self, source_path: str, dest_path: str):
        """
        Orchestrate operations in Bronze layer.
        """
        self.logger.info("Starting bronze layer")
        df = self.bronze.read_file(path=source_path)
        self.logger.info(f"Successfully read files. Writing to {dest_path}")
        self.bronze.write_to_processed(dataframe=df, dest_path=dest_path)
        self.logger.info(f"Successfully write to {dest_path}. Finishing bronze layer...")

    def run(self, source_dir: str, dest_dir: str):
        """
        Run the ETL pipeline.
        """
        src_dir = Path(source_dir)
        out_dir = Path(dest_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / 'bronze.parquet'
        self.logger.info(f"Running bronze on directory {src_dir} -> {out_path}")
        self.run_bronze(str(src_dir), str(out_path))