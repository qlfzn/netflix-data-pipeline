from pathlib import Path
from typing import List, Optional

from services import BronzeService


class NetflixETL:
    def __init__(self) -> None:
        self.bronze = BronzeService()

    def run_bronze(self, source_path: str, dest_path: str):
        """
        Orchestrate operations in Bronze layer
        """
        df = self.bronze.read_file(path=source_path)
        self.bronze.write_to_processed(dataframe=df, dest_path=dest_path)

    def run(self, source_dir: str, dest_dir: str):
        """
        Run the ETL pipeline.
        """
        src_dir = Path(source_dir)
        out_dir = Path(dest_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / 'bronze.parquet'
        print(f"Running bronze on directory {src_dir} -> {out_path}")
        self.run_bronze(str(src_dir), str(out_path))