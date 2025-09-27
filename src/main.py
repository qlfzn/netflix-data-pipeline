from pathlib import Path
import argparse
import sys

from etl import NetflixETL


def parse_args():
	p = argparse.ArgumentParser(description="Run Netflix ETL pipeline")
	p.add_argument('--source', default=str(Path(__file__).resolve().parents[1] / 'data'), help='Source data directory')
	p.add_argument('--dest', default=str(Path(__file__).resolve().parents[1] / 'out'), help='Destination output directory')
	return p.parse_args()


def main():
	args = parse_args()
	etl = NetflixETL()

	try:
		etl.run(source_dir=args.source, dest_dir=args.dest)
	except Exception as e:
		print(f"ETL failed: {e}")
		sys.exit(1)

	print("Completed.")


if __name__ == '__main__':
	main()