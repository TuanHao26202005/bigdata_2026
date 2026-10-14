"""Chạy CLI tương đương bốn notebook, không cần Jupyter."""
import argparse
from .config import make_spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["ingest", "eda", "clean", "report", "all"])
    parser.add_argument("--skip-benchmark", action="store_true")
    args = parser.parse_args()
    spark = make_spark()
    try:
        if args.stage in ("ingest", "all"):
            from .ingest import ingest_all
            ingest_all(spark)
        if args.stage in ("eda", "all"):
            from .quality import eda
            eda(spark)
        if args.stage in ("clean", "all"):
            from .pipeline import clean_all
            clean_all(spark)
        if args.stage in ("report", "all"):
            from .report import report_all
            report_all(spark, run_benchmark=not args.skip_benchmark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
