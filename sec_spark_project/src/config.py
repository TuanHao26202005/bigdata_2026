"""Cấu hình chung; đặt SEC_PROJECT_ROOT để chạy trên Colab/thư mục khác."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("SEC_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
QUARTERS = os.environ.get("SEC_QUARTERS", "2024q1,2024q2,2024q3,2024q4").split(",")
TABLES = ("sub", "num", "tag", "pre")
DATA = ROOT / "data"
REPORTS = ROOT / "reports"
BASE_URL = "https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets"


def make_spark(app_name="SEC cleaning"):
    # Phải đặt trước khi JVM khởi động; notebook cần restart nếu đã có SparkContext.
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--driver-memory 3g pyspark-shell")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName(app_name)
        .config("spark.driver.memory", "3g")
        .config("spark.driver.maxResultSize", "256m")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "32"))
        .config("spark.default.parallelism", "8")
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "2097152")
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "2097152")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.csv.parser.columnPruning.enabled", "false")
        .config("spark.local.dir", str(DATA / "spark_tmp"))
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
