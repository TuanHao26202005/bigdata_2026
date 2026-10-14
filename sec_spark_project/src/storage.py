"""Theo dõi các lớp lưu trữ dữ liệu trước và sau làm sạch."""
import json
from pathlib import Path

from .config import DATA, REPORTS


def tsv_bytes(path):
    """Tính dung lượng các file TSV trong một thư mục."""
    root = Path(path)
    return sum(file.stat().st_size for file in root.rglob("*.tsv"))


def rename_tsv_parts(path):
    """Đổi phần mở rộng mặc định của Spark từ .csv thành .tsv."""
    for file in Path(path).glob("part-*.csv"):
        file.rename(file.with_suffix(".tsv"))


def parquet_bytes(path):
    """Tính dung lượng các file Parquet trong một thư mục."""
    root = Path(path)
    return sum(file.stat().st_size for file in root.rglob("*.parquet"))


def storage_entry(spark, path, stage, table):
    """Tạo thông tin lưu trữ cho một bảng Parquet."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy dữ liệu {stage}/{table}: {path}")
    df = spark.read.parquet(str(path))
    return {
        "stage": stage,
        "table": table,
        "format": "parquet",
        "path": str(path),
        "rows": df.count(),
        "columns": len(df.columns),
        "partitions": df.rdd.getNumPartitions(),
        "bytes_on_disk": parquet_bytes(path),
    }


def write_storage_manifest(spark, entries, filename="storage_manifest.json"):
    """Ghi manifest JSON mô tả dữ liệu trước/sau làm sạch."""
    path = REPORTS / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
    return entries


def bronze_manifest(spark, quarters, tables):
    """Lập manifest cho dữ liệu trước làm sạch."""
    return [
        {
            **storage_entry(
                spark,
                DATA / "bronze" / table / quarter,
                "before_cleaning_bronze",
                table,
            ),
            "quarter": quarter,
        }
        for table in tables
        for quarter in quarters
    ]


def cleaned_manifest(spark, tables=("sub", "num", "tag", "pre", "facts", "facts_consolidated")):
    """Lập manifest cho dữ liệu sau làm sạch."""
    entries = []
    for table in tables:
        path = DATA / "silver" / table
        if path.exists():
            entries.append(storage_entry(spark, path, "after_cleaning_silver", table))
    return entries


def export_cleaned_tsv(spark, quarters, tables=("sub", "num", "tag", "pre", "facts", "facts_consolidated")):
    """Xuất dữ liệu sau làm sạch thành các thư mục TSV theo quý."""
    quarter_columns = {
        "sub": "source_quarter",
        "num": "source_quarter",
        "tag": "source_quarter",
        "pre": "source_quarter",
        "facts": "filing_source_quarter",
        "facts_consolidated": "filing_source_quarter",
    }
    entries = []
    for table in tables:
        source = DATA / "silver" / table
        if not source.exists():
            continue
        df = spark.read.parquet(str(source))
        quarter_column = quarter_columns[table]
        if quarter_column not in df.columns:
            raise ValueError(f"{table}: thiếu cột phân quý {quarter_column}")
        for quarter in quarters:
            output = DATA / "cleaned_tsv" / table / quarter
            (df.filter(df[quarter_column] == quarter)
                .coalesce(1)
                .write.mode("overwrite")
                .option("header", True)
                .option("sep", "\t")
                .option("extension", "tsv")
                .option("quote", "\u0000")
                .option("escape", "\u0000")
                .csv(str(output)))
            rename_tsv_parts(output)
            entries.append({
                "stage": "after_cleaning",
                "table": table,
                "quarter": quarter,
                "format": "tsv",
                "path": str(output),
                "rows": spark.read.option("header", True).option("sep", "\t").csv(str(output)).count(),
                "bytes_on_disk": tsv_bytes(output),
            })
    write_storage_manifest(spark, entries, "storage_after_cleaning_tsv.json")
    return entries
