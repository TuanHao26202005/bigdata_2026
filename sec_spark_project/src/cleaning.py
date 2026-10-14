"""Các phép làm sạch tái sử dụng; không cache toàn bảng NUM."""
import json
import shutil
from functools import reduce
from pathlib import Path
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType
from .config import DATA, REPORTS
from .schemas import TARGET_TYPES, KEYS, REQUIRED


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def log_count(df, ten_buoc, before=None, log_path=None, **details):
    """Action count kích hoạt lazy evaluation; before là số dòng đầu bước.

    Trả về số dòng để caller dùng lại; ghi JSONL trước/sau, không thu NUM về Python.
    """
    after = df.count()
    item = {"step": ten_buoc, "before": after if before is None else int(before),
            "after": after, "removed": 0 if before is None else int(before)-after,
            **details}
    path = Path(log_path or REPORTS / "cleaning_log.jsonl")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
    print(json.dumps(item, ensure_ascii=False, default=str))
    return after


def count_conditions(df, conditions):
    if not conditions:
        return {}
    expressions = [F.sum(F.when(v, 1).otherwise(0)).alias(k) for k, v in conditions.items()]
    return {k: int(v or 0) for k, v in df.agg(*expressions).first().asDict().items()}


def trim_to_null(df):
    # Không sửa nội dung ở giữa doc/footnote; chỉ bỏ khoảng trắng đầu/cuối.
    exprs = []
    for field in df.schema:
        c = F.col(field.name)
        if isinstance(field.dataType, StringType) and not field.name.startswith("source_"):
            cleaned = F.trim(F.regexp_replace(c, r"^[\s\u00a0]+|[\s\u00a0]+$", ""))
            c = F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned)
        exprs.append(c.alias(field.name))
    return df.select(*exprs)


def whitespace_issues(df):
    checks = {}
    for field in df.schema:
        if isinstance(field.dataType, StringType) and not field.name.startswith("source_"):
            c = F.col(field.name)
            checks[field.name] = c.isNotNull() & ((F.length(F.trim(c)) == 0) |
                c.rlike(r"^[\s\u00a0]|[\s\u00a0]$"))
    return count_conditions(df, checks)


def cast_columns(df, table):
    """Giữ cột *_raw cùng cờ invalid_* cho mọi cột ép kiểu."""
    bools = {"wksi", "prevrpt", "detail", "custom", "abstract", "inpth", "negating"}
    # Metadata 2024: iprx minInclusive=0, dù phần mô tả gọi là positive integer.
    nonnegative = {"qtrs", "iprx", "dimn", "footlen", "floatmems", "report", "line"}
    positive = {"cik", "nciks"}
    exprs = []
    extras = []
    for name in df.columns:
        c = F.col(name)
        dtype = TARGET_TYPES[table].get(name, "string")
        if dtype == "string":
            exprs.append(c)
            continue
        if dtype == "date":
            typed = F.when(c.rlike(r"^\d{8}$"), F.to_date(c, "yyyyMMdd"))
        elif dtype == "timestamp":
            # SUB thực tế có phần giây thập phân, ví dụ 16:13:00.0.
            typed = F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]")
        elif dtype in ("int", "bigint"):
            typed = F.when(c.rlike(r"^[+-]?\d+$"), c.cast(dtype))
            if name in bools:
                typed = F.when(typed.isin(0, 1), typed)
            elif name in nonnegative:
                typed = F.when(typed >= 0, typed)
            elif name in positive:
                typed = F.when(typed > 0, typed)
            elif name == "fy":
                typed = F.when(typed.between(1800, 2100), typed)
        else:
            typed = c.cast(dtype)
            if dtype == "double":
                typed = F.when(~F.isnan(typed) & (F.abs(typed) != float("inf")), typed)
        exprs.append(typed.alias(name))
        extras.extend([c.alias(f"{name}_raw"),
                       (c.isNotNull() & typed.isNull()).alias(f"invalid_{name}")])
    return df.select(*exprs, *extras)


def normalize_company(df):
    return (df.withColumn("name_original", F.col("name"))
        .withColumn("name", F.upper(F.trim(F.regexp_replace("name", r"[\s\u00a0]+", " ")))))


def required_condition(table):
    return reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in REQUIRED[table]])


def normalize_units(df):
    # Alias hình thức, không tự đổi EUR -> USD, không gộp USD và USD/shares.
    u = F.lower(F.regexp_replace(F.trim(F.col("uom")), r"\s*/\s*", "/"))
    mapped = (F.when(u == "usd", "USD").when(u == "eur", "EUR")
        .when(u == "shares", "shares").when(u == "pure", "pure")
        .when(u == "usd/shares", "USD/shares").otherwise(F.col("uom")))
    return df.withColumn("uom_original", F.col("uom")).withColumn("uom", mapped)


def duplicate_count(df, keys):
    row = df.groupBy(*keys).count().filter("count > 1").agg(
        F.sum(F.col("count") - 1).alias("excess")).first()
    return int(row.excess or 0)


def key_conflicts(df, table):
    # Phân biệt bản sao thật với hai payload khác nhau dùng chung khóa.
    business = [c for c in TARGET_TYPES[table] if c not in KEYS[table]]
    payload = F.to_json(F.struct(*[F.col(c) for c in business]), {"ignoreNullFields": "false"})
    return (df.groupBy(*KEYS[table]).agg(F.countDistinct(payload).alias("payload_variants"))
        .filter("payload_variants > 1"))


def deterministic_dedup(df, table):
    keys = KEYS[table]
    tie_cols = sorted(c for c in df.columns if c not in keys)
    tie = F.sha2(F.to_json(F.struct(*[F.col(c) for c in tie_cols]),
                          {"ignoreNullFields": "false"}), 256)
    w = Window.partitionBy(*keys).orderBy(F.col("source_quarter").desc(), tie.asc())
    ranked = df.withColumn("_duplicate_rank", F.row_number().over(w))
    # row_number cố định bản giữ lại; dropDuplicates thực thi yêu cầu theo khóa SEC.
    kept = ranked.filter("_duplicate_rank = 1").drop("_duplicate_rank").dropDuplicates(keys)
    removed = ranked.filter("_duplicate_rank > 1").drop("_duplicate_rank")
    return kept, removed


def latest_filings(spark, sub):
    """Chính sách latest observed filing; không khẳng định bản /A là bản đầy đủ."""
    sub.createOrReplaceTempView("sub_candidates")
    ranked = spark.sql("""
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY cik, regexp_replace(form, '/A$', ''), period, fy, fp
            ORDER BY COALESCE(accepted, CAST(filed AS TIMESTAMP)) DESC,
                     filed DESC, adsh DESC
          ) AS _filing_rank
        FROM sub_candidates
    """)
    kept = (ranked.filter("_filing_rank = 1").drop("_filing_rank")
        .withColumn("unresolved_prevrpt", F.coalesce(F.col("prevrpt") == 1, F.lit(False))))
    return kept, ranked.filter("_filing_rank > 1").drop("_filing_rank")


def maybe_broadcast(df, max_bytes=2 * 1024 * 1024, max_rows=20000):
    """Đo phía Spark, chỉ collect 2 số; giới hạn theo bytes và số dòng.

    JSON bytes * 4 là ước lượng bảo thủ, không phải đo chính xác heap JVM.
    Chỉ gọi với dimension đã project ít cột; không bao giờ gọi trên NUM.
    """
    size = F.length(F.encode(F.to_json(F.struct(*[F.col(c) for c in df.columns])), "UTF-8"))
    stats = df.agg(F.count("*").alias("n"), F.sum(size).alias("bytes")).first()
    use = stats.n <= max_rows and int(stats.bytes or 0) * 4 <= max_bytes
    return (F.broadcast(df) if use else df), {"rows": stats.n,
        "estimated_bytes_x4": int(stats.bytes or 0) * 4, "broadcast_hint": use}


def outlier_stats(num):
    # So cùng chỉ tiêu/phiên bản/đơn vị/kỳ/chiều, không trộn các đại lượng khác nhau.
    groups = ["tag", "version", "uom", "qtrs", "dimh"]
    return num.groupBy(*groups).agg(
        F.count("value").alias("n_values"), F.min("value").alias("min_value"),
        F.max("value").alias("max_value"),
        F.percentile_approx("value", [0.01, 0.25, 0.50, 0.75, 0.99], 1000).alias("percentiles"))


def flag_outliers(num, stats):
    groups = ["tag", "version", "uom", "qtrs", "dimh"]
    cutoffs = stats.select(*groups, "n_values", F.col("percentiles")[1].alias("p25"),
                          F.col("percentiles")[3].alias("p75"))
    joined = num.join(cutoffs, groups, "left")
    iqr = F.col("p75") - F.col("p25")
    evaluable = F.coalesce((F.col("n_values") >= 20) & (iqr > 0) & F.col("value").isNotNull(), F.lit(False))
    return (joined.withColumn("outlier_evaluable", evaluable)
        .withColumn("is_outlier", F.when(evaluable,
            (F.col("value") < F.col("p25")-3*iqr) |
            (F.col("value") > F.col("p75")+3*iqr)).otherwise(False)))


def union_quarters(spark, table, quarters):
    frames = [spark.read.parquet(str(DATA / "bronze" / table / q)) for q in quarters]
    signature = lambda df: sorted((f.name, f.dataType.simpleString()) for f in df.schema)
    first = signature(frames[0])
    for quarter, frame in zip(quarters, frames):
        if signature(frame) != first:
            raise ValueError(f"Schema không khớp: {table}/{quarter}")
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=False), frames)


class Audit:
    """Materialize từng bước xuống đĩa, xóa checkpoint cũ sau khi ghi thành công."""
    def __init__(self, spark):
        self.spark = spark
        self.counts = {}
        self.paths = {}
        self.log_path = REPORTS / "cleaning_log.jsonl"
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.write_text("", encoding="utf-8")

    def reject(self, df, step, table, reason):
        path = DATA / "quarantine" / step / table
        df.withColumn("rejection_reason", F.lit(reason)).write.mode("overwrite").parquet(str(path))

    def stage(self, df, step, table, details=None, before=None):
        path = DATA / "work" / step / table
        df.write.mode("overwrite").parquet(str(path))
        result = self.spark.read.parquet(str(path))
        n = log_count(result, f"{step}/{table}",
            before=self.counts.get(table) if before is None else before,
            log_path=self.log_path, **(details or {}))
        old = self.paths.get(table)
        self.paths[table], self.counts[table] = path, n
        if old and old != path and old.exists():
            shutil.rmtree(old)
        return result
