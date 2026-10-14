"""Profile chỉ thu các kết quả tổng hợp nhỏ về driver."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from .schemas import COLUMNS, KEYS, REQUESTED_NUM_KEY
from .cleaning import duplicate_count, write_json, union_quarters
from .config import REPORTS, QUARTERS, TABLES


def profile(df, table, stage):
    cols = [c for c in COLUMNS[table] if c in df.columns]
    expressions = [F.count("*").alias("rows")]
    types = {f.name: f.dataType for f in df.schema}
    for i, c in enumerate(cols):
        null = F.col(c).isNull()
        missing = null
        if isinstance(types[c], StringType):
            missing = null | (F.length(F.regexp_replace(F.col(c), r"[\s\u00a0]", "")) == 0)
        expressions.extend([
            F.sum(F.when(null, 1).otherwise(0)).alias(f"n{i}"),
            F.sum(F.when(missing, 1).otherwise(0)).alias(f"m{i}"),
        ])
    values = df.agg(*expressions).first().asDict()
    n = values["rows"]
    per_column = [{"table": table, "stage": stage, "column": c, "rows": n,
        "null_count": int(values[f"n{i}"] or 0),
        "null_pct": 100.0 * int(values[f"n{i}"] or 0) / n if n else None,
        "missing_including_blank_pct": 100.0 * int(values[f"m{i}"] or 0) / n if n else None}
        for i, c in enumerate(cols)]
    summary = {"table": table, "stage": stage, "rows": n,
               "duplicate_excess_sec_key": duplicate_count(df, KEYS[table])}
    if table == "num":
        summary["duplicate_excess_requested_key"] = duplicate_count(df, REQUESTED_NUM_KEY)
    return summary, per_column


def distribution(df, column, path):
    # Dùng Spark ghi CSV cho phân bố lớn, không toPandas toàn bảng.
    (df.groupBy(column).count().orderBy(F.desc("count"))
        .write.mode("overwrite").option("header", True).csv(str(path)))


def eda(spark, quarters=QUARTERS):
    summaries, columns = [], []
    for table in TABLES:
        df = union_quarters(spark, table, quarters)
        summary, detail = profile(df, table, "bronze")
        summaries.append(summary)
        columns.extend(detail)
        print(summary)
        if table == "sub":
            for column in ("form", "fy", "prevrpt"):
                distribution(df, column, REPORTS / "eda" / column)
        if table == "num":
            distribution(df, "uom", REPORTS / "eda" / "uom")
            odd = df.filter(F.col("value").isNotNull() &
                ~F.trim(F.col("value")).rlike(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$"))
            odd.limit(100).write.mode("overwrite").option("header", True).csv(str(REPORTS / "eda" / "odd_value_examples"))
    write_json(REPORTS / "profile_bronze_summary.json", summaries)
    write_json(REPORTS / "profile_bronze_columns.json", columns)
    return summaries
