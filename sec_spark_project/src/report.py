"""Báo cáo thật khi chạy dữ liệu thật; không gán sẵn số liệu SEC."""
import json
import statistics
import time
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from .config import DATA, REPORTS, QUARTERS, TABLES
from .schemas import COLUMNS
from .quality import profile
from .cleaning import write_json
from .ingest import read_txt


SQL_QUERIES = {
    "01_company_filing_coverage": """
      SELECT fy, form, COUNT(*) AS filings, COUNT(DISTINCT cik) AS companies
      FROM clean_sub GROUP BY fy, form ORDER BY fy, form
    """,
    "02_value_quality_by_unit": """
      SELECT uom, COUNT(*) AS facts,
             SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS missing_values,
             SUM(CASE WHEN outlier_evaluable THEN 1 ELSE 0 END) AS evaluated,
             SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS flagged,
             ROUND(100.0 * SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END)
               / NULLIF(SUM(CASE WHEN outlier_evaluable THEN 1 ELSE 0 END), 0), 2)
               AS flagged_pct_of_evaluated
      FROM clean_facts GROUP BY uom ORDER BY facts DESC
    """,
    # Chỉ minh họa cohort quan sát được, không tuyên bố top toàn thị trường FY2024.
    "03_top_observed_revenue_2024": """
      WITH income_tags AS (
        SELECT DISTINCT adsh, tag, version
        FROM clean_pre WHERE stmt = 'IS' AND inpth = 0
      ), candidates AS (
        SELECT f.*,
          CASE f.tag
            WHEN 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN 1
            WHEN 'Revenues' THEN 2
            WHEN 'SalesRevenueNet' THEN 3
          END AS tag_priority
        FROM clean_facts f
        LEFT SEMI JOIN income_tags p
          ON f.adsh=p.adsh AND f.tag=p.tag AND f.version=p.version
        WHERE f.fy=2024 AND f.form IN ('10-K','10-K/A')
          AND f.fp='FY' AND f.ddate=f.period AND f.qtrs=4
          AND f.uom='USD' AND f.coreg IS NULL AND f.dimn=0
          AND f.dimh RLIKE '^(0x)?0+$' AND f.iprx IN (0,1) AND f.value IS NOT NULL
          AND f.custom=0 AND f.version LIKE 'us-gaap/%'
          AND f.tag IN ('RevenueFromContractWithCustomerExcludingAssessedTax',
                        'Revenues','SalesRevenueNet')
      ), ranked AS (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY cik
          ORDER BY period DESC, accepted DESC NULLS LAST, filed DESC,
                   tag_priority ASC, iprx ASC, adsh DESC, version DESC
        ) AS rn FROM candidates
      )
      SELECT cik, name, period, form, tag, value AS revenue_usd,
             is_outlier, unresolved_prevrpt
      FROM ranked WHERE rn=1 ORDER BY revenue_usd DESC LIMIT 10
    """,
}


def parquet_bytes(path):
    return sum(p.stat().st_size for p in Path(path).rglob("*.parquet"))


def benchmark(spark, quarters=QUARTERS, repeats=3):
    records = []
    for quarter in quarters:
        txt_path = DATA / "raw" / quarter / "num.tsv"
        pq_path = DATA / "bronze" / "num" / quarter
        durations = {"TXT": [], "Parquet": []}
        checksums = {}
        def run(fmt):
            spark.catalog.clearCache()
            start = time.perf_counter()
            df = read_txt(spark, txt_path, "num") if fmt == "TXT" else spark.read.parquet(str(pq_path))
            # Cả hai phải giải mã cùng các cột; checksum ngăn count metadata-only.
            h = F.pmod(F.xxhash64("adsh", "tag", "value"), F.lit(1000000007)).cast("decimal(38,0)")
            row = df.select("adsh", "tag", "value").agg(F.count("*").alias("n"), F.sum(h).alias("checksum")).first()
            return time.perf_counter()-start, (row.n, str(row.checksum))
        # Một lượt warm-up; OS page cache vẫn tồn tại, không gọi đây là cold benchmark.
        for fmt in ("TXT", "Parquet"):
            _, checksums[fmt] = run(fmt)
        for repeat in range(repeats):
            order = ("TXT", "Parquet") if repeat % 2 == 0 else ("Parquet", "TXT")
            for fmt in order:
                elapsed, result = run(fmt)
                assert result == checksums[fmt], "Dữ liệu benchmark thay đổi giữa các lượt"
                durations[fmt].append(elapsed)
        assert checksums["TXT"] == checksums["Parquet"], "TXT và Bronze khác nội dung"
        for fmt, path in (("TXT", txt_path), ("Parquet", pq_path)):
            records.append({"quarter": quarter, "table": "num", "format": fmt,
                "median_seconds": statistics.median(durations[fmt]),
                "runs_seconds": durations[fmt], "rows": checksums[fmt][0],
                "bytes_on_disk": path.stat().st_size if fmt == "TXT" else parquet_bytes(path),
                "note": "warm-process; OS cache uncontrolled; scan adsh/tag/value; Parquet includes provenance"})
    write_json(REPORTS / "benchmark.json", records)
    pd.DataFrame(records).to_csv(REPORTS / "benchmark.csv", index=False)
    return records


def report_all(spark, quarters=QUARTERS, run_benchmark=True):
    REPORTS.mkdir(parents=True, exist_ok=True)
    baseline = REPORTS / "profile_bronze_summary.json"
    if not baseline.exists():
        from .quality import eda
        eda(spark, quarters)
    before_summary = json.loads(baseline.read_text(encoding="utf-8"))
    before_columns = json.loads((REPORTS / "profile_bronze_columns.json").read_text(encoding="utf-8"))
    after_summary, after_columns = [], []
    for table in TABLES:
        df = spark.read.parquet(str(DATA / "silver" / table))
        summary, detail = profile(df, table, "silver")
        after_summary.append(summary)
        after_columns.extend(detail)
    write_json(REPORTS / "profile_silver_summary.json", after_summary)
    write_json(REPORTS / "profile_silver_columns.json", after_columns)
    summary_df = pd.DataFrame(before_summary+after_summary)
    null_df = pd.DataFrame(before_columns+after_columns)
    summary_df.to_csv(REPORTS / "before_after_rows.csv", index=False)
    null_df.to_csv(REPORTS / "before_after_nulls.csv", index=False)
    for table in TABLES:
        for metric in ("null_pct", "missing_including_blank_pct"):
            subset = null_df[null_df.table == table]
            pivot = subset.pivot(index="column", columns="stage", values=metric).reindex(COLUMNS[table])
            ax = pivot.plot.bar(figsize=(max(11, len(pivot)*0.32), 5), color=["#8ca7c6", "#167d7f"])
            ax.set_title(f"{table.upper()}: {metric} trước / sau")
            ax.set_ylabel("Tỷ lệ (%)")
            ax.set_xlabel("Cột")
            ax.set_ylim(0, 105)
            plt.tight_layout()
            plt.savefig(REPORTS / f"{table}_{metric}.png", dpi=160)
            plt.close()
    for table in ("sub", "pre"):
        spark.read.parquet(str(DATA / "silver" / table)).createOrReplaceTempView(f"clean_{table}")
    facts = spark.read.parquet(str(DATA / "silver" / "facts"))
    facts.createOrReplaceTempView("clean_facts")
    for name, query in SQL_QUERIES.items():
        result = spark.sql(query)
        result.show(20, truncate=False)
        result.write.mode("overwrite").option("header", True).csv(str(REPORTS / "sql" / name))
        (REPORTS / f"{name}.sql").write_text(query.strip()+"\n", encoding="utf-8")
    metrics = benchmark(spark, quarters) if run_benchmark else []
    log = [json.loads(line) for line in (REPORTS / "cleaning_log.jsonl").read_text(encoding="utf-8").splitlines()]
    pd.DataFrame(log).to_csv(REPORTS / "cleaning_log.csv", index=False)
    metadata = {"spark_version": spark.version, "quarters": quarters,
                "master": spark.sparkContext.master,
                "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions")}
    write_json(REPORTS / "environment.json", metadata)
    text = ["# Báo cáo chất lượng dữ liệu SEC", "",
            "Số liệu trong báo cáo được sinh bởi lần chạy hiện tại.",
            "Null cấu trúc (ví dụ coreg) và null do lỗi parse có ý nghĩa khác nhau.",
            "Tăng % null sau trim/cast không tự động có nghĩa chất lượng giảm.", "",
            "## Số dòng và trùng theo khóa SEC", "",
            "|Bảng|Giai đoạn|Dòng|Dòng dư theo khóa|", "|---|---|---:|---:|"]
    for item in before_summary + after_summary:
        text.append(f"|{item['table']}|{item['stage']}|{item['rows']}|{item['duplicate_excess_sec_key']}|")
    text += ["", "## Giới hạn", "",
        "Dữ liệu phát hành năm 2024 không tương đương toàn bộ kết quả kinh doanh FY2024.",
        "Top doanh thu chỉ minh họa tập quan sát: 3 tag ưu tiên, USD, không chiều, iprx thấp nhất trong 0/1.",
        "Không cộng nhiều tag doanh thu, không cộng USD với shares, không xóa số âm hoặc outlier.",
        "Chọn hồ sơ mới nhất có thể mất fact chỉ có ở hồ sơ gốc nếu bản sửa là một phần.",
        "Bronze và quarantine giữ bằng chứng để khôi phục; đây không phải bản dựng lại báo cáo đầy đủ.", "",
        "Chi tiết từng cột: before_after_nulls.csv. Log từng bước: cleaning_log.csv.",
        "Biểu đồ: các tệp PNG trong cùng thư mục.",
        "Benchmark: benchmark.csv; đo scan 3 cột, median 3 lượt, cache hệ điều hành không kiểm soát."]
    (REPORTS / "quality_report.md").write_text("\n".join(text)+"\n", encoding="utf-8")
    return summary_df, null_df, metrics
