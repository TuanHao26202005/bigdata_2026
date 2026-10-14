"""Đủ 12 bước; thứ tự theo phụ thuộc dữ liệu thay vì thứ tự đánh số đề bài."""
from pyspark.sql import functions as F, Window
from .config import DATA, REPORTS, QUARTERS, TABLES
from .schemas import KEYS, REQUESTED_NUM_KEY
from .cleaning import (Audit, union_quarters, whitespace_issues, trim_to_null,
    cast_columns, count_conditions, normalize_company, required_condition,
    normalize_units, duplicate_count, deterministic_dedup, key_conflicts,
    latest_filings, maybe_broadcast, outlier_stats, flag_outliers, write_json)
from .quality import distribution
from .storage import cleaned_manifest, export_cleaned_tsv, write_storage_manifest


def clean_all(spark, quarters=QUARTERS):
    audit = Audit(spark)
    frames = {}
    # 11. Hợp nhất trước: bắt được trùng và bản sửa nằm ở hai quý khác nhau.
    for table in TABLES:
        rows_in = sum(spark.read.parquet(str(DATA / "bronze" / table / q)).count() for q in quarters)
        df = union_quarters(spark, table, quarters)
        frames[table] = audit.stage(df, "11_union", table,
            {"schema_match": True, "quarters": quarters}, before=rows_in)
        assert audit.counts[table] == rows_in, "unionByName không được làm mất dòng"

    # 2. Trim trước cast để lỗi do khoảng trắng không thành lỗi dữ liệu giả.
    for table in TABLES:
        df = frames[table]
        frames[table] = audit.stage(trim_to_null(df), "02_blank_trim", table,
            {"affected_by_column": whitespace_issues(df)})

    # 1. Ngày, số: cast thất bại -> null + giữ *_raw và invalid_*.
    for table in TABLES:
        typed = cast_columns(frames[table], table)
        flags = count_conditions(typed, {c: F.col(c) for c in typed.columns if c.startswith("invalid_")})
        frames[table] = audit.stage(typed, "01_types", table, {"invalid_casts": flags})

    # 3. Chuẩn hóa tên, vẫn dùng CIK để xác định công ty.
    sub = (normalize_company(frames["sub"])
        .withColumn("form", F.upper("form")).withColumn("fp", F.upper("fp")))
    affected = count_conditions(sub, {"name_changed": ~F.col("name").eqNullSafe(F.col("name_original"))})
    frames["sub"] = audit.stage(sub, "03_company", "sub", affected)

    # 4. Không impute số tài chính. Chỉ loại thiếu khóa/metadata thiết yếu.
    for table in TABLES:
        df = frames[table]
        valid = required_condition(table)
        details = count_conditions(df, {"missing_required": ~valid})
        null_counts = count_conditions(df, {c: F.col(c).isNull() for c in df.columns
                                           if not c.startswith("invalid_")})
        denominator = audit.counts[table]
        details["null_pct_by_column"] = {
            c: 100.0*n/denominator if denominator else None for c, n in null_counts.items()
        }
        audit.reject(df.filter(~valid), "04_missing", table, "missing_required_or_invalid_key")
        frames[table] = audit.stage(df.filter(valid), "04_missing", table, details)

    # 7. Chuẩn hóa trước khử trùng vì đơn vị nằm trong khóa NUM.
    num = frames["num"]
    distribution(num, "uom", REPORTS / "uom_before")
    fixed = normalize_units(num)
    details = count_conditions(fixed, {"uom_changed": ~F.col("uom").eqNullSafe(F.col("uom_original"))})
    frames["num"] = audit.stage(fixed, "07_units", "num", details)
    distribution(frames["num"], "uom", REPORTS / "uom_after")

    # 5. Cùng khóa khác payload được xuất riêng để review, không gọi đó là bản sao.
    for table in TABLES:
        df = frames[table]
        conflicts = key_conflicts(df, table)
        conflict_count = conflicts.count()
        conflicts.write.mode("overwrite").parquet(str(REPORTS / "key_conflicts" / table))
        details = {"duplicate_excess_sec_key": duplicate_count(df, KEYS[table]),
                   "conflicting_keys": conflict_count}
        if table == "num":
            details["duplicate_excess_requested_key"] = duplicate_count(df, REQUESTED_NUM_KEY)
        kept, removed = deterministic_dedup(df, table)
        audit.reject(removed, "05_duplicates", table, "duplicate_key_latest_source_then_hash")
        frames[table] = audit.stage(kept, "05_duplicates", table, details)

    # 9. Đo mồ côi trước lọc form/bản sửa, tránh gán sai cho loại theo phạm vi.
    subkeys = frames["sub"].select("adsh")
    tagkeys = frames["tag"].select("tag", "version")
    for table in ("num", "pre"):
        df = frames[table]
        missing_sub = df.join(subkeys, ["adsh"], "left_anti")
        missing_tag = df.join(tagkeys, ["tag", "version"], "left_anti")
        details = {"missing_valid_sub": missing_sub.count(), "missing_valid_tag_version": missing_tag.count(),
                   "note": "hai số có thể chồng lấp; before-after là tổng loại duy nhất"}
        audit.reject(missing_sub, "09_orphan_sub", table, "no_valid_sub_parent")
        audit.reject(missing_tag, "09_orphan_tag", table, "no_valid_tag_version_parent")
        valid = df.join(subkeys, ["adsh"], "left_semi").join(tagkeys, ["tag", "version"], "left_semi")
        frames[table] = audit.stage(valid, "09_orphans", table, details)

    # 10. Có giữ 10-K/A, 10-Q/A làm ứng viên để bước 6 không bỏ bản sửa mới.
    sub = frames["sub"]
    distribution(sub, "form", REPORTS / "form_before")
    scope = F.col("form").isin("10-K", "10-Q", "10-K/A", "10-Q/A")
    audit.reject(sub.filter(~scope), "10_form", "sub", "outside_project_form_scope")
    frames["sub"] = audit.stage(sub.filter(scope), "10_form", "sub",
                               count_conditions(sub, {"outside_scope": ~scope}))
    subkeys = frames["sub"].select("adsh")
    for table in ("num", "pre"):
        df = frames[table]
        audit.reject(df.join(subkeys, ["adsh"], "left_anti"), "10_form", table, "parent_form_outside_scope")
        frames[table] = audit.stage(df.join(subkeys, ["adsh"], "left_semi"), "10_form", table)

    # 6. Spark SQL ROW_NUMBER; latest trong phạm vi quan sát năm phát hành 2024.
    sub = frames["sub"]
    latest, old = latest_filings(spark, sub)
    details = count_conditions(sub, {"prevrpt_1_candidates": F.col("prevrpt") == 1})
    details.update(count_conditions(latest, {"unresolved_prevrpt": F.col("unresolved_prevrpt")}))
    audit.reject(old, "06_revisions", "sub", "older_observed_filing_same_company_form_period")
    frames["sub"] = audit.stage(latest, "06_revisions", "sub", details)
    subkeys = frames["sub"].select("adsh")
    for table in ("num", "pre"):
        df = frames[table]
        audit.reject(df.join(subkeys, ["adsh"], "left_anti"), "06_revisions", table, "parent_older_observed_filing")
        frames[table] = audit.stage(df.join(subkeys, ["adsh"], "left_semi"), "06_revisions", table)

    # 8. IQR theo nhóm >=20 số và IQR>0, cờ không phải kết luận sai kế toán.
    num = frames["num"]
    stats_path = REPORTS / "outlier_group_statistics"
    outlier_stats(num).write.mode("overwrite").parquet(str(stats_path))
    stats = spark.read.parquet(str(stats_path))
    tagged = flag_outliers(num, stats)
    details = count_conditions(tagged, {"flagged": F.col("is_outlier"),
                                      "evaluable": F.col("outlier_evaluable")})
    frames["num"] = audit.stage(tagged, "08_outliers", "num", details)

    # Silver riêng cho 4 bảng; bảng không có fy được nối sub trước khi partition.
    output = DATA / "silver"
    frames["sub"].write.mode("overwrite").partitionBy("fy").parquet(str(output / "sub"))
    frames["tag"].write.mode("overwrite").parquet(str(output / "tag"))
    fy_map = frames["sub"].select("adsh", "fy")
    for table in ("num", "pre"):
        frames[table].join(fy_map, ["adsh"], "inner").write.mode("overwrite").partitionBy("fy").parquet(str(output / table))

    # 12. Project trước join; giữ đúng 1 dimension row/key để không nhân bản NUM.
    num = spark.read.parquet(str(output / "num"))
    subdim = frames["sub"].select("adsh", "cik", "name", "sic", "form", "period", "fp",
        "filed", "accepted", "prevrpt", "unresolved_prevrpt",
        F.col("source_quarter").alias("filing_source_quarter"))
    tagdim = frames["tag"].select("tag", "version", "custom", "abstract", "datatype", "iord", "crdr", "tlabel")
    subdim, sub_plan = maybe_broadcast(subdim)
    tagdim, tag_plan = maybe_broadcast(tagdim)
    flat = num.join(subdim, ["adsh"], "inner").join(tagdim, ["tag", "version"], "inner")
    flat.write.mode("overwrite").partitionBy("fy").parquet(str(output / "facts"))
    final = spark.read.parquet(str(output / "facts"))
    from .cleaning import log_count
    final_count = log_count(final, "12_join/facts", before=audit.counts["num"],
                           sub_join=sub_plan, tag_join=tag_plan)
    assert final_count == audit.counts["num"], "Join làm mất/nhân dòng: kiểm tra khóa dimension"

    # Thực hiện khóa 7 cột trên lát cắt hợp nhất, không chiều, fact ưu tiên.
    # Metadata thực tế cho phép iprx=0; chọn thấp nhất trong 0/1 bằng window.
    consolidated = final.filter(F.col("coreg").isNull() & (F.col("dimn") == 0) &
                               F.col("dimh").rlike("^(0x)?0+$") & F.col("iprx").isin(0, 1))
    preferred_window = Window.partitionBy(*REQUESTED_NUM_KEY).orderBy(F.col("iprx"), F.col("dimh"))
    consolidated = (consolidated.withColumn("_preferred_rank", F.row_number().over(preferred_window))
                    .filter("_preferred_rank=1").drop("_preferred_rank"))
    before_core = consolidated.count()
    consolidated = consolidated.dropDuplicates(REQUESTED_NUM_KEY)
    consolidated.write.mode("overwrite").partitionBy("fy").parquet(str(output / "facts_consolidated"))
    after_core = log_count(spark.read.parquet(str(output / "facts_consolidated")),
                          "05_requested_key/consolidated", before=before_core)
    assert before_core == after_core, "Phạm vi consolidated còn khóa không nhất quán"
    write_json(REPORTS / "run_scope.json", {
        "release_quarters": quarters, "forms": ["10-K", "10-Q", "10-K/A", "10-Q/A"],
        "revision_policy": "latest observed filing per cik/base_form/period/fy/fp",
        "warning": "Bản sửa có thể chỉ sửa một phần; dữ liệu 2024 không bao phủ đầy đủ FY2024",
        "outlier_policy": "3*IQR, nhóm tag/version/uom/qtrs/dimh, ít nhất 20 giá trị",
        "final_fact_rows": final_count,
    })
    write_storage_manifest(spark, cleaned_manifest(spark),
                           "storage_after_cleaning.json")
    export_cleaned_tsv(spark, quarters)
    return final
