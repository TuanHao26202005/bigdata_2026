import csv
import hashlib
import json
import os
import shutil
import time
import zipfile
from pathlib import Path
import requests
from pyspark.sql import functions as F
from .config import DATA, REPORTS, BASE_URL, QUARTERS, TABLES
from .schemas import COLUMNS, schema_for_header
from .storage import bronze_manifest, write_storage_manifest


def download_quarter(quarter):
    path = DATA / "downloads" / f"{quarter}_notes.zip"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and zipfile.is_zipfile(path):
        return path
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua:
        raise RuntimeError(
            "Chưa có ZIP. Tải từ trang SEC vào data/downloads/ hoặc đặt "
            "SEC_USER_AGENT bằng tên dự án kèm email liên hệ thật."
        )
    url = f"{BASE_URL}/{quarter}_notes.zip"
    part = path.with_suffix(".part")
    for attempt in range(3):
        try:
            with requests.get(url, headers={"User-Agent": ua, "Accept-Encoding": "identity"},
                              stream=True, timeout=(30, 180)) as response:
                if response.status_code == 403:
                    raise PermissionError(f"SEC trả 403: tải thủ công {url} vào {path}")
                response.raise_for_status()
                with part.open("wb") as target:
                    for chunk in response.iter_content(1024 * 1024):
                        target.write(chunk)
            if not zipfile.is_zipfile(part):
                raise ValueError("Phản hồi không phải ZIP hợp lệ")
            part.replace(path)
            return path
        except PermissionError:
            raise
        except (requests.RequestException, ValueError):
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("Tải thất bại")


def extract_quarter(quarter, archive):
    target = DATA / "raw" / quarter
    target.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zf:
        for table in TABLES:
            members = [m for m in zf.infolist()
                       if Path(m.filename).name.lower() in (f"{table}.txt", f"{table}.tsv")]
            if len(members) != 1:
                raise ValueError(f"{quarter}: cần đúng 1 file {table}.txt/.tsv")
            with zf.open(members[0]) as source, (target / f"{table}.tsv").open("wb") as dest:
                shutil.copyfileobj(source, dest, length=1024 * 1024)
    return target


def read_txt(spark, path, table):
    with Path(path).open(encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle, delimiter="\t", quoting=csv.QUOTE_NONE))
    schema = schema_for_header(table, header)
    return (spark.read.schema(schema).option("header", True).option("sep", "\t")
        .option("encoding", "UTF-8").option("quote", "\u0000")
        .option("escape", "\u0000").option("multiLine", False)
        .option("ignoreLeadingWhiteSpace", False).option("ignoreTrailingWhiteSpace", False)
        .option("nullValue", "__SEC_RESERVED_NULL_SENTINEL__").option("emptyValue", "")
        .option("mode", "FAILFAST").option("enforceSchema", False)
        .csv(str(path)).select(*COLUMNS[table]))


def sha256_file(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_all(spark, quarters=QUARTERS):
    manifest = []
    REPORTS.mkdir(parents=True, exist_ok=True)
    for quarter in quarters:
        raw = DATA / "raw" / quarter
        if not all((raw / f"{t}.tsv").exists() for t in TABLES):
            raw = extract_quarter(quarter, download_quarter(quarter))
        for table in TABLES:
            path = raw / f"{table}.tsv"
            df = (read_txt(spark, path, table)
                .withColumn("source_quarter", F.lit(quarter))
                .withColumn("source_file", F.lit(path.name)))
            output = DATA / "bronze" / table / quarter
            df.write.mode("overwrite").parquet(str(output))
            count = spark.read.parquet(str(output)).count()
            manifest.append({"quarter": quarter, "table": table, "rows": count,
                             "txt_bytes": path.stat().st_size, "sha256": sha256_file(path),
                             "download_url": f"{BASE_URL}/{quarter}_notes.zip"})
            print(f"Bronze {quarter}/{table}: {count:,} dòng")
    (REPORTS / "ingest_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_storage_manifest(spark, bronze_manifest(spark, quarters, TABLES),
                           "storage_before_cleaning.json")
    return manifest
