"""Tên cột: SEC aqfsn_1.pdf, mục 5.1, 5.2, 5.4, 5.7.

Bronze có schema StringType tường minh để không mất giá trị sai kiểu.
TARGET_TYPES xác định schema nghiệp vụ Silver; tuyệt đối không inferSchema.
Thứ tự cột lấy từ header đã kiểm tra để tránh nhầm vị trí giữa các đợt phát hành.
"""
from pyspark.sql.types import StructType, StructField, StringType

COLUMNS = {
    "sub": "adsh cik name sic countryba stprba cityba zipba bas1 bas2 baph countryma stprma cityma zipma mas1 mas2 countryinc stprinc ein former changed afs wksi fye form period fy fp filed accepted prevrpt detail instance nciks aciks pubfloatusd floatdate floataxis floatmems".split(),
    "num": "adsh tag version ddate qtrs uom dimh iprx value footnote footlen dimn coreg durp datp dcml".split(),
    "tag": "tag version custom abstract datatype iord crdr tlabel doc".split(),
    "pre": "adsh report line stmt inpth tag version prole plabel negating".split(),
}

RAW_SCHEMAS = {
    table: StructType([StructField(c, StringType(), True) for c in cols])
    for table, cols in COLUMNS.items()
}

TARGET_TYPES = {table: {c: "string" for c in cols} for table, cols in COLUMNS.items()}
TARGET_TYPES["sub"].update({
    "cik": "bigint", "wksi": "int", "fy": "int", "prevrpt": "int", "detail": "int",
    "nciks": "int", "floatmems": "int", "pubfloatusd": "decimal(38,4)",
    "changed": "date", "period": "date", "filed": "date", "floatdate": "date",
    "accepted": "timestamp",
})
TARGET_TYPES["num"].update({
    "ddate": "date", "qtrs": "int", "iprx": "int", "value": "decimal(38,4)",
    "footlen": "int", "dimn": "int", "durp": "double", "datp": "double", "dcml": "int",
})
TARGET_TYPES["tag"].update({"custom": "int", "abstract": "int"})
TARGET_TYPES["pre"].update({"report": "int", "line": "int", "inpth": "int", "negating": "int"})

# Khóa đề bài không đủ với dữ liệu Notes: giữ riêng để đối chiếu trong EDA.
REQUESTED_NUM_KEY = "adsh tag version ddate qtrs uom coreg".split()
KEYS = {
    "sub": ["adsh"],
    "tag": ["tag", "version"],
    "num": "adsh tag version ddate qtrs uom dimh iprx".split(),
    "pre": ["adsh", "report", "line"],
}
REQUIRED = {
    "sub": "adsh cik form period fy fp filed".split(),
    "num": KEYS["num"],
    "tag": KEYS["tag"],
    "pre": "adsh report line tag version".split(),
}


def schema_for_header(table, header):
    expected = set(COLUMNS[table])
    if len(header) != len(set(header)) or set(header) != expected:
        raise ValueError(
            f"[kiểm tra lại với readme SEC] {table}: "
            f"thiếu={sorted(expected-set(header))}, thừa={sorted(set(header)-expected)}"
        )
    return StructType([StructField(c, StringType(), True) for c in header])
