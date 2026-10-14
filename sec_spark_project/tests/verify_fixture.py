"""Kiểm tra các bất biến quan trọng sau pipeline trên fixture riêng."""
import json
from pyspark.sql import functions as F
from src.config import DATA, REPORTS, make_spark
from src.schemas import KEYS
from src.cleaning import duplicate_count, write_json


def main():
    spark = make_spark("SEC synthetic verification")
    checks = []
    def check(name, condition):
        assert condition, name
        checks.append({"check": name, "passed": True})
    try:
        facts = spark.read.parquet(str(DATA / "silver" / "facts"))
        sub = spark.read.parquet(str(DATA / "silver" / "sub"))
        num = spark.read.parquet(str(DATA / "silver" / "num"))
        check("Không nhân dòng ở join", facts.count() == num.count())
        check("Khóa SEC NUM duy nhất", duplicate_count(num, KEYS["num"]) == 0)
        check("Giữ khác chiều cùng khóa rút gọn", facts.filter("dimh = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'").count() == 1)
        check("Giữ iprx=2", facts.filter("iprx=2").count() == 1)
        check("Giữ iprx=0 theo metadata thực tế", facts.filter("iprx=0 AND dimh='0x00000000'").count() == 1)
        consolidated = spark.read.parquet(str(DATA / "silver" / "facts_consolidated"))
        check("Chọn fact ưu tiên thấp nhất và dimh có 0x", consolidated.filter("cik=1000 AND uom='USD' AND iprx=0").count() == 1)
        check("Parse timestamp có giây thập phân", sub.filter("cik=1000 AND accepted IS NOT NULL AND NOT invalid_accepted").count() == 1)
        check("Loại số quý thập phân sai kiểu", facts.filter("qtrs_raw='1.5'").count() == 0)
        check("Loại ngày không tồn tại", facts.filter("ddate_raw='20240230'").count() == 0)
        check("Loại tag mồ côi", facts.filter("tag='UnknownTag'").count() == 0)
        check("Giữ giá trị thiếu, không điền 0", facts.filter("uom='shares' AND value IS NULL").count() == 1)
        check("Giữ raw và cờ lỗi số", facts.filter("value_raw='bad-number' AND value IS NULL AND invalid_value").count() == 1)
        check("Giữ số âm EUR", facts.filter("uom='EUR' AND value=-42").count() == 1)
        check("Giữ số 0", facts.filter("uom='CustomUnit' AND value=0").count() == 1)
        check("Giữ outlier có gắn cờ", facts.filter("value=1000000000000 AND is_outlier").count() == 1)
        check("Chọn bản sửa ở quý sau", sub.filter("cik=2000 AND form='10-K/A'").count() == 1)
        check("Bỏ hồ sơ cũ khỏi Silver", sub.filter("adsh='0000002000-24-000001'").count() == 0)
        check("Giữ và cảnh báo prevrpt chưa giải quyết", sub.filter("cik=3000 AND unresolved_prevrpt").count() == 1)
        check("Tên công ty đã chuẩn hóa", sub.filter("cik=1000 AND name='COMPANY 0 LTD'").count() == 1)
        check("Lọc form 8-K", sub.filter("form='8-K'").count() == 0)
        check("Đủ log 12 bước", {f"{i:02d}" for i in range(1,13)} <=
            {json.loads(x)["step"][:2] for x in (REPORTS / "cleaning_log.jsonl").read_text().splitlines()})
        write_json(REPORTS / "verification.json", {"synthetic_only": True, "checks": checks})
        print(f"PASS {len(checks)} kiểm tra dữ liệu nhân tạo")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
