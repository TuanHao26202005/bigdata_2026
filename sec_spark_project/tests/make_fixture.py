"""Tạo ZIP và TSV giả lập 4 quý để chạy thử không cần tải dữ liệu thật."""
import csv
import shutil
import zipfile
from pathlib import Path
from src.schemas import COLUMNS


def make_fixture(root):
    root = Path(root)
    marker = root / "SYNTHETIC_DATA_ONLY.txt"
    raw = root / "data" / "raw"
    if raw.exists():
        if not marker.exists():
            raise RuntimeError("Không ghi fixture vào thư mục đang có Raw không được đánh dấu nhân tạo")
        shutil.rmtree(raw)  # Buộc ingest đọc ZIP fixture vừa tạo ở lần chạy này.
    (root / "data" / "downloads").mkdir(parents=True, exist_ok=True)
    rows = {q: {t: [] for t in COLUMNS} for q in ("2024q1", "2024q2", "2024q3", "2024q4")}
    revenue = "RevenueFromContractWithCustomerExcludingAssessedTax"
    version = "us-gaap/2024"
    def sub(adsh, cik, **kwargs):
        return dict(adsh=adsh, cik=str(cik), name="  example   corp ", fy="2024", fp="FY",
            form="10-K", period="20240930", filed="20241015", accepted="2024-10-15 12:00:00",
            prevrpt="0", wksi="0", detail="1", nciks="1", **kwargs)
    def fact(adsh, value="100", **kwargs):
        item = dict(adsh=adsh, tag=revenue, version=version, ddate="20240930", qtrs="4",
            uom="USD", dimh="0"*32, iprx="1", value=value, coreg="", dimn="0",
            footlen="0", durp="0", datp="0", dcml="0")
        item.update(kwargs)
        return item
    def pre(adsh):
        return dict(adsh=adsh, report="1", line="1", stmt="IS", inpth="0", tag=revenue,
                    version=version, prole="label", plabel="Revenue", negating="0")
    for i in range(28):
        q = f"2024q{i%4+1}"
        adsh = f"{1000+i:010d}-24-000001"
        row = sub(adsh, 1000+i)
        row["name"] = f"  company   {i} ltd "
        rows[q]["sub"].append(row)
        rows[q]["num"].append(fact(adsh, str(100+i if i < 27 else 1000000000000)))
        rows[q]["pre"].append(pre(adsh))
    first = rows["2024q1"]["num"][0]
    rows["2024q1"]["num"].append(dict(first))  # Bản sao thật.
    rows["2024q1"]["num"].append(dict(first, dimh="a"*32, dimn="1", value="50"))
    rows["2024q1"]["num"].append(dict(first, iprx="2", value="101"))
    rows["2024q1"]["num"].append(dict(first, iprx="0", dimh="0x00000000", value="99"))
    rows["2024q1"]["sub"][0]["accepted"] = "2024-10-15 12:00:00.0"
    rows["2024q1"]["num"].append(dict(first, value="12", ddate="20240230"))
    rows["2024q1"]["num"].append(dict(first, tag="UnknownTag", value="5"))
    rows["2024q1"]["num"].append(dict(first, adsh="9999999999-24-000001", value="8"))
    rows["2024q1"]["num"].append(dict(first, uom=" EUR ", value="-42"))
    rows["2024q1"]["num"].append(dict(first, uom="shares", value=" "))
    rows["2024q1"]["num"].append(dict(first, uom="pure", value="bad-number"))
    rows["2024q1"]["num"].append(dict(first, uom="CustomUnit", value="0"))
    rows["2024q1"]["num"].append(dict(first, qtrs="1.5", value="17"))
    old = "0000002000-24-000001"
    new = "0000002000-24-000002"
    old_sub = sub(old, 2000)
    old_sub.update(prevrpt="1", filed="20240201", accepted="2024-02-01 12:00:00")
    new_sub = sub(new, 2000)
    new_sub.update(form="10-K/A", filed="20240501", accepted="2024-05-01 12:00:00")
    for q, row in (("2024q1", old_sub), ("2024q2", new_sub)):
        rows[q]["sub"].append(row)
        rows[q]["num"].append(fact(row["adsh"], "400"))
        rows[q]["pre"].append(pre(row["adsh"]))
    # Bản cờ prevrpt=1 nhưng thiếu bản thay thế trong phạm vi phải được giữ và gắn cờ.
    unresolved = sub("0000003000-24-000001", 3000)
    unresolved.update(prevrpt="1", name=" ")
    rows["2024q3"]["sub"].append(unresolved)
    rows["2024q3"]["num"].append(fact(unresolved["adsh"], "77", uom="usd"))
    rows["2024q3"]["pre"].append(pre(unresolved["adsh"]))
    scope = sub("0000004000-24-000001", 4000)
    scope["form"] = "8-K"
    rows["2024q4"]["sub"].append(scope)
    rows["2024q4"]["num"].append(fact(scope["adsh"]))
    rows["2024q4"]["pre"].append(pre(scope["adsh"]))
    bad = sub("0000005000-24-000001", 5000)
    bad["fy"] = "bad-year"
    rows["2024q4"]["sub"].append(bad)
    for q, tables in rows.items():
        tables["tag"].append(dict(tag=revenue, version=version, custom="0", abstract="0",
            datatype="monetary", iord="D", crdr="C", tlabel="Revenue", doc="Synthetic test tag"))
        raw = root / "fixture_tsv" / q
        raw.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(root / "data" / "downloads" / f"{q}_notes.zip", "w", zipfile.ZIP_DEFLATED) as zf:
            for table, items in tables.items():
                path = raw / f"{table}.tsv"
                with path.open("w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=COLUMNS[table], delimiter="\t", lineterminator="\n")
                    writer.writeheader()
                    writer.writerows(items)
                zf.write(path, path.name)
    (root / "SYNTHETIC_DATA_ONLY.txt").write_text("Dữ liệu nhân tạo để kiểm thử, KHÔNG phải SEC thật.\n", encoding="utf-8")
    shutil.rmtree(root / "fixture_tsv")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    make_fixture(parser.parse_args().output)
