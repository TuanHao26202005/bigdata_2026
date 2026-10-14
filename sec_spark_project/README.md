# SEC Spark 2024 Full Project

## Mục tiêu

Đây là đồ án xử lý dữ liệu lớn bằng **Apache Spark** với bộ dữ liệu **SEC Financial Statement and Notes** năm 2024.

Dự án tập trung vào:

- tải và giải nén dữ liệu SEC;
- nạp dữ liệu thành **Bronze**;
- profiling / EDA;
- làm sạch dữ liệu bằng Spark;
- tạo **Silver**, **facts**, và **facts_consolidated**;
- xuất báo cáo chất lượng và benchmark;
- lưu dữ liệu sau làm sạch thêm dưới dạng **TSV theo từng quý**.

## Luồng hoạt động

```text
SEC ZIP
  ↓
Raw TSV
  ↓
Bronze Parquet
  ↓
EDA / profiling
  ↓
Cleaning bằng Spark
  ↓
Silver Parquet
  ↓
facts / facts_consolidated
  ↓
Reports + manifest + TSV xuất theo quý
```

## Cấu trúc dữ liệu lưu trữ

| Giai đoạn | Định dạng | Thư mục |
|---|---|---|
| Raw | TSV/TXT gốc | `data/raw/<quarter>/` |
| Bronze | Parquet | `data/bronze/<table>/<quarter>/` |
| Work checkpoint | Parquet | `data/work/<step>/<table>/` |
| Quarantine | Parquet | `data/quarantine/<step>/<table>/` |
| Silver | Parquet | `data/silver/<table>/` |
| Facts | Parquet | `data/silver/facts/` |
| Facts consolidated | Parquet | `data/silver/facts_consolidated/` |
| TSV sau làm sạch | TSV theo quý | `data/cleaned_tsv/<table>/<quarter>/` |

Parquet là định dạng chính để xử lý vì giữ schema tốt, hỗ trợ partition và đọc nhanh bằng Spark. TSV sau làm sạch chỉ dùng để trao đổi, kiểm tra thủ công hoặc nộp mẫu dữ liệu.

## `facts` và `facts_consolidated`

- `facts`: bảng fact đầy đủ sau khi join `NUM + SUB + TAG`.
- `facts_consolidated`: lát cắt rút gọn của `facts`, chỉ giữ một bản ghi đại diện theo khóa phân tích, loại các biến thể dimension không cần thiết.

Tóm tắt:

```text
facts = dữ liệu đầy đủ sau join
facts_consolidated = dữ liệu rút gọn để truy vấn gọn hơn
```

## Kết quả sau khi chạy

Sau mỗi lần chạy, dự án sinh các manifest và báo cáo:

- `reports/storage_before_cleaning.json`
- `reports/storage_after_cleaning.json`
- `reports/storage_after_cleaning_tsv.json`
- `reports/cleaning_log.jsonl`
- `reports/quality_report.md`
- `reports/before_after_rows.csv`
- `reports/before_after_nulls.csv`
- `reports/benchmark.csv`
- `reports/environment.json`
- `reports/run_scope.json`

## Yêu cầu môi trường

- Python 3.10–3.12
- Java 17
- PySpark 3.5.6
- Linux / WSL2 / Google Colab

## Cài đặt

### Linux / WSL2

```bash
sudo apt-get update
sudo apt-get install -y openjdk-17-jdk python3-venv unzip
cd sec_spark_project
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Dữ liệu đầu vào

Project dùng **SEC Financial Statement and Notes** với 4 quý:

- `2024q1`
- `2024q2`
- `2024q3`
- `2024q4`

Bạn có thể:

1. Tải thủ công 4 file ZIP vào `data/downloads/`
2. Hoặc đặt biến môi trường `SEC_USER_AGENT` bằng tên dự án và email liên hệ thật để cho phép download tự động.

## Chạy toàn bộ pipeline

```bash
python -m src.run all
```

Hoặc chạy từng bước:

```bash
python -m src.run ingest
python -m src.run eda
python -m src.run clean
python -m src.run report
```

## Notebook

- `notebooks/01_ingest.ipynb`
- `notebooks/02_eda.ipynb`
- `notebooks/03_clean.ipynb`
- `notebooks/04_report.ipynb`

Chạy theo đúng thứ tự trên.

## Phần cleaning chính

Pipeline làm sạch thực hiện các bước chính:

1. Hợp nhất 4 quý.
2. Trim khoảng trắng và đổi chuỗi rỗng thành null.
3. Ép kiểu dữ liệu và giữ cột raw.
4. Chuẩn hóa tên công ty và một số trường nghiệp vụ.
5. Lọc bản ghi thiếu khóa bắt buộc.
6. Chuẩn hóa đơn vị đo.
7. Loại duplicate và ghi nhận key conflict.
8. Lọc orphan theo SUB/TAG.
9. Lọc form ngoài phạm vi.
10. Chọn filing mới nhất trong nhóm revision.
11. Gắn cờ outlier.
12. Join tạo `facts` và `facts_consolidated`.

Mỗi bước đều có log trước/sau để kiểm tra số dòng và dễ audit.

## Xuất TSV sau làm sạch

Ngoài Parquet, pipeline còn xuất TSV theo từng quý vào:

```text
data/cleaned_tsv/<table>/<quarter>/
```

Mỗi thư mục quý chứa một file `part-*.tsv`. Pipeline dùng `coalesce(1)` cho bước xuất TSV để gộp dữ liệu của từng bảng/quý thành một file; Parquet Bronze/Silver vẫn giữ nhiều part để xử lý phân tán. Nếu muốn đọc lại bằng Spark:

```python
spark.read.option("header", True).option("sep", "\t").csv("data/cleaned_tsv/facts/2024q1")
```

## Lưu ý

- Dữ liệu raw, Bronze, Silver và TSV xuất ra là dữ liệu sinh ra khi chạy, không nên commit lên git.
- `facts` không thay thế `facts_consolidated`; hai bảng phục vụ mục đích khác nhau.
- Đồ án này dùng Spark local mode để phù hợp máy cá nhân, nhưng có thể đổi `SPARK_MASTER` nếu chạy trên cluster.
