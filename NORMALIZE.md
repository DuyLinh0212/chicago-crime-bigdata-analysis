# Chuẩn hóa Bronze thành Parquet Silver

`normalize.py` chạy bằng PySpark trong Ubuntu WSL và kết nối HDFS có sẵn. Job đọc CSV trực tiếp từ HDFS Bronze, ghi Parquet nén Snappy vào Silver và tạo đủ 72 thư mục năm/tháng cho giai đoạn 2018–2023.

## 1. Chuẩn bị HDFS và dữ liệu

Kiểm tra Ubuntu WSL mở được rồi chạy từ PowerShell:

```powershell
cd D:\BigData\TH_Bigdata
.\scripts\start_3_datanodes.ps1
python .\wsl_runtime.py hdfs dfs -ls -R /chicago-crimes/bronze
```

Bronze cần chứa các CSV đã upload, ví dụ:

```text
/chicago-crimes/bronze/ingest_date=2026-10-15/chicago_crime_2018.csv
/chicago-crimes/bronze/ingest_date=2026-10-15/chicago_crime_2023.csv
```

Nếu Bronze chưa có dữ liệu, thực hiện hướng dẫn trong [UPLOAD_TO_HDFS.md](UPLOAD_TO_HDFS.md). Job chuẩn hóa không đọc thư mục `data` trên Windows và không tự upload lại dữ liệu.

## 2. Chạy chuẩn hóa

Chạy script PowerShell từ thư mục dự án:

```powershell
.\scripts\run_normalize.ps1
```

Script tự xác định thư mục dự án và dùng UTF-8 để hiển thị thông báo tiếng Việt. Có thể gọi script bằng đường dẫn tuyệt đối từ thư mục khác.

Lệnh Python tương đương:

```powershell
python .\wsl_runtime.py normalize
```

Spark chạy một lần trong Ubuntu, dùng `local[2]`, driver memory `1g`. Script đọc địa chỉ NameNode từ cấu hình gốc, dùng chính user Ubuntu `linh` và chờ đủ ba DataNode hoạt động. File mới được ghi với replication `2`, block size `134217728` byte. Không cần Docker Desktop cho luồng chuẩn hóa.

Nếu Ubuntu có Spark, script dùng bản đó; nếu chưa có, dùng chung bộ PySpark 3.5.1 đã cài trên Windows qua `/mnt`, không tải hoặc copy thêm package. Spark 3.5.1 cần Python 3.8–3.11: script ưu tiên Python 3.11 do uv quản lý đã có trong Ubuntu, không dùng Python hệ thống 3.14. Có thể chọn rõ `spark_home` và `spark_python` trong `hdfs_wsl.json`.

Thông báo của job bằng tiếng Việt; một số log nội bộ của Spark/Hadoop vẫn bằng tiếng Anh.

## 3. Quy tắc dữ liệu

- Quét đệ quy mọi CSV trong Bronze, kể cả nhiều ngày ingest. Bỏ qua file hoặc thư mục có tên bắt đầu bằng `.` hoặc `_`.
- Đầu vào là CSV UTF-8, có header và một bản ghi trên mỗi dòng như sáu file nguồn hiện tại. Hỗ trợ dấu phẩy và dấu ngoặc kép đã escape trong trường. JSONL và CSV có trường chứa xuống dòng chưa được hỗ trợ.
- Header phải có đúng 12 cột của dữ liệu hiện tại; thứ tự cột có thể khác nhau. Đổi `primery_description` thành `primary_description`. Sai header làm job dừng trước khi thay Silver.
- Thu gọn khoảng trắng, chuyển chuỗi rỗng thành `null`; viết hoa địa chỉ, loại tội phạm và mô tả, giữ dấu câu.
- Chuyển ngày dạng `M/d/yyyy H:mm`, dạng có giây hoặc AM/PM và ISO thành timestamp. Phiên Spark dùng múi giờ `America/Chicago`; chuỗi ISO có múi giờ được đổi về giờ Chicago trước khi tính năm/tháng.
- `arrest`, `domestic`: nhận `TRUE/FALSE` không phân biệt hoa thường hoặc `1/0`; giá trị khác thành `null`.
- `community_area`: số nguyên 1–77, chấp nhận dạng số nguyên có phần thập phân bằng 0 như `25.0`; giá trị khác thành `null`.
- Tọa độ phải là số hữu hạn: latitude từ -90 đến 90, longitude từ -180 đến 180. Giữ dòng thiếu/sai tọa độ, đặt trường sai thành `null`. Tạo lại `location` khi có đủ hai tọa độ hợp lệ.
- Tính lại `year` từ ngày báo cáo, tạo `month` dạng `01`–`12`.
- Loại dòng sai số trường CSV, lỗi dấu ngoặc kép, sai ngày, thiếu `primary_type` hoặc ngày ngoài 2018–2023. In tổng số dòng bị loại và các lý do; một dòng có thể có nhiều lý do.
- Khử trùng hoàn toàn theo 12 trường đã chuẩn hóa. Đường dẫn file và ngày ingest không tham gia so trùng, nên bản sao upload ở nhiều ngày sẽ được gộp.

Vì dữ liệu không có ID vụ việc, hai vụ khác nhau có đủ 12 trường giống nhau cũng sẽ được gộp theo quy tắc khử trùng đã chọn.

## 4. Đầu ra và kiểm tra

```text
/chicago-crimes/silver/
├── year=2018/
│   ├── month=01/part-*.snappy.parquet
│   ├── ...
│   └── month=12/
├── ...
└── year=2023/month=12/
```

Tháng không có dữ liệu vẫn có thư mục, nhưng không tạo file Parquet giả. Tên cụ thể của file `part-*` do Spark sinh ra.

| Cột | Kiểu dữ liệu |
| --- | --- |
| `date_reported` | timestamp, ghi Parquet dạng TIMESTAMP_MICROS |
| `city_block`, `primary_type`, `primary_description`, `location_description`, `location` | string |
| `arrest`, `domestic` | boolean |
| `community_area`, `year` | integer |
| `latitude`, `longitude` | double |
| `month` | string hai chữ số |

`year` và `month` là các cột phân vùng, lưu trong đường dẫn thư mục. Spark có thể suy luận `month` thành số khi tự đọc dataset; dùng schema tường minh như `SILVER_SCHEMA` trong `normalize.py` nếu cần giữ kiểu chuỗi hai chữ số.

```powershell
python .\wsl_runtime.py hdfs dfs -ls -R /chicago-crimes/silver
python .\wsl_runtime.py hdfs dfs -du -h /chicago-crimes/silver
```

Giao diện duyệt HDFS: <http://localhost:9870/explorer.html#/chicago-crimes/silver>.

Job đối chiếu: `đầu vào = dòng lỗi + dòng trùng bị loại + dòng Silver`. Sau khi ghi, job đọc lại Parquet để kiểm tra số dòng, kiểu dữ liệu vật lý, phân vùng, replication và block size trước khi thay Silver.

## 5. Chạy lại và xử lý thất bại

Mỗi lần chạy dựng lại toàn bộ Silver từ mọi CSV hiện có trong Bronze. File Parquet được ghi trước vào `/chicago-crimes/_staging/normalize-<mã-lần-chạy>/silver`. Sau khi kiểm tra thành công, job chuyển Silver cũ sang backup, chuyển dữ liệu mới vào Silver rồi xóa backup.

Nếu không có CSV, không có dòng hợp lệ hoặc ghi/kiểm tra staging thất bại, Silver hiện có được giữ nguyên. Nếu thay thư mục thất bại, job thử khôi phục Silver cũ. Bản ghi Bronze không bị sửa hoặc xóa.

Chỉ chạy một job chuẩn hóa tại một thời điểm và chờ job hoàn tất trước khi chạy Spark SQL đọc Silver. Trong lúc thay thư mục có một khoảng ngắn Silver chưa sẵn sàng. Cần dung lượng cho bản Silver mới trong staging cùng bản Silver cũ.

Nếu tiến trình Spark/Ubuntu bị dừng đột ngột hoặc máy mất điện, kiểm tra `/chicago-crimes/_staging/`. Bản cũ có thể còn trong `normalize-<mã-lần-chạy>/silver-backup`. Nếu đường dẫn Silver đã mất, cần chuyển backup đó về `/chicago-crimes/silver` trước lần chạy tiếp theo; không xóa thư mục chứa backup chưa được kiểm tra.

## 6. Kiểm tra bằng dữ liệu mẫu

Lệnh sau chạy kiểm tra bằng Spark trong Ubuntu, ghi dữ liệu mẫu vào `/tmp` và không ghi dữ liệu lên HDFS:

```powershell
python .\wsl_runtime.py check-normalize
```

Các tình huống gồm: sửa header 2023, đổi kiểu, ISO và múi giờ, tọa độ thiếu/sai, CSV hỏng, khử trùng giữa file, nén Snappy, đủ 72 thư mục, chạy lại giữ số dòng ổn định và khôi phục bản cũ khi thay Silver thất bại. Cấu hình HDFS thực tế vẫn cần cụm ba DataNode đang chạy để kiểm tra end-to-end.

Kiểm tra bridge WSL và xử lý cấu hình bằng fixture, chạy được ngay trên Windows:

```powershell
python -m unittest discover -s tests -p test_wsl_runtime.py -v
```
