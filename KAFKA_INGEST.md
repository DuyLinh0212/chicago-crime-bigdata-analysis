# Kafka ingest theo ngày trên GitHub

## Chạy và xem kết quả

Push `main` để chạy workflow **Kafka ingest - Ba ngày mô phỏng**, hoặc vào **Actions → Kafka ingest - Ba ngày mô phỏng → Run workflow**. Workflow batch cũ tiếp tục chạy riêng trên runner khác.

Không cần chạy Docker, Spark, Airflow hoặc tải thêm image trên máy cá nhân. GitHub dựng môi trường tạm thời cho mỗi lần chạy; ba ngày lịch sử được mô phỏng liên tiếp trong cùng workflow, không chờ ba ngày thực tế.

1. CI tạo một NameNode, ba DataNode, PostgreSQL và Kafka KRaft 3.9.2.
2. Kiểm tra checksum, upload sáu CSV và tạo Silver nền một lần.
3. FastAPI sinh 10 vụ giả/ngày cho 29, 30 và 31/12/2023. API trả cùng nội dung khi gọi lại cùng ngày.
4. Producer gọi API, gửi 10 vụ và một bản gửi lại để kiểm tra chống trùng; chờ broker xác nhận rồi gửi thông báo `day_ready` hai lần.
5. Consumer nghe topic thông báo, gọi Airflow REST API và xác nhận offset sau khi Airflow nhận yêu cầu. Mã DAG run cố định theo ngày ngăn kích hoạt trùng.
6. Airflow chạy Spark AvailableNow, chuẩn hóa bổ sung và đủ bốn phân tích, xuất báo cáo cho ngày đó. CI chỉ gửi ngày tiếp theo sau khi DAG thành công.

Mở từng bước workflow để chụp log trực tiếp. Bước **Mô phỏng ba ngày** in tiến độ DAG; log chi tiết của từng task và ảnh terminal nằm trong artifact.

## Artifact cần tải

| Artifact | Nội dung |
|---|---|
| `bang-chung-kafka-<run>-<attempt>` | Log đầy đủ, ảnh HDFS/DAG, ảnh hiển thị log thực tế dạng terminal, manifest, sự kiện gốc, offset, checkpoint, sổ lô và log task Airflow. |
| `bao-cao-kafka-ba-ngay-<run>-<attempt>` | Thư mục `2023-12-29`, `2023-12-30`, `2023-12-31`; mỗi ngày có `analystics/` với 15 PNG và CSV, `report/index.html` và `report/report-overview.png`. |
| `silver-parquet-kafka-<run>-<attempt>` | Silver cuối cùng sau khi bổ sung 30 vụ giả. Chỉ xuất khi workflow thành công. |

Giải nén báo cáo, giữ `analystics` và `report` cạnh nhau rồi mở `report/index.html`. Ảnh log có ghi commit SHA, workflow, ngày và mã lô; đây là ảnh trình bày log thực tế, không phải ảnh chụp giao diện terminal GitHub. Toàn bộ code chụp nằm trong `screenshots/`. Artifact giữ **14 ngày**.

## Dữ liệu và đường dẫn HDFS

```text
/chicago-crimes/
├── bronze/ingest_date=.../*.csv       # Sáu CSV nguyên bản
├── bronze/kafka/ingest_date=.../      # JSONL: event_json, topic, partition, offset, timestamp
├── silver/year=2018..2023/month=01..12/part-*.parquet
│   # File mới: silver/year=2023/month=12/part-demo-2023-12-29-0000.parquet
├── gold/<workflow-run>-<ngày>/       # Bốn phân tích và báo cáo theo ngày
├── _checkpoints/kafka-bronze/        # Checkpoint chung cho ba lần AvailableNow
└── _ingest/demo-<ngày>/              # Plan bất biến, dấu thử lỗi và xác nhận commit
```

Topic `chicago-crimes-events` có ba partition; `chicago-crimes-day-ready` có một partition. Kafka là một broker phục vụ CI, replication Kafka bằng 1. HDFS vẫn replication **2**, block size **134217728**, Parquet Snappy.

API có `/health` và `/crimes?date=2023-12-29`; chỉ nhận ba ngày đã chốt. Các vụ giả có `simulation=true`, `event_id` xác định và địa chỉ bắt đầu bằng `CI-DEMO-`. Bronze giữ nguyên JSON sự kiện cùng metadata Kafka. Silver giữ schema chuẩn hóa cũ; ghi chú mô phỏng được giữ trong manifest, nhãn biểu đồ và báo cáo.

## Cách bổ sung và phục hồi

Spark 3.5.1 dùng connector Kafka cùng phiên bản. AvailableNow xử lý dữ liệu có sẵn và kết thúc, checkpoint giữ vị trí cho ngày tiếp theo. Không truyền `endingOffsets` vào streaming query; Producer tạm dừng giữa các ngày, rồi đối chiếu dữ liệu Bronze với khoảng offset đã xác nhận.

Chuẩn hóa JSONL tái sử dụng quy tắc trong Normalize. Chống trùng theo 12 cột chuẩn hóa, so sánh null-safe với Silver tháng đích. Chỉ thêm file mới, không dựng lại Silver trong DAG ingest.

Parquet được ghi và kiểm tra trong staging; manifest chứa file nguồn, file đích, kích thước và checksum HDFS được lưu trước khi đổi tên file. Retry tiếp tục theo manifest; file đã có phải khớp checksum. Các task đọc Silver chỉ chạy sau khi lô được xác nhận, với một writer và một DAG run tại một thời điểm. Cơ chế này phục vụ pipeline CI, không cung cấp giao dịch nhiều writer cho Parquet thuần.

Ngày đầu CI cố ý lỗi sau khi chuyển file nhưng trước khi xác nhận lô. Airflow phải thử lại thành công. CI còn chạy lại lô cuối đã commit để chứng minh số dòng ổn định. Các thông điệp gửi lại được giữ trong Bronze nhưng loại khỏi Silver.

## Điều kiện CI thành công

- Đúng ba DAG run, mỗi run đủ 12 task thành công; ngày đầu có retry thực tế.
- 33 thông điệp Kafka tương ứng 30 vụ mới; tổng Silver tăng +10, +20, +30 so với nền.
- Với nguồn hiện tại: **1.448.706 → 1.448.736** bản ghi. CI luôn lấy số nền thực tế để kiểm tra.
- File Silver nền giữ nguyên checksum; đủ 72 thư mục, ba DataNode và hai bản sao mỗi block.
- Mỗi ngày có 15 biểu đồ, bảng số liệu, báo cáo và ảnh bằng chứng; số liệu khớp Silver và đúng commit.
- Khi task lỗi không khôi phục được, DAG/workflow thất bại; bước thu thập bằng chứng vẫn chạy.

Đây là dữ liệu nền thật được bổ sung các vụ **giả lập phục vụ thực hành**. Không sử dụng báo cáo Kafka này như thống kê thuần dữ liệu tội phạm thực tế. Cụm, Kafka và checkpoint trên runner kết thúc cùng workflow; artifact lưu bản bằng chứng để tải về.
