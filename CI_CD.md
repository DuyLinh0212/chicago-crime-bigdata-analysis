# CI/CD trên GitHub Actions

Mỗi lần push nhánh `main`, workflow chạy toàn bộ sáu CSV 2018–2023 trên Ubuntu của GitHub. Trong tab **Actions**, chọn workflow **HDFS - Chuẩn hóa - Phân tích**, mở từng bước để xem log tiếng Việt. Có thể bấm **Run workflow** để chạy lại thủ công.

## Mốc 1: HDFS và Parquet

Workflow tạo NameNode và ba DataNode độc lập, replication 2, block size 128 MiB. Sau khi kiểm tra SHA-256 nguồn, uploader đưa sáu CSV lên Bronze và đối chiếu lại SHA-256 bằng cách đọc file từ HDFS. Normalize dựng lại Silver hai lần và so sánh kết quả. CI đọc lại Parquet, kiểm tra đủ 72 phân vùng, schema, nén Snappy và hai bản sao block thực tế.

Các bài kiểm tra mẫu gồm ngày/giờ, lỗi header năm 2023, tọa độ, null, trùng lặp, chạy lại và khôi phục khi ghi/thay thư mục thất bại. Tất cả kiểm tra chỉ được chạy trên GitHub trong đợt triển khai này.

## Lấy ảnh và dữ liệu

Ở cuối trang một lần chạy, mục **Artifacts** có:

- `bang-chung-<run>-<attempt>`: log, checksum, manifest, schema, ảnh giao diện HDFS và ảnh hiển thị log thật.
- `silver-parquet-<run>-<attempt>`: dữ liệu Parquet Silver theo năm/tháng.
- `phan-tich-bao-cao-<run>-<attempt>`: 15 biểu đồ PNG, bảng CSV, manifest và báo cáo HTML/ảnh tổng quan. Giải nén toàn bộ artifact rồi mở `report/index.html`; giữ nguyên thư mục `analystics` bên cạnh để HTML tải được ảnh.

Artifact giữ 14 ngày. Mở thư mục `screenshots` sau khi giải nén để lấy ảnh; mở `logs` để xem toàn bộ log. Code chụp ảnh nằm riêng trong thư mục `screenshots` của repo. Ảnh log là ảnh trình bày nội dung log thực tế, không phải ảnh trang GitHub đã đăng nhập. SHA commit và mã lần chạy được ghi trên ảnh.

Log nghiệp vụ, tên bước và thông báo do dự án tạo dùng tiếng Việt. Log nội bộ Spark/Hadoop được giữ nguyên trong bằng chứng chẩn đoán.

## Môi trường và dữ liệu

HDFS được tạo mới trên runner và xóa sau lần chạy; các kết quả cần giữ được xuất artifact trước khi dọn. Cấu hình trong `ci` độc lập với HDFS Ubuntu WSL hiện có trên máy. Không cần khởi động Docker/WSL tại máy cá nhân để CI chạy.

Sáu CSV được commit nguyên byte, `.gitattributes` không đổi xuống dòng CSV. `data/checksums.json` ghi SHA-256 nguồn. Khi cố ý thay dữ liệu, cập nhật manifest này trong cùng commit. Dữ liệu không có cột `district` (quận cảnh sát); `community_area` là khu dân cư, không được xem là district.

## Mốc 2: Bốn phân tích từ Silver

Thư mục `analystics` có đúng bốn chương trình: `temporal_analysis.py`, `crime_type_analysis.py`, `spatial_analysis.py`, `cross_analysis.py`. Mỗi chương trình cung cấp `analyze(spark, silver_path, output_dir, gold_path) -> dict` và CLI chạy bằng spark-submit với ba tham số `--silver-path`, `--output-dir`, `--gold-path`.

Các chương trình tổng hợp bằng Spark, chỉ chuyển bảng kết quả nhỏ sang Pandas/Matplotlib. Tổng theo từng chiều phải khớp Silver; ảnh được kiểm tra checksum, kích thước và khả năng mở; font DejaVu Sans được kiểm tra glyph tiếng Việt. Múi giờ America/Chicago; các khung giờ 00–05, 06–11, 12–17, 18–23. Giữ mọi trường hợp đồng hạng và bổ sung mốc thời gian không có vụ bằng 0.

So sánh bốn nhóm dùng THEFT, ROBBERY, ASSAULT + BATTERY, HOMICIDE; trộm xe MOTOR VEHICLE THEFT được giữ riêng. Khu/địa điểm thiếu nằm trong nhóm Không xác định. Số vụ là số bản ghi sau khử trùng 12 trường, không phải phép đếm ID vụ việc.

Gold lưu theo `/chicago-crimes/gold/<run-id>/analystics/<nhóm>` và `/chicago-crimes/gold/<run-id>/report`; có bảng số liệu CSV, PNG, manifest và HTML. Workflow kiểm tra đủ 15 ảnh trước khi xuất báo cáo. Các log nghiệp vụ đều bằng tiếng Việt.

Mốc 1 đã thành công: [run 39773189166](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis/actions/runs/39773189166), 1.450.979 dòng nguồn → 1.448.706 dòng Silver, loại 2.273 dòng trùng.

Mốc 2 đã thành công: [run 39775032785](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis/actions/runs/39775032785), đủ 15 biểu đồ, bảng CSV, manifest, HTML và PNG tổng quan; ảnh báo cáo cũng được ghi và kiểm chứng trên Gold.

## Mốc 3: Airflow điều phối thật

Airflow 3.3.2/Python 3.11 được cài trên GitHub runner bằng constraints chính thức; metadata dùng PostgreSQL 16. API server, scheduler và DAG processor chạy thật. DAG `chicago_crimes_pipeline` nằm trong `airflow/dags`, có 13 task; tối đa một DAG run và một task đồng thời. Bốn phân tích nằm trong TaskGroup, thực thi lần lượt để giới hạn RAM. Các task gọi cùng CLI `python -m pipeline.stages <bước>` đã đạt CI ở hai mốc trước.

Workflow khởi động HDFS/PostgreSQL rồi kiểm tra dữ liệu mẫu. Tiếp theo `ci/run_airflow.py` kích hoạt hai DAG run qua REST API:

1. `*_thu_loi`: task kiểm tra đầu vào cố ý thoát lỗi; CI yêu cầu DAG có trạng thái **failed**. Đây là lần thử lỗi mong đợi, không phải lỗi của lần chạy chính.
2. `*_thanh_cong`: upload toàn bộ dữ liệu, Normalize hai lần, kiểm tra Silver, chạy bốn phân tích, tạo HTML/PNG, công bố Gold, xuất Silver và xác nhận đầu ra. CI yêu cầu đủ **13 task success**.

Không đánh dấu thành công thủ công, không dùng task `all_done` để che lỗi. Bất kỳ trạng thái bất thường hay quá thời gian đều làm workflow thất bại. Sau DAG, workflow chụp giao diện HDFS/Airflow và xuất artifact. Các file `airflow-*.json` trong manifests lưu trạng thái thực tế; `airflow-task-logs` chứa log task đầy đủ.

API Airflow chỉ lắng nghe loopback của runner; chế độ quản trị đơn giản không mật khẩu chỉ dùng cho môi trường CI tạm thời. DAG không tự lập lịch hằng ngày, không backfill. Mỗi push main hoặc Run workflow là một lượt chạy toàn bộ. Không cần tự tạo GitHub secrets để chạy cấu hình CI này.
