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
