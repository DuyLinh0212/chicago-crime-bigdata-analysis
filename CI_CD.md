# CI/CD trên GitHub Actions

Mỗi lần push nhánh `main`, workflow chạy toàn bộ sáu CSV 2018–2023 trên Ubuntu của GitHub. Trong tab **Actions**, chọn workflow **HDFS - Chuẩn hóa - Phân tích**, mở từng bước để xem log tiếng Việt. Có thể bấm **Run workflow** để chạy lại thủ công.

## Mốc 1: HDFS và Parquet

Workflow tạo NameNode và ba DataNode độc lập, replication 2, block size 128 MiB. Sau khi kiểm tra SHA-256 nguồn, uploader đưa sáu CSV lên Bronze và đối chiếu lại SHA-256 bằng cách đọc file từ HDFS. Normalize dựng lại Silver hai lần và so sánh kết quả. CI đọc lại Parquet, kiểm tra đủ 72 phân vùng, schema, nén Snappy và hai bản sao block thực tế.

Các bài kiểm tra mẫu gồm ngày/giờ, lỗi header năm 2023, tọa độ, null, trùng lặp, chạy lại và khôi phục khi ghi/thay thư mục thất bại. Tất cả kiểm tra chỉ được chạy trên GitHub trong đợt triển khai này.

## Lấy ảnh và dữ liệu

Ở cuối trang một lần chạy, mục **Artifacts** có:

- `bang-chung-<run>-<attempt>`: log, checksum, manifest, schema, ảnh giao diện HDFS và ảnh hiển thị log thật.
- `silver-parquet-<run>-<attempt>`: dữ liệu Parquet Silver theo năm/tháng.

Artifact giữ 14 ngày. Mở thư mục `screenshots` sau khi giải nén để lấy ảnh; mở `logs` để xem toàn bộ log. Code chụp ảnh nằm riêng trong thư mục `screenshots` của repo. Ảnh log là ảnh trình bày nội dung log thực tế, không phải ảnh trang GitHub đã đăng nhập. SHA commit và mã lần chạy được ghi trên ảnh.

Log nghiệp vụ, tên bước và thông báo do dự án tạo dùng tiếng Việt. Log nội bộ Spark/Hadoop được giữ nguyên trong bằng chứng chẩn đoán.

## Môi trường và dữ liệu

HDFS được tạo mới trên runner và xóa sau lần chạy; các kết quả cần giữ được xuất artifact trước khi dọn. Cấu hình trong `ci` độc lập với HDFS Ubuntu WSL hiện có trên máy. Không cần khởi động Docker/WSL tại máy cá nhân để CI chạy.

Sáu CSV được commit nguyên byte, `.gitattributes` không đổi xuống dòng CSV. `data/checksums.json` ghi SHA-256 nguồn. Khi cố ý thay dữ liệu, cập nhật manifest này trong cùng commit. Dữ liệu không có cột `district` (quận cảnh sát); `community_area` là khu dân cư, không được xem là district.
