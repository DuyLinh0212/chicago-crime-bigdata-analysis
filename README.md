# Chicago Crimes — HDFS có sẵn trong Ubuntu WSL

## Chạy tự động trên GitHub

Repository: [chicago-crime-bigdata-analysis](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis).

Mỗi lần push `main` sẽ chạy toàn bộ dữ liệu trên GitHub Actions. Xem [hướng dẫn CI/CD, log và artifact](CI_CD.md). Các cấu hình CI nằm riêng trong `ci`, không cần chạy pipeline trên máy cá nhân.

Luồng chính dùng Hadoop hiện có trong Ubuntu, không tạo cụm HDFS Docker và không cần tải image NameNode/DataNode.

## Cấu hình đang sử dụng

File hdfs_wsl.json khai báo Ubuntu, user linh, Hadoop tại /home/linh/hadoop, cấu hình NameNode tại /home/linh/hadoop/etc/hadoop và ba cấu hình DataNode tại /home/linh/hadoop-conf/dn1, dn2, dn3.

NameNode gốc dùng hdfs://localhost:9000. Metadata và block hiện có tiếp tục được lưu trong /home/linh/hadoop_data. Script đọc đường dẫn từ XML, kiểm tra clusterID và không tự format NameNode.

Cấu hình riêng của dự án nằm trong ~/.cache/chicago-crimes-hdfs/conf, PID/log riêng nằm trong các thư mục pids và logs cùng cache. XML gốc không bị sửa. Replication=2 và block size=134217728 áp dụng cho các file mới do uploader/Spark của dự án ghi; các file HDFS cũ không tự đổi block size.

## Chạy từ PowerShell

Khởi động và kiểm tra HDFS:

```powershell
cd D:\BigData\TH_Bigdata
.\scripts\start_3_datanodes.ps1
python .\wsl_runtime.py status
python .\wsl_runtime.py hdfs dfs -ls -R /chicago-crimes/bronze
```

Nếu Bronze chưa có dữ liệu trên HDFS Ubuntu, upload trước rồi chạy chuẩn hóa:

```powershell
python .\upload_to_hdfs.py
.\scripts\run_normalize.ps1
```

Nếu Bronze đã có dữ liệu, chỉ cần chạy script chuẩn hóa.

Spark ưu tiên bản đã có trong Ubuntu; nếu chưa có, dùng chung thư mục PySpark đã cài trên Windows qua /mnt. Không tải/copy thêm bộ Spark. Với Spark 3.5.1, script chọn Python 3.8–3.11; máy hiện có Python 3.11 do uv quản lý trong Ubuntu, tránh Python hệ thống 3.14. Có thể đặt spark_home/spark_python trong hdfs_wsl.json để chọn rõ bản muốn dùng.

## Dữ liệu HDFS

/chicago-crimes/bronze/ingest_date=YYYY-MM-DD/ chứa dữ liệu nguồn CSV.

/chicago-crimes/silver/year=2018..2023/month=01..12/ chứa Parquet chuẩn hóa, nén Snappy.

/chicago-crimes/gold/ dành cho thống kê và ảnh báo cáo.

- [Hướng dẫn upload](UPLOAD_TO_HDFS.md)
- [Hướng dẫn chuẩn hóa](NORMALIZE.md)

HDFS Ubuntu và cụm Docker cũ là hai namespace riêng. File đã upload vào Docker trước đây không tự xuất hiện trong Ubuntu; hãy kiểm tra Bronze trước khi quyết định upload lại.

## Docker và WSL

compose.yaml hiện chỉ còn kiểm tra Spark tùy chọn. Cấu hình cụm Docker cũ được giữ tại compose.docker-legacy.yaml để tham khảo; các script chính không gọi file này. Không xóa image/volume Docker cũ trong quá trình chuyển đổi.

Nếu WSL báo 0x8007274c hoặc không phản hồi, cần mở/khôi phục Ubuntu trước khi chạy. Có thể kiểm tra bằng lệnh wsl -d Ubuntu -u linh --exec /bin/true. Script sẽ báo lỗi kết nối, không thay thế HDFS bằng cụm Docker mới.
