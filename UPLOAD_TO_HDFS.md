# Hướng dẫn tải dữ liệu lên HDFS

## 1. Chuẩn bị

- Mở được Ubuntu WSL; Hadoop và cấu hình ba DataNode đã có sẵn trong Ubuntu.
- Kiểm tra distro, user và đường dẫn trong `hdfs_wsl.json`.
- Chép các file cần tải vào `D:\BigData\TH_Bigdata\data`. Có thể giữ thư mục con; uploader sẽ giữ cấu trúc tương đối đó trên HDFS.
- Chạy lệnh từ PowerShell. Không cần cài thêm Python package.

Uploader truyền từng file qua stdin của lệnh `hdfs dfs -put` trong Ubuntu WSL, không tạo thêm bản sao dữ liệu tạm. Nội dung file được giữ nguyên; script không chuyển CSV sang JSONL hoặc Parquet. Nếu dùng Bronze cho dữ liệu Kafka JSONL, hãy đặt các file JSONL vào `data`.

## 2. Bật NameNode và 3 DataNode

Từ thư mục dự án:

```powershell
cd D:\BigData\TH_Bigdata
.\scripts\start_3_datanodes.ps1
```

Script dùng lại Hadoop trong Ubuntu, đọc cấu hình NameNode và `dn1`, `dn2`, `dn3`, kiểm tra metadata/clusterID và khởi động các tiến trình còn thiếu. Cấu trúc HDFS được tạo bằng `mkdir -p`, không ghi đè file. Không tải image Docker hoặc format lại NameNode.

## 3. Tải dữ liệu

Mặc định, file được tải vào phân vùng theo ngày hiện tại:

```powershell
python .\upload_to_hdfs.py
```

Chọn ngày ingest cụ thể:

```powershell
python .\upload_to_hdfs.py --ingest-date 2026-10-15
```

Nếu file cùng đường dẫn đã có trong phân vùng ngày đó, script bỏ qua file để tránh ghi đè. Muốn tải lại và ghi đè:

```powershell
python .\upload_to_hdfs.py --ingest-date 2026-10-15 --overwrite
```

Đường dẫn đích có dạng:

```text
/chicago-crimes/bronze/ingest_date=YYYY-MM-DD/<đường-dẫn-tương-đối-trong-data>
```

Ví dụ `data\kafka\crimes.jsonl` được lưu thành `/chicago-crimes/bronze/ingest_date=2026-10-15/kafka/crimes.jsonl`.

## 4. Kiểm tra file trên HDFS

```powershell
python .\wsl_runtime.py hdfs dfs -ls -R /chicago-crimes/bronze
```

Giao diện NameNode: <http://localhost:9870>.

## Lưu ý

- Thư mục `D:\BigData\TH_Bigdata\data` hiện là nguồn duy nhất mà script quét; file đặt ngoài thư mục đó sẽ không được tải.
- Mỗi lần chạy mặc định tạo/ghi vào phân vùng ngày hiện tại. Chọn `--ingest-date` để tải lại vào một phân vùng khác.
- HDFS đang dùng metadata/block gốc của Ubuntu trong `/home/linh/hadoop_data`; cấu hình riêng của dự án nằm tại `~/.cache/chicago-crimes-hdfs/`.
- HDFS Ubuntu khác namespace với cụm Docker trước đây. Kiểm tra Bronze trên Ubuntu trước khi upload lại; script không tự chuyển hoặc xóa dữ liệu trong Docker.
- Kiểm tra trạng thái: `python .\wsl_runtime.py status`. Nếu WSL không phản hồi hoặc báo `0x8007274c`, cần khôi phục Ubuntu trước khi chạy upload.
