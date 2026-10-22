$ErrorActionPreference = "Stop"

# UTF-8 cho Windows PowerShell và terminal hiển thị tiếng Việt.
$utf8 = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8
$OutputEncoding = $utf8
if ($env:OS -eq "Windows_NT") {
    & "$env:SystemRoot\System32\chcp.com" 65001 > $null
}

$projectRoot = Split-Path -Parent $PSScriptRoot
$runtimeFile = Join-Path $projectRoot "wsl_runtime.py"
$normalizeFile = Join-Path $projectRoot "normalize.py"

foreach ($requiredFile in @($runtimeFile, $normalizeFile)) {
    if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
        throw "Không tìm thấy file cần thiết: $requiredFile"
    }
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Không tìm thấy Python trên Windows."
}

Push-Location $projectRoot
try {
    Write-Host "Đang chạy luồng chuẩn hóa CSV Bronze thành Parquet Silver..."
    Write-Host "Dùng HDFS có sẵn và chạy Spark trong Ubuntu WSL."
    & python $runtimeFile normalize
    if ($LASTEXITCODE -ne 0) {
        throw "Luồng chuẩn hóa thất bại (mã lỗi $LASTEXITCODE). Xem chi tiết ở log phía trên."
    }

    Write-Host "Đã hoàn tất luồng chuẩn hóa."
    Write-Host "Dữ liệu Parquet được lưu tại HDFS: /chicago-crimes/silver/"
}
finally {
    Pop-Location
}
