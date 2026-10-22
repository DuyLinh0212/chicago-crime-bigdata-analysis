$ErrorActionPreference = "Stop"

# Dùng UTF-8 cho thông báo tiếng Việt trong Windows PowerShell/Terminal.
$utf8 = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $utf8
$OutputEncoding = $utf8
if ($env:OS -eq "Windows_NT") {
    & "$env:SystemRoot\System32\chcp.com" 65001 > $null
}

$projectRoot = Split-Path -Parent $PSScriptRoot
$runtimeFile = Join-Path $projectRoot "wsl_runtime.py"

if (-not (Test-Path -LiteralPath $runtimeFile -PathType Leaf)) {
    throw "Không tìm thấy cầu nối Ubuntu WSL: $runtimeFile"
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Không tìm thấy Python trên Windows."
}

Push-Location $projectRoot
try {
    Write-Host "Đang dùng HDFS có sẵn trong Ubuntu để khởi động NameNode và 3 DataNode..."
    & python $runtimeFile start
    if ($LASTEXITCODE -ne 0) {
        throw "Không khởi động được HDFS Ubuntu (mã lỗi $LASTEXITCODE). Xem thông báo phía trên."
    }
    Write-Host "Đã sẵn sàng HDFS Ubuntu với 3 DataNode và cấu trúc thư mục."
}
finally {
    Pop-Location
}
