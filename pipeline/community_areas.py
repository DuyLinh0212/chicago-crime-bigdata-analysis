"""Tên 77 community area từ dịch vụ GIS chính thức của City of Chicago."""
import json
from pathlib import Path

REFERENCE = json.loads((Path(__file__).resolve().parents[1] / "data/reference/community_areas.json").read_text(encoding="utf-8"))
AREAS = REFERENCE["areas"]
if set(AREAS) != {str(number) for number in range(1, 78)} or len(set(AREAS.values())) != 77:
    raise RuntimeError("Bảng tham chiếu cần đủ 77 mã và tên khu dân cư duy nhất.")


def community_name(code):
    key = str(code).strip()
    if key not in AREAS:
        return "Không xác định"
    if AREAS[key] == "MCKINLEY PARK":
        return "McKinley Park"
    return AREAS[key].title()


def community_label(code):
    name = community_name(code)
    return name if name == "Không xác định" else f"{code} — {name}"


def add_community_names(table):
    if "khu_dan_cu" in table.columns:
        table = table.copy()
        table["ten_khu_dan_cu"] = table.khu_dan_cu.map(community_name)
        table["nhan_khu_dan_cu"] = table.khu_dan_cu.map(community_label)
    return table
