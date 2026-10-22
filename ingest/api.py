"""FastAPI sinh đúng mười vụ giả cho mỗi ngày lịch sử đã chốt."""
from fastapi import FastAPI, HTTPException, Query
from ingest.model import DATES, digest, events_for_day

app = FastAPI(title="API mô phỏng Chicago Crimes", description="Chỉ phục vụ thực hành CI, không phải vụ việc thật.")


@app.get("/health")
def health():
    return {"trang_thai": "san_sang", "simulation": True}


@app.get("/crimes")
def crimes(date: str = Query(description="Ngày mô phỏng YYYY-MM-DD")):
    if date not in DATES:
        raise HTTPException(status_code=422, detail="Chỉ hỗ trợ ngày 29, 30 và 31/12/2023.")
    events = events_for_day(date)
    print(f"API: đã trả {len(events)} vụ giả ngày {date}; checksum={digest(events)}", flush=True)
    return {"event_date": date, "simulation": True, "count": len(events), "events": events, "sha256": digest(events)}
