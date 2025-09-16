import io
import re
import csv
import datetime as dt
from typing import Dict, List

import pdfplumber
import requests
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="PF Weekly Summary (All Days + Cost Centre Code)")

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

# -------------------- helpers --------------------

def _page1_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        p0 = pdf.pages[0]
        txt = p0.extract_text(layout=True) or p0.extract_text() or ""
    # flatten whitespace and join lines
    txt = txt.replace("\n", " ")
    txt = re.sub(r"[ \t]+", " ", txt)

    # CRITICAL: collapse spaces between digits (PDF kerning issue)
    #  "2 8 1 . 9 3" -> "281.93"
    txt = re.sub(r"(?<=\d)\s+(?=\d)", "", txt)          # 1 2 3 -> 123
    txt = re.sub(r"(?<=\d)\s*\.\s*(?=\d)", ".", txt)    # 123 . 45 -> 123.45
    return txt

def _fmt_money(x: str) -> str:
    s = (x or "").replace(",", "").replace("£", "").strip()
    if not s:
        return "0.00"
    try:
        return f"{float(s):.2f}"
    except Exception:
        s2 = re.sub(r"[^0-9.]", "", s)
        return f"{float(s2 or 0):.2f}"

def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

# -------------------- meta parsing --------------------

def parse_meta(text: str) -> Dict[str, str]:
    meta: Dict[str, str] = {}

    m = re.search(r"Route\s*No\.?\s*[:.]?\s*([A-Za-z0-9\-_/]+)", text, re.I)
    if m:
        meta["route"] = m.group(1).strip()

    m = re.search(r"Invoice\s*No\.?\*?\s*[:.]?\s*([A-Za-z0-9\-_/]+)", text, re.I)
    if m:
        meta["invoice"] = m.group(1).strip()

    m = re.search(r"Cost\s*Centre\s*Code\s*[:.]?\s*([A-Za-z0-9]+)", text, re.I)
    if m:
        meta["cost_centre"] = _only_digits(m.group(1))

    m = re.search(
        r"Week\s*ending\s*Saturday\s*[:.]?\s*(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})",
        text, re.I,
    )
    if m:
        raw = m.group(1)
        d, mth, y = re.split(r"[./-]", raw)
        d, mth, y = int(d), int(mth), int(y)
        if y < 100:
            y += 2000
        meta["week_ending_iso"] = dt.date(y, mth, d).isoformat()

    return meta

def dates_from_weekending(iso_saturday: str) -> Dict[str, str]:
    sat = dt.date.fromisoformat(iso_saturday)
    return {
        "Monday":    (sat - dt.timedelta(days=5)).isoformat(),
        "Tuesday":   (sat - dt.timedelta(days=4)).isoformat(),
        "Wednesday": (sat - dt.timedelta(days=3)).isoformat(),
        "Thursday":  (sat - dt.timedelta(days=2)).isoformat(),
        "Friday":    (sat - dt.timedelta(days=1)).isoformat(),
        "Saturday":  sat.isoformat(),
    }

# -------------------- day blocks --------------------

def _segment_for_day(text: str, day: str) -> str:
    # find this day's start
    m = re.search(rf"\b{day}\b", text, re.I)
    if not m:
        return ""
    start = m.end()

    # find the nearest next day label
    next_pos = len(text)
    for other in DAY_NAMES:
        if other == day:
            continue
        mm = re.search(rf"\b{other}\b", text[start:], re.I)
        if mm:
            pos = start + mm.start()
            if pos < next_pos:
                next_pos = pos
    return text[start:next_pos]

def extract_day_numbers(segment: str) -> Dict[str, str]:
    # Very tolerant patterns: “Total Stops” or “Stops”, punctuation optional
    pat = (
        r"(?:Total\s*)?Stops\s*[:.]?\s*(\d+).*?"
        r"(?:Total\s*)?Parcels\s*[:.]?\s*(\d+).*?"
        r"Payment\s*[:.]?\s*£?\s*([0-9]+(?:\.[0-9]{1,2})?)"
    )
    m = re.search(pat, segment, re.I | re.S)
    if m:
        stops, parcels, pay = m.groups()
        return {
            "stops": str(int(stops)),
            "parcels": str(int(parcels)),
            "payment": _fmt_money(pay),
        }

    # Fallback: find each metric separately inside the segment
    def grab(label_pat: str, money: bool = False, default: str = "0") -> str:
        mm = re.search(label_pat, segment, re.I)
        if not mm:
            return "0.00" if money else default
        val = mm.group(1)
        return _fmt_money(val) if money else str(int(val))

    stops = grab(r"(?:Total\s*)?Stops\s*[:.]?\s*(\d+)")
    parcels = grab(r"(?:Total\s*)?Parcels\s*[:.]?\s*(\d+)")
    payment = grab(r"Payment\s*[:.]?\s*£?\s*([0-9]+(?:\.[0-9]{1,2})?)", money=True)
    return {"stops": stops, "parcels": parcels, "payment": payment}

def parse_days(text: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for day in DAY_NAMES:
        seg = _segment_for_day(text, day)
        if not seg:
            continue
        out[day] = extract_day_numbers(seg)
    return out

# -------------------- CSV rows --------------------

CSV_COLUMNS = [
    "Day", "Date", "Stops", "Parcels", "Payment",
    "Invoice Number", "Route Number", "Cost Centre Code",
]

def build_rows(pdf_bytes: bytes) -> List[Dict[str, str]]:
    text = _page1_text(pdf_bytes)
    meta = parse_meta(text)

    missing = []
    if "week_ending_iso" not in meta: missing.append("Week ending Saturday")
    if "route" not in meta:           missing.append("Route No.")
    if "invoice" not in meta:         missing.append("Invoice No.")
    if missing:
        raise ValueError(f"Missing required field(s): {', '.join(missing)}")

    dates = dates_from_weekending(meta["week_ending_iso"])
    days = parse_days(text)

    rows: List[Dict[str, str]] = []
    for day in DAY_NAMES:
        d = days.get(day, {"stops": "0", "parcels": "0", "payment": "0.00"})
        rows.append({
            "Day": day,
            "Date": dates[day],
            "Stops": d["stops"],
            "Parcels": d["parcels"],
            "Payment": d["payment"],
            "Invoice Number": meta.get("invoice", ""),
            "Route Number": meta.get("route", ""),
            "Cost Centre Code": meta.get("cost_centre", ""),
        })
    return rows

def stream_csv(pdf_bytes: bytes, filename="weekly_summary.csv") -> StreamingResponse:
    def gen():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
        w.writeheader()
        yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        for r in build_rows(pdf_bytes):
            w.writerow(r)
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)
    return StreamingResponse(gen(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})

# -------------------- API --------------------

class UrlIn(BaseModel):
    file_url: str

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        with requests.get(body.file_url, timeout=60) as r:
            r.raise_for_status()
            pdf_bytes = r.content
        return stream_csv(pdf_bytes, "weekly_summary.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        return stream_csv(pdf_bytes, "weekly_summary.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
