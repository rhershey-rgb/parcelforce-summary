import io
import re
import csv
import datetime as dt
from typing import Dict, Tuple

import pdfplumber
import requests
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="PF Weekly Summary (Monday row + Cost Centre Code)")

# ---------- helpers ----------

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _page1_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return pdf.pages[0].extract_text() or ""

def _grab(label_regex: str, text: str, group: int = 1, flags: int = re.I) -> str:
    """
    Return the first capture group after a label.
    Example: label_regex='Route\\s*No\\.?', will match 'Route No.' then capture the next token/value.
    """
    # Look for: <label> [: or . optional] <optional spaces> <value>
    # Value is the rest of the line (not greedy) until newline or double-space break.
    pat = re.compile(rf"{label_regex}\s*[:.]?\s*([^\n\r]+)", flags)
    m = pat.search(text)
    return _clean(m.group(group)) if m else ""

def _week_ending_date(text: str) -> dt.date | None:
    # Accept 14-09-25 or 14.09.25 or 14/09/25
    m = re.search(r"Week\s*ending\s*Saturday\s*[:.]?\s*(\d{2})[-./](\d{2})[-./](\d{2})", text, re.I)
    if not m:
        return None
    d, mth, yy = map(int, m.groups())
    # assume 20xx
    year = 2000 + yy if yy < 100 else yy
    try:
        return dt.date(year, mth, d)
    except ValueError:
        return None

def _monday_date_from_weekending(sat: dt.date) -> dt.date:
    # Monday is 5 days before Saturday
    return sat - dt.timedelta(days=5)

def _find_day_metrics(day: str, text: str) -> Tuple[int, int, float] | None:
    """
    Look on page 1 for:
      <Day> ... Total Stops: <int> ... Total Parcels: <int> ... Payment: <money>
    Be flexible with spacing and line breaks.
    """
    pat = re.compile(
        rf"{day}\b.*?"
        r"Total\s*Stops\s*[:]\s*(\d+).*?"
        r"Total\s*Parcels\s*[:]\s*(\d+).*?"
        r"Payment\s*[:£]?\s*(\d+(?:\.\d{{1,2}})?)",
        re.I | re.S
    )
    m = pat.search(text)
    if not m:
        return None
    stops = int(m.group(1))
    parcels = int(m.group(2))
    pay = float(m.group(3))
    return stops, parcels, pay

def _extract_structured_fields(text: str) -> Dict[str, str]:
    """
    Pulls the top/bottom identifiers strictly from page 1 labels.
    We anchor to the **labels** to avoid picking up stray words like
    'Collection Stop 150%'.
    """
    out: Dict[str, str] = {}

    # Route No (stick to the label cell on page 1)
    # Grab the text on the same line as 'Route No.' only.
    out["route_no"] = _grab(r"Route\s*No\.?", text)
    # Only keep the simple token at the start (avoid extra trailing commentary)
    out["route_no"] = _clean(re.split(r"\s{2,}", out["route_no"])[0])

    # Invoice No
    out["invoice_no"] = _grab(r"Invoice\s*No\.?\*?", text)

    # Cost Centre Code (bottom block on page 1)
    m = re.search(r"Cost\s*Centre\s*Code\s*[:.]?\s*([A-Za-z]?\d+)", text, re.I)
    out["cost_centre"] = _clean(m.group(1)) if m else ""

    return out

# ---------- CSV builders ----------

def _csv_from_bytes(pdf_bytes: bytes) -> StreamingResponse:
    """
    Build a single-row CSV for **Monday** with:
    Day,Date,Stops,Parcels,Payment,Invoice Number,Route Number,Cost Centre Code
    """
    text = _page1_text(pdf_bytes)
    if not text:
        return JSONResponse(status_code=400, content={"error": "No text found on page 1"})

    # Required identifiers
    ids = _extract_structured_fields(text)
    route_no = ids.get("route_no", "")
    invoice_no = ids.get("invoice_no", "")
    cost_centre = ids.get("cost_centre", "")

    # Week-ending → Monday date
    sat = _week_ending_date(text)
    if not sat:
        return JSONResponse(status_code=400, content={"error": "Week ending Saturday date not found"})
    monday = _monday_date_from_weekending(sat)

    # Monday metrics
    metrics = _find_day_metrics("Monday", text)
    if not metrics:
        # fall back to zeros if the block isn't present
        stops, parcels, pay = 0, 0, 0.0
    else:
        stops, parcels, pay = metrics

    # Compose CSV in memory
    def gen():
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["Day", "Date", "Stops", "Parcels", "Payment", "Invoice Number", "Route Number", "Cost Centre Code"])
        w.writerow([
            "Monday",
            monday.strftime("%Y-%m-%d"),
            stops,
            parcels,
            f"{pay:.2f}",
            invoice_no,
            route_no,
            cost_centre
        ])
        yield buf.getvalue()

    return StreamingResponse(
        gen(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="monday_summary.csv"'}
    )

# ---------- API ----------

class UrlIn(BaseModel):
    file_url: str

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        r = requests.get(body.file_url, timeout=60)
        r.raise_for_status()
        pdf_bytes = r.content
        return _csv_from_bytes(pdf_bytes)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        return _csv_from_bytes(pdf_bytes)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

