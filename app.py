import io
import re
import csv
import datetime as dt
from typing import Dict, Tuple, Optional, List

import pdfplumber
import requests
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="PF Weekly Summary (All Days + Cost Centre Code)")

# -------------------- small helpers --------------------

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _page1_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        first = pdf.pages[0]
        # use layout extraction to keep ordering a bit better
        txt = first.extract_text(layout=True) or first.extract_text() or ""
    # normalize whitespace so regex across lines works
    return re.sub(r"[ \t]+", " ", txt).replace("\n", " ")

def _fmt_money(x: str) -> str:
    s = (x or "").replace(",", "").replace("£", "").strip()
    if not s:
        return "0.00"
    try:
        return f"{float(s):.2f}"
    except Exception:
        # last resort: strip non-numeric, keep dot
        s2 = re.sub(r"[^0-9.]", "", s)
        return f"{float(s2 or 0):.2f}"

def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

# -------------------- parsing of page 1 --------------------

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

def parse_summary_page1(text: str) -> Dict[str, Dict[str, str]]:
    """
    Returns a dict:
      meta: Route No, Invoice No, WeekEnding (ISO), CostCentre
      days: {day: {"stops":.., "parcels":.., "payment":..}}
    """
    out = {"meta": {}, "days": {}}

    # Route number (ensure we hit the actual label)
    m = re.search(r"Route\s*No\.?\s*[:.]?\s*([A-Za-z0-9\-_/]+)", text, re.I)
    if m:
        out["meta"]["route"] = m.group(1).strip()

    # Invoice number (accept an optional asterisk after "No")
    m = re.search(r"Invoice\s*No\.?\*?\s*[:.]?\s*([A-Za-z0-9\-_/]+)", text, re.I)
    if m:
        out["meta"]["invoice"] = m.group(1).strip()

    # Cost Centre Code (digits)
    m = re.search(r"Cost\s*Centre\s*Code\s*[:.]?\s*([A-Za-z0-9]+)", text, re.I)
    if m:
        out["meta"]["cost_centre"] = _only_digits(m.group(1))

    # Week ending Saturday dd.mm.yy / dd-mm-yy / dd/mm/yy
    m = re.search(
        r"Week\s*ending\s*Saturday\s*[:.]?\s*(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})",
        text, re.I,
    )
    if m:
        raw = m.group(1)
        # unify separators
        d, mth, y = re.split(r"[./-]", raw)
        d, mth, y = int(d), int(mth), int(y)
        if y < 100:  # 25 -> 2025 (assume 2000s)
            y += 2000
        sat = dt.date(y, mth, d)
        out["meta"]["week_ending_iso"] = sat.isoformat()
    # If not found, meta will simply miss it (we’ll error later).

    # Day blocks: "<Day> ... Total Stops: X ... Total Parcels: Y ... Payment: Z"
    # Make it tolerant about spaces and punctuation.
    for day in DAY_NAMES:
        pat = (
            rf"{day}\b.*?"
            r"Total\s*Stops\s*[:.]?\s*(\d+)\D+?"
            r"Total\s*Parcels\s*[:.]?\s*(\d+)\D+?"
            r"Payment\s*[:.]?\s*£?\s*([0-9]+(?:\.[0-9]{{1,2}})?)"
        )
        m = re.search(pat, text, re.I | re.S)
        if m:
            stops, parcels, pay = m.groups()
            out["days"][day] = {
                "stops": str(int(stops)),               # remove leading zeros
                "parcels": str(int(parcels)),
                "payment": _fmt_money(pay),
            }

    return out

def dates_from_weekending(iso_saturday: str) -> Dict[str, str]:
    sat = dt.date.fromisoformat(iso_saturday)
    # map Mon..Sat to actual dates
    return {
        "Monday":    (sat - dt.timedelta(days=5)).isoformat(),
        "Tuesday":   (sat - dt.timedelta(days=4)).isoformat(),
        "Wednesday": (sat - dt.timedelta(days=3)).isoformat(),
        "Thursday":  (sat - dt.timedelta(days=2)).isoformat(),
        "Friday":    (sat - dt.timedelta(days=1)).isoformat(),
        "Saturday":  sat.isoformat(),
    }

# -------------------- CSV building --------------------

CSV_COLUMNS = [
    "Day", "Date", "Stops", "Parcels", "Payment",
    "Invoice Number", "Route Number", "Cost Centre Code",
]

def build_rows(pdf_bytes: bytes) -> List[Dict[str, str]]:
    text = _page1_text(pdf_bytes)
    parsed = parse_summary_page1(text)

    # Validate must-have fields
    missing = []
    if not parsed["meta"].get("week_ending_iso"):
        missing.append("Week ending Saturday")
    if not parsed["meta"].get("route"):
        missing.append("Route No.")
    if not parsed["meta"].get("invoice"):
        missing.append("Invoice No.")
    if missing:
        raise ValueError(f"Missing required field(s): {', '.join(missing)}")

    dates = dates_from_weekending(parsed["meta"]["week_ending_iso"])
    route = parsed["meta"].get("route", "")
    invoice = parsed["meta"].get("invoice", "")
    cost_centre = parsed["meta"].get("cost_centre", "")

    rows: List[Dict[str, str]] = []
    for day in DAY_NAMES:
        day_data = parsed["days"].get(day, {"stops": "0", "parcels": "0", "payment": "0.00"})
        rows.append({
            "Day": day,
            "Date": dates[day],
            "Stops": day_data["stops"],
            "Parcels": day_data["parcels"],
            "Payment": day_data["payment"],
            "Invoice Number": invoice,
            "Route Number": route,
            "Cost Centre Code": cost_centre,
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


