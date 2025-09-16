import io
import csv
import re
import datetime as dt
from typing import Dict, List, Iterable

import requests
import pdfplumber
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="Parcelforce Weekly Summary → CSV", version="1.0.2")

# ---------- Models ----------
class UrlIn(BaseModel):
    file_url: str

# ---------- Helpers ----------
DAY_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

DAY_TO_OFFSET = {
    "Monday": 5,
    "Tuesday": 4,
    "Wednesday": 3,
    "Thursday": 2,
    "Friday": 1,
    "Saturday": 0,
}

HEADERS = [
    "Date",
    "Route No",
    "Total Stops",
    "Total Parcels",
    "Payment",
    "Internal Reference",
    "Contract Number",
    "Cost Centre Code",
]

def parse_week_ending(text: str) -> dt.date:
    """
    Find 'Week ending Saturday:14.09.25' -> datetime.date(2025,9,14)
    """
    m = re.search(r"Week\s+ending\s+Saturday\s*:\s*(\d{2})\.(\d{2})\.(\d{2})", text, re.I)
    if not m:
        raise ValueError("Week ending Saturday date not found")
    d, mth, y2 = map(int, m.groups())
    year = 2000 + y2  # assume 20xx
    return dt.date(year, mth, d)

def grab_field(text: str, label_pattern: str) -> str:
    """
    Grab a single token or token-ish after a label (e.g., 'Route No', 'Invoice No', etc).
    """
    # Try a tolerant “value on same line” pattern
    m = re.search(
        rf"{label_pattern}\s*[:.]?\s*([^\s\r\n]+.*?)(?:\r?\n|$)", text, re.I
    )
    return (m.group(1).strip() if m else "")

def parse_totals(text: str) -> List[Dict[str, str]]:
    """
    Extract per-day totals like:
    Monday Total Stops: 107 Total Parcels: 226 Payment:281.93
    """
    rows: List[Dict[str, str]] = []
    day_re = re.compile(
        r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\s+"
        r"Total\s+Stops:\s*(\d+)\s+"
        r"Total\s+Parcels:\s*(\d+)\s+"
        r"Payment:\s*([0-9]+(?:\.[0-9]{1,2})?)",
        re.I,
    )
    for m in day_re.finditer(text):
        day, stops, parcels, pay = m.groups()
        rows.append({
            "day": day.capitalize(),
            "stops": stops,
            "parcels": parcels,
            "payment": f"{float(pay):.2f}",
        })
    return rows

def date_for_day(week_ending_sat: dt.date, day: str) -> str:
    """
    Map Monday..Saturday to dates based on the week ending Saturday.
    Return dd/mm/YYYY for consistency with your other CSVs.
    """
    offset = DAY_TO_OFFSET.get(day, 0)
    d = week_ending_sat - dt.timedelta(days=offset)
    return d.strftime("%d/%m/%Y")

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return "\n".join((p.extract_text() or "") for p in pdf.pages)

def build_csv_stream(rows: List[Dict[str, str]], filename="weekly_summary.csv") -> StreamingResponse:
    def gen():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow(r)
        yield buf.getvalue()
    # Ensure correct headers for filename + CSV mime
    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )

# ---------- Endpoints ----------
@app.get("/healthz")
def healthz():
    return {"status": "healthy"}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        # Download the PDF
        r = requests.get(body.file_url, stream=True, timeout=60)
        r.raise_for_status()
        pdf_bytes = r.content

        text = extract_text_from_pdf(pdf_bytes)

        # Header fields
        route_no          = grab_field(text,   r"Route\s*No")
        invoice_no        = grab_field(text,   r"Invoice\s*No")
        internal_ref      = grab_field(text,   r"Internal\s*Reference")
        contract_number   = grab_field(text,   r"Contract\s*Number")
        cost_centre_code  = grab_field(text,   r"Cost\s*Centre\s*Code")

        week_end = parse_week_ending(text)
        totals = parse_totals(text)

        # Build CSV rows
        out_rows = []
        for t in totals:
            out_rows.append({
                "Date": date_for_day(week_end, t["day"]),
                "Route No": route_no,
                "Total Stops": t["stops"],
                "Total Parcels": t["parcels"],
                "Payment": t["payment"],
                "Internal Reference": internal_ref,
                "Contract Number": contract_number,
                "Cost Centre Code": cost_centre_code,
            })

        return build_csv_stream(out_rows, filename="weekly_summary.csv")

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    """
    Optional: upload the PDF directly (handy for local tests).
    """
    try:
        pdf_bytes = await file.read()
        text = extract_text_from_pdf(pdf_bytes)

        route_no          = grab_field(text,   r"Route\s*No")
        invoice_no        = grab_field(text,   r"Invoice\s*No")
        internal_ref      = grab_field(text,   r"Internal\s*Reference")
        contract_number   = grab_field(text,   r"Contract\s*Number")
        cost_centre_code  = grab_field(text,   r"Cost\s*Centre\s*Code")

        week_end = parse_week_ending(text)
        totals = parse_totals(text)

        out_rows = []
        for t in totals:
            out_rows.append({
                "Date": date_for_day(week_end, t["day"]),
                "Route No": route_no,
                "Total Stops": t["stops"],
                "Total Parcels": t["parcels"],
                "Payment": t["payment"],
                "Internal Reference": internal_ref,
                "Contract Number": contract_number,
                "Cost Centre Code": cost_centre_code,
            })

        safe_name = (file.filename or "weekly").rsplit(".", 1)[0] + ".csv"
        return build_csv_stream(out_rows, filename=safe_name)

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
