import io
import re
import csv
import datetime as dt
from typing import Dict, List, Tuple

import pdfplumber
import requests
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="PF Weekly Summary → CSV", version="1.1.0")

# ------------------------
# Helpers
# ------------------------

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

def norm(s: str) -> str:
    """Normalize whitespace and NBSP; keep it single-line friendly while
    allowing our regex to span across former line breaks."""
    if not s:
        return ""
    s = s.replace("\u00A0", " ")  # NBSP -> space
    # Keep one space for any whitespace/newline run
    s = re.sub(r"\s+", " ", s).strip()
    return s

def float_2(s: str) -> str:
    if s is None or s == "":
        return "0.00"
    s = s.replace(",", "")
    try:
        return f"{float(s):.2f}"
    except:
        return "0.00"

def parse_week_ending(text: str) -> dt.date:
    """
    Find a 'Week ending ... 14.09.25' style date robustly.
    Accepts dots/slashes/dashes/spaces, optional 'Saturday' or 'Sat',
    optional colon, and 2- or 4-digit years; tolerates newlines.
    """
    t = text.replace("\u00A0", " ")
    t = re.sub(r"[ \t]+", " ", t)

    pat = re.compile(
        r"Week\s*end(?:ing)?\s*(?:Saturday|Sat)?\s*[:\-]?\s*"
        r"(\d{1,2})[.\-\/ ](\d{1,2})[.\-\/ ](\d{2,4})",
        re.I | re.S,
    )
    m = pat.search(t)

    # Fallback: any dd sep mm sep yy/yyyy within ~120 chars of "Week"
    if not m:
        m = re.search(
            r"Week.{0,120}?(\d{1,2})[.\-\/ ](\d{1,2})[.\-\/ ](\d{2,4})",
            t, re.I | re.S
        )

    if not m:
        raise ValueError("Week ending Saturday date not found")

    d, mth, y = map(int, m.groups())
    if y < 100:
        y += 2000
    return dt.date(y, mth, d)

def extract_meta(text: str) -> Dict[str, str]:
    """
    Grab the freeform metadata fields with very tolerant patterns.
    """
    def grab(label_pat: str, capture_pat: str = r"[:\- ]\s*([\w\-\/_\.]+)"):
        # Look for <label> : value   (allow dot/slash/underscore in value)
        m = re.search(label_pat + r"\s*" + capture_pat, text, re.I)
        return m.group(1).strip() if m else ""

    return {
        "Route No":           grab(r"Route\s*No\.?"),
        "Invoice No":         grab(r"Invoice\s*No\.?"),
        "Internal Reference": grab(r"Internal\s*Reference"),
        "Contract Number":    grab(r"Contract\s*Number"),
        "Cost Centre Code":   grab(r"Cost\s*Centre\s*Code"),
    }

# One flexible pattern per weekday to grab the 3 numbers
def build_day_regex(day: str) -> re.Pattern:
    """
    Example text (with or without spaces/line breaks):
      "Tuesday Total Stops:105 Total Parcels:168 Payment:263.48"
    We allow arbitrary junk/newlines between the tokens, and optional currency sign.
    """
    pat = (
        rf"{day}\b.*?"
        r"Total\s*Stops\s*[:\-]?\s*(\d+)\D+"
        r"Total\s*Parcels\s*[:\-]?\s*(\d+)\D+"
        r"Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{{2}})?)"
    )
    return re.compile(pat, re.I | re.S)

DAY_PATTERNS = {d: build_day_regex(d) for d in DAYS}

def extract_day_triplets(text: str) -> Dict[str, Tuple[int,int,str]]:
    """
    Return { day: (stops, parcels, payment_str) }.
    If a day's block isn't found, default to zeros.
    """
    out: Dict[str, Tuple[int,int,str]] = {}
    for d in DAYS:
        m = DAY_PATTERNS[d].search(text)
        if m:
            stops = int(m.group(1))
            parcels = int(m.group(2))
            pay = float_2(m.group(3))
        else:
            stops, parcels, pay = 0, 0, "0.00"
        out[d] = (stops, parcels, pay)
    return out

def compute_dates(sat: dt.date) -> Dict[str, dt.date]:
    # Monday is Saturday - 5, Tuesday -4, ..., Saturday 0
    offsets = {"Monday": -5, "Tuesday": -4, "Wednesday": -3, "Thursday": -2, "Friday": -1, "Saturday": 0}
    return {d: sat + dt.timedelta(days=off) for d, off in offsets.items()}

def pdf_to_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages = []
        for p in pdf.pages:
            t = p.extract_text() or ""
            pages.append(t)
    return "\n".join(pages)

def make_csv(rows: List[Dict[str, str]]) -> str:
    fieldnames = ["Date","Route No","Total Stops","Total Parcels","Payment","Internal Reference","Contract Number","Cost Centre Code"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()

def parse_report_to_rows(text_raw: str) -> List[Dict[str, str]]:
    text = norm(text_raw)
    # meta first (so we can use in every line)
    meta = extract_meta(text)
    # week ending -> per-day dates
    sat = parse_week_ending(text)
    dates = compute_dates(sat)
    # day triplets
    triplets = extract_day_triplets(text)

    rows: List[Dict[str, str]] = []
    for d in DAYS:
        stops, parcels, pay = triplets[d]
        rows.append({
            "Date": dates[d].strftime("%Y-%m-%d"),
            "Route No": meta["Route No"],
            "Total Stops": str(stops),
            "Total Parcels": str(parcels),
            "Payment": pay,
            "Internal Reference": meta["Internal Reference"],
            "Contract Number": meta["Contract Number"],
            "Cost Centre Code": meta["Cost Centre Code"],
        })
    return rows

# ------------------------
# API
# ------------------------

class UrlIn(BaseModel):
    file_url: str

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/")
def root():
    return {"endpoints": ["/process/url", "/process/file", "/healthz"]}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        r = requests.get(body.file_url, timeout=60)
        r.raise_for_status()
        pdf_bytes = r.content
        text = pdf_to_text(pdf_bytes)
        rows = parse_report_to_rows(text)
        csv_str = make_csv(rows)
        return Response(
            content=csv_str,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="weekly_summary.csv"'}
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        text = pdf_to_text(pdf_bytes)
        rows = parse_report_to_rows(text)
        csv_str = make_csv(rows)
        return Response(
            content=csv_str,
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{(file.filename or "weekly")}.csv"'}
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
