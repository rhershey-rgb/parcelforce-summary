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

app = FastAPI(title="PF Weekly Summary — Monday + Tuesday only (stepwise)")

# -------------------- helpers --------------------

DAY_SEQUENCE = ["Monday", "Tuesday"]  # add more later as we validate

def page1_text(pdf_bytes: bytes) -> str:
    """Extract page 1 text and normalize spacing so numbers parse reliably."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        p0 = pdf.pages[0]
        txt = p0.extract_text(layout=True) or p0.extract_text() or ""
    # flatten whitespace
    txt = txt.replace("\n", " ")
    txt = re.sub(r"[ \t]+", " ", txt)

    # collapse spaces inside digit/decimal runs
    txt = re.sub(r"(?<=\d)\s+(?=\d)", "", txt)           # 1 2 3 -> 123
    txt = re.sub(r"(?<=\d)\s*\.\s*(?=\d)", ".", txt)     # 12 . 34 -> 12.34
    return txt

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def fmt_money(s: str) -> str:
    s = (s or "").strip().replace(",", "").replace("£", "")
    if not s:
        return "0.00"
    try:
        return f"{float(s):.2f}"
    except Exception:
        s2 = re.sub(r"[^0-9.]", "", s)
        return f"{float(s2 or 0):.2f}"

# -------------------- meta --------------------

def parse_meta(text: str) -> Dict[str, str]:
    meta: Dict[str, str] = {}

    m = re.search(r"Route\s*No\.?\s*[:.]?\s*([A-Za-z0-9\-_/]+)", text, re.I)
    if m: meta["route"] = m.group(1).strip()

    m = re.search(r"Invoice\s*No\.?\*?\s*[:.]?\s*([A-Za-z0-9\-_/]+)", text, re.I)
    if m: meta["invoice"] = m.group(1).strip()

    m = re.search(r"Cost\s*Centre\s*Code\s*[:.]?\s*([A-Za-z0-9]+)", text, re.I)
    if m: meta["cost_centre"] = only_digits(m.group(1))

    m = re.search(r"Week\s*ending\s*Saturday\s*[:.]?\s*(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})", text, re.I)
    if m:
        d, mth, y = re.split(r"[./-]", m.group(1))
        d, mth, y = int(d), int(mth), int(y)
        if y < 100: y += 2000
        meta["week_ending_iso"] = dt.date(y, mth, d).isoformat()

    return meta

def dates_from_weekending(iso_saturday: str) -> Dict[str, str]:
    sat = dt.date.fromisoformat(iso_saturday)
    return {
        "Monday":  (sat - dt.timedelta(days=5)).isoformat(),
        "Tuesday": (sat - dt.timedelta(days=4)).isoformat(),
    }

# -------------------- day parsing (explicit blocks) --------------------

def segment(text: str, start_label: str, end_label: str | None) -> str:
    """Return the substring starting at 'start_label' up to 'end_label' (or end)."""
    m = re.search(rf"\b{start_label}\b", text, re.I)
    if not m:
        return ""
    start = m.end()
    if end_label:
        n = re.search(rf"\b{end_label}\b", text[start:], re.I)
        if n:
            return text[start:start + n.start()]
    return text[start:]

def extract_metrics(seg: str) -> Dict[str, str]:
    """Find Stops, Parcels, Payment in the given segment (labels literal)."""
    def grab_int(label: str) -> str:
        mm = re.search(rf"{label}\s*[:.]?\s*(\d+)", seg, re.I)
        return str(int(mm.group(1))) if mm else "0"

    def grab_money(label: str) -> str:
        mm = re.search(rf"{label}\s*[:.]?\s*£?\s*([0-9]+(?:\.[0-9]{{1,2}})?)", seg, re.I)
        return fmt_money(mm.group(1)) if mm else "0.00"

    return {
        "stops":   grab_int("Stops"),
        "parcels": grab_int("Parcels"),
        "payment": grab_money("Payment"),
    }

def parse_monday(text: str) -> Dict[str, str]:
    mon_seg = segment(text, "Monday", "Tuesday")
    return extract_metrics(mon_seg)

def parse_tuesday(text: str) -> Dict[str, str]:
    tue_seg = segment(text, "Tuesday", "Wednesday")
    return extract_metrics(tue_seg)

# -------------------- CSV assembly --------------------

CSV_COLUMNS = [
    "Day", "Date", "Stops", "Parcels", "Payment",
    "Invoice Number", "Route Number", "Cost Centre Code",
]

def build_rows(pdf_bytes: bytes) -> List[Dict[str, str]]:
    text = page1_text(pdf_bytes)
    meta = parse_meta(text)
    missing = []
    if "week_ending_iso" not in meta: missing.append("Week ending Saturday")
    if "route" not in meta:           missing.append("Route No.")
    if "invoice" not in meta:         missing.append("Invoice No.")
    if missing:
        raise ValueError(f"Missing required field(s): {', '.join(missing)}")

    dates = dates_from_weekending(meta["week_ending_iso"])

    # --- Monday ---
    mon = parse_monday(text)
    row_mon = {
        "Day": "Monday",
        "Date": dates["Monday"],
        "Stops": mon["stops"],
        "Parcels": mon["parcels"],
        "Payment": mon["payment"],
        "Invoice Number": meta.get("invoice", ""),
        "Route Number": meta.get("route", ""),
        "Cost Centre Code": meta.get("cost_centre", ""),
    }

    # --- Tuesday ---
    tue = parse_tuesday(text)
    row_tue = {
        "Day": "Tuesday",
        "Date": dates["Tuesday"],
        "Stops": tue["stops"],
        "Parcels": tue["parcels"],
        "Payment": tue["payment"],
        "Invoice Number": meta.get("invoice", ""),
        "Route Number": meta.get("route", ""),
        "Cost Centre Code": meta.get("cost_centre", ""),
    }

    return [row_mon, row_tue]

def stream_csv(pdf_bytes: bytes, filename="weekly_summary_mon_tue.csv") -> StreamingResponse:
    def gen():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
        w.writeheader(); yield buf.getvalue(); buf.seek(0); buf.truncate(0)
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
        return stream_csv(pdf_bytes, "weekly_summary_mon_tue.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    try:
        pdf_bytes = await file.read()
        return stream_csv(pdf_bytes, "weekly_summary_mon_tue.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
