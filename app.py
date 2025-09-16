import io, re, csv, datetime as dt
from typing import Dict, Tuple
import requests
import pdfplumber
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import Response, JSONResponse

app = FastAPI(title="PF Weekly (Monday only) → CSV", version="1.0.0")

# ---------- helpers ----------
def _money(x: str) -> str:
    if not x: return "0.00"
    x = x.replace("£","").replace(",","").strip()
    try: return f"{float(x):.2f}"
    except: return "0.00"

def _date_from_we(s: str) -> dt.date:
    # Accept 14.09.25 / 14-09-2025 / 14 09 25 etc.
    s = re.sub(r"[^\d]", "/", s).strip("/")       # normalize separators to '/'
    d, m, y = s.split("/")[:3]
    y = int(y)
    if y < 100: y += 2000
    return dt.date(int(y), int(m), int(d))

def _page1_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages: raise ValueError("No pages in PDF")
        return pdf.pages[0].extract_text() or ""

def _grab_line(page1: str, label_regex: str) -> str:
    # Value on the SAME LINE as the label (no bleed to next line)
    m = re.search(label_regex + r"\s*[:.]?\s*([^\r\n]+)", page1, re.I | re.M)
    return (m.group(1).strip() if m else "")

def _find_week_ending(page1: str) -> dt.date:
    m = re.search(r"^Week\s*ending\s*Saturday\s*[:\-]?\s*([0-9]{1,2}[./ -][0-9]{1,2}[./ -][0-9]{2,4})",
                  page1, re.I | re.M)
    if m: return _date_from_we(m.group(1))
    # fallback: any dd sep mm sep yy/yyyy near “Week”
    m = re.search(r"Week.{0,120}?(\d{1,2})[./ -](\d{1,2})[./ -](\d{2,4})", page1, re.I | re.S)
    if not m: raise ValueError("Week ending Saturday date not found")
    return _date_from_we(f"{m.group(1)}/{m.group(2)}/{m.group(3)}")

def _monday_region(page1: str) -> str:
    # Slice from "Monday" up to the next day/known header to avoid cross-day leakage
    start_m = re.search(r"^Monday\b:?", page1, re.I | re.M) or re.search(r"\bMonday\b:?", page1, re.I)
    if not start_m: return ""
    start = start_m.start()
    sentinels = []
    for token in ["Tuesday","Route No","Week ending","Invoice No","Internal Reference","Contract Number","Cost Centre Code"]:
        m = re.search(rf"^{token}", page1, re.I | re.M)
        if m: sentinels.append(m.start())
    end = min([s for s in sentinels if s > start], default=len(page1))
    return page1[start:end][:1200]

def _parse_monday_metrics(seg: str) -> Tuple[int,int,str]:
    if not seg: return 0,0,"0.00"
    seg_norm = re.sub(r"\s+", " ", seg.replace("\u00a0"," "))
    # “Stops:” or “Total Stops:”
    s_m = (re.search(r"(?:Total\s+)?Stops\s*[:\-]?\s*(\d+)", seg, re.I | re.S)
           or re.search(r"(?:Total\s+)?Stops\s*[:\-]?\s*(\d+)", seg_norm, re.I))
    p_m = (re.search(r"(?:Total\s+)?Parcels\s*[:\-]?\s*(\d+)", seg, re.I | re.S)
           or re.search(r"(?:Total\s+)?Parcels\s*[:\-]?\s*(\d+)", seg_norm, re.I))
    pay_m = (re.search(r"Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{1,2})?)", seg, re.I | re.S)
             or re.search(r"Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{1,2})?)", seg_norm, re.I))
    stops = int(s_m.group(1)) if s_m else 0
    parcels = int(p_m.group(1)) if p_m else 0
    pay = _money(pay_m.group(1)) if pay_m else "0.00"
    return stops, parcels, pay

# ---------- API ----------
class UrlIn(BaseModel):
    file_url: str

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        r = requests.get(body.file_url, timeout=60)
        r.raise_for_status()
        page1 = _page1_text(r.content)

        # Meta (line-anchored so it won’t swallow the next line)
        route_no    = _grab_line(page1, r"^Route\s*No")
        invoice_no  = _grab_line(page1, r"^Invoice\s*No")

        # Week ending (to derive Monday’s date)
        sat = _find_week_ending(page1)
        monday_date = (sat - dt.timedelta(days=5)).isoformat()
        # Note: if your “Week ending” is 14.09.25, Monday is 2025-09-09 (not 2026).

        # Monday slice → numbers
        mon_seg = _monday_region(page1)
        stops, parcels, pay = _parse_monday_metrics(mon_seg)

        # Build one-row CSV
        cols = ["Day","Date","Stops","Parcels","Payment","Invoice Number","Route Number"]
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=cols)
        w.writeheader()
        w.writerow({
            "Day": "Monday",
            "Date": monday_date,
            "Stops": stops,
            "Parcels": parcels,
            "Payment": pay,
            "Invoice Number": invoice_no,
            "Route Number": route_no,
        })
        csv_str = buf.getvalue()
        return Response(
            content=csv_str,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="monday_only.csv"'}
        )

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
