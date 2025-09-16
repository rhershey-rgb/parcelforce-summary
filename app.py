import io, re, csv, datetime as dt
from typing import Tuple
import requests
import pdfplumber
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import Response, JSONResponse

app = FastAPI(title="PF Weekly (Monday only) → CSV", version="1.0.2")

# ---------- helpers ----------
def _money(x: str) -> str:
    if not x: return "0.00"
    x = x.replace("£","").replace(",","").strip()
    try: return f"{float(x):.2f}"
    except: return "0.00"

def _date_from_we(s: str) -> dt.date:
    # Accept 14.09.25 / 14-09-2025 / 14 09 25 etc.
    s = re.sub(r"[^\d]", "/", s).strip("/")
    d, m, y = s.split("/")[:3]
    y = int(y)
    if y < 100: y += 2000
    return dt.date(int(y), int(m), int(d))

def _page1_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages: raise ValueError("No pages in PDF")
        return pdf.pages[0].extract_text() or ""

def _grab_invoice(page1: str) -> str:
    # Handles "Invoice No.*: LON2332524" etc.; strip leading symbols.
    m = re.search(r"^Invoice\s*No[^\r\n:]*:\s*([^\r\n]+)$", page1, re.I | re.M)
    if not m: return ""
    val = m.group(1).strip()
    val = re.sub(r"^[^A-Za-z0-9]+", "", val)  # drop "*: " or similar
    return val

def _grab_route(page1: str) -> str:
    """
    Extract the route number from the header table.
    We anchor on the literal 'Route No' label, then only scan a tiny window
    (same line or the next line/cell). This avoids accidental matches like
    'Collection Stop 150%'.
    """
    # 1) find the label anywhere
    m = re.search(r"Route\s*No\.?", page1, re.I)
    if m:
        # window immediately after the label (same line / next cell)
        window = page1[m.end(): m.end() + 120]
        # also include a bit before & after in case of line breaks/cell boundaries
        block = page1[max(0, m.start() - 20): m.end() + 200]
        # prefer the first 1–5 digit token near the label
        m2 = re.search(r"\b(\d{1,5})\b", window)
        if not m2:
            m2 = re.search(r"Route\s*No[^\d\r\n]{0,40}\b(\d{1,5})\b", block, re.I)
        if m2:
            return m2.group(1)

    # 2) fallback: scan only the very top of the page (header area), not the whole page
    top = page1[:1000]
    m3 = re.search(r"Route\s*No[^\d]{0,40}\b(\d{1,5})\b", top, re.I)
    if m3:
        return m3.group(1)

    return ""


def _find_week_ending(page1: str) -> dt.date:
    m = re.search(r"^Week\s*ending\s*Saturday\s*[:\-]?\s*([0-9]{1,2}[./ -][0-9]{1,2}[./ -][0-9]{2,4})",
                  page1, re.I | re.M)
    if m: return _date_from_we(m.group(1))
    m = re.search(r"Week.{0,120}?(\d{1,2})[./ -](\d{1,2})[./ -](\d{2,4})", page1, re.I | re.S)
    if not m: raise ValueError("Week ending Saturday date not found")
    return _date_from_we(f"{m.group(1)}/{m.group(2)}/{m.group(3)}")

def _monday_numbers(page1: str) -> Tuple[int,int,str]:
    """
    Many PDFs print 'Monday Tuesday' together, then:
      'Total 107 226 £281.93  Total 105 168 £263.48'
    This grabs the first triple for Monday.
    """
    txt = page1.replace("\u00a0"," ")
    pat = re.compile(
        r"Monday\s+Tuesday.*?Total\s+(\d+)\s+(\d+)\s+£?\s*([0-9]+(?:\.[0-9]{1,2})?)",
        re.I | re.S
    )
    m = pat.search(txt)
    if m:
        return int(m.group(1)), int(m.group(2)), _money(m.group(3))

    # Fallback: local region from "Monday" up to next sentinel then first "Total a b £c"
    start_m = re.search(r"\bMonday\b:?", page1, re.I)
    if start_m:
        start = start_m.start()
        end_candidates = []
        for token in ["Tuesday","Route No","Week ending","Invoice No","Internal Reference","Contract Number","Cost Centre Code"]:
            m2 = re.search(rf"\b{token}\b", page1, re.I)
            if m2 and m2.start() > start:
                end_candidates.append(m2.start())
        end = min(end_candidates) if end_candidates else len(page1)
        seg = page1[start:end]
        m3 = re.search(r"Total\s+(\d+)\s+(\d+)\s+£?\s*([0-9]+(?:\.[0-9]{1,2})?)", seg, re.I)
        if m3:
            return int(m3.group(1)), int(m3.group(2)), _money(m3.group(3))

    return 0,0,"0.00"

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

        invoice_no = _grab_invoice(page1)
        route_no   = _grab_route(page1)

        sat = _find_week_ending(page1)
        monday_date = (sat - dt.timedelta(days=5)).isoformat()

        stops, parcels, pay = _monday_numbers(page1)

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
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="monday_only.csv"'}
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
