import io, re, csv, datetime, requests
from typing import List, Dict, Tuple
import pdfplumber
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse, PlainTextResponse
from pydantic import BaseModel

app = FastAPI(title="Parcelforce weekly summary", version="2.0.0")

# ---------- helpers ----------
DAY_NAMES = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

def money(s: str) -> str:
    if not s: return "0.00"
    s = s.replace("£","").replace(",","").strip()
    try:
        return f"{float(s):.2f}"
    except:
        # sometimes “£0” or blank – fall back to 0
        return "0.00"

def parse_week_ending(text: str) -> datetime.date | None:
    # Week ending Saturday 14-09-25  OR  14/09/2025
    m = re.search(r"Week\s*ending\s*Saturday\s*[: ]*\s*(\d{2})[-/](\d{2})[-/](\d{2,4})",
                  text, flags=re.I)
    if not m: 
        return None
    d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    y = 2000 + y if y < 100 else y
    return datetime.date(y, mth, d)

def dates_from_week_end(sat: datetime.date) -> Dict[str,str]:
    # Mon is -5 … Sat is 0
    offsets = dict(zip(DAY_NAMES, [ -5,-4,-3,-2,-1,0 ]))
    return {day: (sat + datetime.timedelta(days=offsets[day])).isoformat()
            for day in DAY_NAMES}

def grab_header(text: str) -> Dict[str,str]:
    def find(pat, default=""):
        m = re.search(pat, text, flags=re.I)
        return m.group(1).strip() if m else default
    return {
        "invoice": find(r"Invoice\s*No\.?\*?:\s*([A-Z0-9\-_/]+)"),
        "route":   find(r"Route\s*No\.?\s*[:.]?\s*([A-Za-z0-9\-_/]+)"),
        "cost":    find(r"Cost\s*Centre\s*Code\s*:\s*([0-9]+)")
    }

def grab_day_totals(text: str, day: str) -> Tuple[str, str, str]:
    """
    Return (stops, parcels, payment) for a given day panel found on THIS PAGE.
    We take the FIRST 'Total …' line after the day label to avoid picking up
    the page's weekly grand total.
    """
    # 1) Find where the day's panel starts
    mstart = re.search(rf"\b{re.escape(day)}\b", text, flags=re.I)
    if not mstart:
        return ("0", "0", "0.00")

    # 2) Scan forward from the day label; take the FIRST 'Total …' line
    tail = text[mstart.start():]

    # anchor 'Total' at line start to avoid matching narrative uses of the word
    m = re.search(r"(?mi)^[ \t]*Total[ \t]+(\d+)[ \t]+(\d+)[ \t]+£?[ \t]*([\d\.,]+)", tail)
    if not m:
        # very defensive fallback: anywhere after the label
        m = re.search(r"Total\s+(\d+)\s+(\d+)\s+£?\s*([\d\.,]+)", tail, flags=re.I)

    if m:
        stops, parcels, pay = m.group(1), m.group(2), m.group(3)
        return (stops.strip(), parcels.strip(), money(pay))

    return ("0", "0", "0.00")


def extract_first_pages(pdf_bytes: bytes) -> List[Dict[str,str]]:
    rows: List[Dict[str,str]] = []
 with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
    for page in pdf.pages:
        page_text = page.extract_text() or ""
        # Only treat this as an invoice "first page" if it has a Monday panel
        if re.search(r"\bMonday\b", page_text, re.I):
            # read header values (Route No, Invoice No, Cost Centre) from this page_text
            # then call grab_day_totals(page_text, "Monday"/"Tuesday"/... ) for each day
            

            header = grab_header(txt)
            sat = parse_week_ending(txt)
            if not sat:
                # if week-ending missing, skip this page safely
                continue
            date_map = dates_from_week_end(sat)

            for day in DAY_NAMES:
                stops, parcels, pay = grab_day_totals(txt, day)
                rows.append({
                    "Day": day,
                    "Date": date_map[day],
                    "Stops": stops,
                    "Parcels": parcels,
                    "Payment": pay,
                    "Invoice Number": header["invoice"],
                    "Route Number": header["route"],
                    "Cost Centre Code": header["cost"],
                })
    return rows

# ---------- HTTP layer ----------
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
        rows = extract_first_pages(pdf_bytes)
        if not rows:
            return PlainTextResponse("No invoice first-pages found.", status_code=422)
        return stream_csv(rows)
    except Exception as e:
        return PlainTextResponse(f"error: {e}", status_code=400)

@app.post("/process/file")
async def process_file(file: UploadFile = File(...)):
    pdf_bytes = await file.read()
    rows = extract_first_pages(pdf_bytes)
    if not rows:
        return PlainTextResponse("No invoice first-pages found.", status_code=422)
    return stream_csv(rows)

def stream_csv(rows: List[Dict[str,str]]) -> StreamingResponse:
    cols = ["Day","Date","Stops","Parcels","Payment","Invoice Number","Route Number","Cost Centre Code"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    buf.seek(0)
    return StreamingResponse(iter([buf.read()]),
                             media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="weekly-summary.csv"'})
