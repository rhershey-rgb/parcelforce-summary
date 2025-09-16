import io, re, csv, datetime as dt
from typing import Dict, List, Tuple
import requests
import pdfplumber
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI(title="Parcelforce Weekly Summary → CSV", version="1.0.0")

# ---------- helpers ----------
WS = re.compile(r"\s+")
DAY_NAMES = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

def norm(s: str) -> str:
    return WS.sub(" ", (s or "")).strip()

def parse_date_like(s: str) -> dt.date:
    """
    Accept 14.09.25, 14/09/25, 14-09-2025 etc.
    """
    s = s.strip().replace("-", "/").replace(".", "/")
    d, m, y = s.split("/")
    y = int(y)
    if y < 100:  # 25 -> 2025
        y += 2000
    return dt.date(int(y), int(m), int(d))

def money(text: str) -> str:
    if not text:
        return "0.00"
    t = text.replace("£", "").replace(",", "").strip()
    try:
        return f"{float(t):.2f}"
    except Exception:
        return "0.00"

# ---------- page-1 extraction ----------
def extract_summary_from_page1(pdf_bytes: bytes) -> Tuple[Dict[str, str], Dict[str, Tuple[int, int, str]]]:
    """
    Returns:
      meta:  Route No, Internal Reference, Contract Number, Cost Centre Code, WeekEnding (YYYY-MM-DD)
      days:  { "Monday": (stops, parcels, pay), ... }
    """
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages:
            raise ValueError("No pages in PDF")
        txt = pdf.pages[0].extract_text() or ""
    text = norm(txt)

    # --- meta fields (tolerant spacing/punctuation) ---
    def grab(label_pat: str) -> str:
        pat = re.compile(label_pat + r"\s*[:.]?\s*([A-Za-z0-9/_\- ]+)", re.I)
        m = pat.search(text)
        return norm(m.group(1)) if m else ""

    meta = {
        "Route No": grab(r"Route\s*No"),
        "Invoice No": grab(r"Invoice\s*No"),
        "Internal Reference": grab(r"Internal\s*Reference"),
        "Contract Number": grab(r"Contract\s*Number"),
        "Cost Centre Code": grab(r"Cost\s*Centre\s*Code"),
    }

    # Week-ending Saturday date (e.g. 14.09.25)
    m_we = re.search(r"Week\s*ending\s*Saturday\s*[:\-]?\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4})", text, re.I)
    if not m_we:
        raise ValueError("Week ending Saturday date not found")
    sat_date = parse_date_like(m_we.group(1))
    meta["WeekEnding"] = sat_date.isoformat()

    # --- days: flexible pattern, day … Stops … Parcels … Payment (any punctuation/spacing/newlines) ---
    days: Dict[str, Tuple[int, int, str]] = {}

    # Build a huge DOTALL regex for each day separately to avoid cross-matching
    for day in DAY_NAMES:
        pat = re.compile(
            rf"{day}\b.*?Stops\D+(\d+).*?Parcels\D+(\d+).*?Payment\D+£?\s*([0-9]+(?:\.[0-9]{{1,2}})?)",
            re.I | re.S,
        )
        m = pat.search(txt) or pat.search(text)  # try raw then normalized (some PDFs prefer one or the other)
        if m:
            stops = int(m.group(1))
            parcels = int(m.group(2))
            pay = money(m.group(3))
            days[day] = (stops, parcels, pay)

    return meta, days

def rows_for_csv(meta: Dict[str, str], days: Dict[str, Tuple[int,int,str]]) -> List[Dict[str, str]]:
    """Produce 6 rows (Mon..Sat). If a day wasn’t found, zeros are used."""
    sat = dt.date.fromisoformat(meta["WeekEnding"])
    # compute actual dates Mon..Sat from week-ending Saturday
    offsets = {"Monday": -5, "Tuesday": -4, "Wednesday": -3, "Thursday": -2, "Friday": -1, "Saturday": 0}

    rows: List[Dict[str, str]] = []
    for day in DAY_NAMES:
        date_str = (sat + dt.timedelta(days=offsets[day])).isoformat()
        stops, parcels, pay = days.get(day, (0, 0, "0.00"))
        rows.append({
            "Date": date_str,
            "Route No": meta.get("Route No", ""),
            "Total Stops": str(stops),
            "Total Parcels": str(parcels),
            "Payment": pay,
            "Internal Reference": meta.get("Internal Reference", ""),
            "Contract Number": meta.get("Contract Number", ""),
            "Cost Centre Code": meta.get("Cost Centre Code", ""),
        })
    return rows

# ---------- CSV streaming ----------
CSV_COLUMNS = ["Date","Route No","Total Stops","Total Parcels","Payment","Internal Reference","Contract Number","Cost Centre Code"]

def stream_csv(rows: List[Dict[str, str]], filename="weekly_summary.csv") -> StreamingResponse:
    def gen():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
        w.writeheader()
        yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        for r in rows:
            w.writerow(r)
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)
    return StreamingResponse(gen(), media_type="text/csv",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})

# ---------- API ----------
class UrlIn(BaseModel):
    file_url: str

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        # fetch file
        with requests.get(body.file_url, timeout=60) as r:
            r.raise_for_status()
            pdf_bytes = r.content
        meta, days = extract_summary_from_page1(pdf_bytes)
        rows = rows_for_csv(meta, days)
        return stream_csv(rows, "weekly_summary.csv")
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

