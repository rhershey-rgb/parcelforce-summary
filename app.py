import io, re, csv, datetime as dt
from typing import Dict, List, Tuple
import requests
import pdfplumber
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import Response, JSONResponse

app = FastAPI(title="Parcelforce Weekly Summary → CSV", version="1.2.0")

# ---------- helpers ----------
WS = re.compile(r"\s+")
DAY_NAMES = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

def norm(s: str) -> str:
    return WS.sub(" ", (s or "").replace("\u00a0"," ")).strip()  # collapse NBSP too

def parse_date_like(s: str) -> dt.date:
    # Accept 14.09.25, 14/09/25, 14-09-2025 etc.
    s = (s or "").strip().replace("-", "/").replace(".", "/")
    parts = s.split("/")
    if len(parts) != 3:
        raise ValueError(f"Bad date: {s}")
    d, m, y = parts
    y = int(y)
    if y < 100:  # '25' -> 2025
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
    Only reads PAGE 1 as requested.
    """
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages:
            raise ValueError("No pages in PDF")
        raw = (pdf.pages[0].extract_text() or "")

    # Keep both raw (for positional slicing) and normalized copies
    text_raw = raw
    text = raw.replace("\u00a0"," ")
    text_norm = norm(text)

    # --- meta fields (tolerant spacing/punctuation) ---
    def grab(label_pat: str) -> str:
        # Look for "Label[:.]? value" on normalized text
        m = re.search(label_pat + r"\s*[:.]?\s*([A-Za-z0-9/_\-\s]+)", text_norm, re.I)
        return norm(m.group(1)) if m else ""

    meta = {
        "Route No": grab(r"Route\s*No"),
        "Invoice No": grab(r"Invoice\s*No"),
        "Internal Reference": grab(r"Internal\s*Reference"),
        "Contract Number": grab(r"Contract\s*Number"),
        "Cost Centre Code": grab(r"Cost\s*Centre\s*Code"),
    }

    # Week-ending Saturday date (e.g. 14.09.25 or 14/09/2025)
    m_we = re.search(
        r"Week\s*ending\s*Saturday\s*[:\-]?\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4})",
        text, re.I
    )
    if not m_we:
        # fallback: find any dd sep mm sep yy/yyyy within 120 chars of 'Week'
        m_we = re.search(
            r"Week.{0,120}?(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})", text, re.I | re.S
        )
        if not m_we:
            raise ValueError("Week ending Saturday date not found")
        d, m, y = m_we.groups()
        sat_date = parse_date_like(f"{d}/{m}/{y}")
    else:
        sat_date = parse_date_like(m_we.group(1))

    meta["WeekEnding"] = sat_date.isoformat()

    # --- Build day regions on RAW page text (so we keep natural order) ---
    # Start positions for each day label (with optional colon)
    starts: Dict[str, int] = {}
    for day in DAY_NAMES:
        m_start = re.search(rf"\b{day}\b:?", text_raw, re.I)
        if m_start:
            starts[day] = m_start.start()

    # Collect sentinel anchors to decide where each day section ends
    sentinels = []
    for token in DAY_NAMES + ["Route No", "Week ending", "Invoice No", "Internal Reference", "Contract Number", "Cost Centre Code"]:
        m = re.search(token, text_raw, re.I)
        if m:
            sentinels.append(m.start())
    sentinels = sorted(set(sentinels))

    def region_for(day: str) -> str:
        # Return the text slice for a given day, up to the next sentinel
        if day not in starts:
            m = re.search(rf"\b{day}\b:?", text_raw, re.I)
            if not m:
                return ""
            start = m.start()
        else:
            start = starts[day]
        end_candidates = [s for s in sentinels if s > start]
        end = end_candidates[0] if end_candidates else len(text_raw)
        seg = text_raw[start:end]
        # Cap to avoid runaway if anchors are sparse
        return seg[:1200]

    def parse_metrics(seg: str) -> Tuple[int,int,str]:
        """
        Extract:
          - (Total )?Stops: <int>
          - (Total )?Parcels: <int>
          - Payment: £?<num>
        Allow arbitrary punctuation/newlines between tokens; allow missing "Total".
        """
        if not seg:
            return 0, 0, "0.00"
        # Try on raw segment (captures line breaks), then on normalized
        seg_norm = norm(seg)

        # Stops
        s_m = (re.search(r"(?:Total\s+)?Stops\s*[:\-]?\s*(\d+)", seg, re.I | re.S)
               or re.search(r"(?:Total\s+)?Stops\s*[:\-]?\s*(\d+)", seg_norm, re.I))
        # Parcels
        p_m = (re.search(r"(?:Total\s+)?Parcels\s*[:\-]?\s*(\d+)", seg, re.I | re.S)
               or re.search(r"(?:Total\s+)?Parcels\s*[:\-]?\s*(\d+)", seg_norm, re.I))
        # Payment
        pay_m = (re.search(r"Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{1,2})?)", seg, re.I | re.S)
                 or re.search(r"Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{1,2})?)", seg_norm, re.I))

        stops = int(s_m.group(1)) if s_m else 0
        parcels = int(p_m.group(1)) if p_m else 0
        pay = money(pay_m.group(1)) if pay_m else "0.00"
        return stops, parcels, pay

    days: Dict[str, Tuple[int, int, str]] = {}
    for d in DAY_NAMES:
        seg = region_for(d)
        days[d] = parse_metrics(seg)

    return meta, days

def rows_for_csv(meta: Dict[str, str], days: Dict[str, Tuple[int,int,str]]) -> List[Dict[str, str]]:
    """Produce 6 rows (Mon..Sat)."""
    sat = dt.date.fromisoformat(meta["WeekEnding"])
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

def make_csv(rows: List[Dict[str, str]]) -> str:
    cols = ["Date","Route No","Total Stops","Total Parcels","Payment","Internal Reference","Contract Number","Cost Centre Code"]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()

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
        pdf_bytes = r.content
        meta, days = extract_summary_from_page1(pdf_bytes)
        rows = rows_for_csv(meta, days)
        csv_str = make_csv(rows)
        return Response(
            content=csv_str,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="weekly_summary.csv"'}
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


