import io, re, csv, datetime as dt
from typing import Dict, List, Tuple
import requests
import pdfplumber
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import Response, JSONResponse

app = FastAPI(title="Parcelforce Weekly Summary → CSV", version="1.3.0")

DAY_NAMES = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

# ---------- small helpers ----------
def money(text: str) -> str:
    if not text:
        return "0.00"
    t = str(text).replace("£","").replace(",","").strip()
    try:
        return f"{float(t):.2f}"
    except:
        return "0.00"

def parse_date_like(s: str) -> dt.date:
    s = (s or "").strip().replace("-", "/").replace(".", "/")
    parts = s.split("/")
    if len(parts) != 3:
        raise ValueError(f"Bad date: {s}")
    d, m, y = parts
    y = int(y)
    if y < 100:
        y += 2000
    return dt.date(int(y), int(m), int(d))

# ---------- page-1 only extraction ----------
def extract_page1_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages:
            raise ValueError("No pages in PDF")
        return pdf.pages[0].extract_text() or ""

def grab_line_value(page1_text: str, label_regex: str) -> str:
    """
    Return the value on the SAME LINE as the label, up to end of line.
    Stops bleed-over into the next line.
    """
    m = re.search(label_regex + r"\s*[:.]?\s*([^\r\n]+)", page1_text, re.I | re.M)
    return m.group(1).strip() if m else ""

def extract_meta(page1_text: str) -> Dict[str,str]:
    meta = {
        "Route No":           grab_line_value(page1_text, r"^Route\s*No"),
        "Invoice No":         grab_line_value(page1_text, r"^Invoice\s*No"),
        "Internal Reference": grab_line_value(page1_text, r"^Internal\s*Reference"),
        "Contract Number":    grab_line_value(page1_text, r"^Contract\s*Number"),
        "Cost Centre Code":   grab_line_value(page1_text, r"^Cost\s*Centre\s*Code"),
    }
    # Week ending Saturday: dd[./-]mm[./-](yy|yyyy)
    m_we = re.search(
        r"^Week\s*ending\s*Saturday\s*[:\-]?\s*([0-9]{1,2}[./-][0-9]{1,2}[./-][0-9]{2,4})",
        page1_text, re.I | re.M
    )
    if not m_we:
        # Fallback: within 120 chars of "Week"
        m_we = re.search(
            r"Week.{0,120}?(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})",
            page1_text, re.I | re.S
        )
        if not m_we:
            raise ValueError("Week ending Saturday date not found")
        d, m, y = m_we.groups()
        sat = parse_date_like(f"{d}/{m}/{y}")
    else:
        sat = parse_date_like(m_we.group(1))

    meta["WeekEnding"] = sat.isoformat()
    return meta

def build_day_region_map(page1_text: str) -> Dict[str, str]:
    """
    Slice per-day region from that day's header up to the next day/known label.
    Uses raw page1 text to respect real newlines.
    """
    starts: Dict[str,int] = {}
    for day in DAY_NAMES:
        m = re.search(rf"^{day}\b:?", page1_text, re.I | re.M)
        if m:
            starts[day] = m.start()

    # Find all potential 'end' anchors
    sentinels = []
    for token in DAY_NAMES + ["Route No", "Week ending", "Invoice No", "Internal Reference", "Contract Number", "Cost Centre Code"]:
        for m in re.finditer(rf"^{token}", page1_text, re.I | re.M):
            sentinels.append(m.start())
    sentinels = sorted(set(sentinels))

    regions: Dict[str,str] = {}
    for day in DAY_NAMES:
        if day not in starts:
            # fallback: search anywhere (not anchored)
            m = re.search(rf"\b{day}\b:?", page1_text, re.I)
            if not m:
                regions[day] = ""
                continue
            start = m.start()
        else:
            start = starts[day]
        ends = [s for s in sentinels if s > start]
        end = ends[0] if ends else len(page1_text)
        regions[day] = page1_text[start:end][:1200]  # cap just in case
    return regions

def parse_metrics_from_region(seg: str) -> Tuple[int,int,str]:
    """
    Extract:
      - (Total )?Stops: <int>
      - (Total )?Parcels: <int>
      - Payment: £?<num>
    Allow arbitrary punctuation/newlines; allow missing "Total".
    """
    if not seg:
        return 0,0,"0.00"

    # Try on raw then normalized (collapse runs of whitespace)
    seg_norm = re.sub(r"\s+", " ", seg)

    s_m = (re.search(r"(?:Total\s+)?Stops\s*[:\-]?\s*(\d+)", seg, re.I | re.S)
           or re.search(r"(?:Total\s+)?Stops\s*[:\-]?\s*(\d+)", seg_norm, re.I))
    p_m = (re.search(r"(?:Total\s+)?Parcels\s*[:\-]?\s*(\d+)", seg, re.I | re.S)
           or re.search(r"(?:Total\s+)?Parcels\s*[:\-]?\s*(\d+)", seg_norm, re.I))
    pay_m = (re.search(r"Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{1,2})?)", seg, re.I | re.S)
             or re.search(r"Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{1,2})?)", seg_norm, re.I))

    stops = int(s_m.group(1)) if s_m else 0
    parcels = int(p_m.group(1)) if p_m else 0
    pay = money(pay_m.group(1)) if pay_m else "0.00"
    return stops, parcels, pay

def extract_days(page1_text: str) -> Dict[str, Tuple[int,int,str]]:
    """
    Prefer per-day region parse; if a day is still zeroed, use a global fallback.
    """
    out: Dict[str, Tuple[int,int,str]] = {}
    regions = build_day_region_map(page1_text)

    # First pass: region-based
    for day in DAY_NAMES:
        out[day] = parse_metrics_from_region(regions.get(day, ""))

    # Fallback per day if region parse failed
    text_norm = re.sub(r"\s+", " ", page1_text.replace("\u00a0"," "))
    for day in DAY_NAMES:
        if out[day] != (0,0,"0.00"):
            continue
        pat = re.compile(
            rf"{day}\b.*?(?:Total\s+)?Stops\s*[:\-]?\s*(\d+)"
            rf".*?(?:Total\s+)?Parcels\s*[:\-]?\s*(\d+)"
            rf".*?Payment\s*[:\-]?\s*£?\s*([0-9]+(?:[.,][0-9]{{1,2}})?)",
            re.I | re.S
        )
        m = pat.search(page1_text) or pat.search(text_norm)
        if m:
            out[day] = (int(m.group(1)), int(m.group(2)), money(m.group(3)))
    return out

def rows_for_csv(meta: Dict[str,str], days: Dict[str, Tuple[int,int,str]]) -> List[Dict[str,str]]:
    sat = dt.date.fromisoformat(meta["WeekEnding"])
    offsets = {"Monday": -5, "Tuesday": -4, "Wednesday": -3, "Thursday": -2, "Friday": -1, "Saturday": 0}

    rows: List[Dict[str,str]] = []
    for day in DAY_NAMES:
        date_str = (sat + dt.timedelta(days=offsets[day])).isoformat()
        stops, parcels, pay = days.get(day, (0,0,"0.00"))
        rows.append({
            "Date": date_str,
            "Route No": meta.get("Route No",""),
            "Total Stops": str(stops),
            "Total Parcels": str(parcels),
            "Payment": pay,
            "Internal Reference": meta.get("Internal Reference",""),
            "Contract Number": meta.get("Contract Number",""),
            "Cost Centre Code": meta.get("Cost Centre Code",""),
        })
    return rows

def make_csv(rows: List[Dict[str,str]]) -> str:
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
        page1 = extract_page1_text(r.content)
        meta = extract_meta(page1)
        days = extract_days(page1)
        rows = rows_for_csv(meta, days)
        csv_str = make_csv(rows)
        return Response(
            content=csv_str,
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="weekly_summary.csv"'}
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
