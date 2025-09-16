import io, re, csv, datetime as dt
from typing import List, Tuple
import requests
import pdfplumber
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="PF Weekly Summary → CSV", version="0.3.0")

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

class UrlIn(BaseModel):
    file_url: str

# ---------- helpers ----------

def first_page_text(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        return pdf.pages[0].extract_text() or ""

def grab_meta(text: str) -> dict:
    def find(pat, default=""):
        m = re.search(pat, text, flags=re.I)
        return (m.group(1).strip() if m else default)

    # Week ending Saturday e.g. 14-09-25 or 14/09/2025
    wek = find(r"Week\s*ending\s*Saturday\s*([0-9]{1,2}[-/][0-9]{1,2}[-/][0-9]{2,4})")
    sat = None
    if wek:
        dd, mm, yy = re.split(r"[-/]", wek)
        yy = int(yy)
        # two-digit → 2000s
        if yy < 100:
            yy += 2000
        sat = dt.date(yy, int(mm), int(dd))

    invoice = find(r"Invoice\s*No\.?\*?:?\s*([A-Za-z0-9\-]+)")
    route   = find(r"Route\s*No\.?\s*[:.]?\s*([A-Za-z0-9\-_/]+)")
    cost    = find(r"Cost\s*Centre\s*Code.*?([0-9]{4,})")

    return {"sat": sat, "invoice": invoice, "route": route, "cost": cost}

def monday_to_saturday_dates(saturday: dt.date) -> List[dt.date]:
    # Monday..Saturday relative to the Saturday week-ending date
    return [
        saturday - dt.timedelta(days=5),
        saturday - dt.timedelta(days=4),
        saturday - dt.timedelta(days=3),
        saturday - dt.timedelta(days=2),
        saturday - dt.timedelta(days=1),
        saturday,
    ]

def first_six_totals(text: str) -> List[Tuple[int,int,float]]:
    """
    Find the first six occurrences of:
      'Total <stops> <parcels> £<amount>'
    in page 1 text order. These correspond to Monday..Saturday.
    """
    totals = []
    for m in re.finditer(r"Total\s+(\d+)\s+(\d+)\s+£\s?(\d+(?:\.\d{2})?)", text, flags=re.I):
        stops = int(m.group(1))
        parcels = int(m.group(2))
        pay = float(m.group(3))
        totals.append((stops, parcels, pay))
        if len(totals) == 6:
            break
    return totals

def make_csv_for_days(meta: dict, totals: List[Tuple[int,int,float]], days_wanted: int = 6) -> StreamingResponse:
    """
    Build CSV for the first `days_wanted` days (1=Monday only, 2=Mon+Tue, etc.)
    """
    if not meta["sat"]:
        return JSONResponse(status_code=400, content={"error": "Week ending Saturday date not found"})

    dates = monday_to_saturday_dates(meta["sat"])
    rows = []
    # pad totals if PDF had fewer than needed
    while len(totals) < 6:
        totals.append((0,0,0.0))

    for i in range(days_wanted):
        day = DAY_NAMES[i]
        d = dates[i]
        s, p, pay = totals[i]
        rows.append({
            "Day": day,
            "Date": d.isoformat(),
            "Stops": s,
            "Parcels": p,
            "Payment": f"{pay:.2f}",
            "Invoice Number": meta["invoice"],
            "Route Number": meta["route"],
            "Cost Centre Code": meta["cost"],
        })

    def gen():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=[
            "Day","Date","Stops","Parcels","Payment",
            "Invoice Number","Route Number","Cost Centre Code"
        ])
        w.writeheader()
        yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        for r in rows:
            w.writerow(r)
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    return StreamingResponse(
        gen(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="summary.csv"'}
    )

# ---------- endpoints ----------

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/process/url")
def process_url(body: UrlIn):
    try:
        r = requests.get(body.file_url, timeout=60)
        r.raise_for_status()
        text = first_page_text(r.content)
        meta = grab_meta(text)
        totals = first_six_totals(text)

        # For now: return Monday + Tuesday only (days_wanted=2).
        # When you’re ready to extend, change to 6 for the full week.
        return make_csv_for_days(meta, totals, days_wanted=2)

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
