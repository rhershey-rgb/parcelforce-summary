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

def grab_day_totals(text: str, day: str) -> Tuple[str,str,str]:
    """
    Look inside the block that begins with e.g. 'Monday' up to the next day name,
    and read the last 'Total  <stops>  <parcels>  £<payment>' inside it.
    """
    # Slice text from this day to (next day|Totals|end)
    next_labels = DAY_NAMES[DAY_NAMES.index(day)+1:] + ["Totals"]
    next_pat = r"|".join([re.escape(lbl) for lbl in next_labels])
    mblk = re.search(
        rf"{re.escape(day)}\b(.*?)\b(?:{next_pat})",
        text, flags=re.S|re.I
    )
    block = mblk.group(1) if mblk else ""

    # Total line inside the block
    m = re.search(r"Total\s+(\d+)\s+(\d+)\s+£?\s*([\d\.,]+)", block, flags=re.I)
    if not m:
        return ("0","0","0.00")
    stops, parcels, pay = m.groups()
    return (stops.strip(), parcels.strip(), money(pay))

def extract_first_pages(pdf_bytes: bytes) -> List[Dict[str,str]]:
    rows: List[Dict[str,str]] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            txt = p.extract_text() or ""
            # Heuristic: first page of an invoice contains the day panels with "Monday"
            if "Monday" not in txt:
                continue

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


    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

