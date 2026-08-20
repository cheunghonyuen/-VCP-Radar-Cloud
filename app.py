import os, json, time, threading, traceback
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string, redirect, url_for

APP = Flask(__name__)
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
if not DATA_DIR.exists():
    DATA_DIR = Path(__file__).resolve().parent / "cloud_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
MARKET_DIR = DATA_DIR / "market"; MARKET_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"
RESULT_FILE = DATA_DIR / "latest_scan.csv"
TIINGO_TOKEN = os.environ.get("TIINGO_TOKEN", "").strip()
AUTO_SCAN = os.environ.get("AUTO_SCAN", "1") == "1"
MIN_GAP = float(os.environ.get("TIINGO_MIN_GAP_SECONDS", "6"))
MAX_GAP = float(os.environ.get("TIINGO_MAX_GAP_SECONDS", "90"))
AUTO_REFRESH_SECONDS = int(os.environ.get("AUTO_REFRESH_SECONDS", "5"))
current_gap = MIN_GAP
last_request_ts = 0.0
rate_lock = threading.Lock()

UNIVERSE = sorted(set("""
AAPL MSFT NVDA AVGO ORCL AMD CRM ADBE QCOM TXN INTU AMAT LRCX KLAC MU PANW CRWD NOW PLTR
AMZN TSLA HD LOW BKNG TJX SBUX MCD NKE CMG ORLY AZO ROST MAR
GOOGL META NFLX DIS T VZ TMUS CHTR
JPM BAC WFC GS MS C AXP SCHW BLK SPGI CME ICE COF USB PNC
LLY UNH JNJ ABBV MRK TMO ABT DHR AMGN GILD ISRG VRTX REGN BSX MDT SYK
WMT COST PG KO PEP PM MO CL MDLZ KMB GIS
GE CAT DE ETN PH HON RTX LMT NOC GD UPS FDX UNP CSX WM RSG UBER
XOM CVX COP EOG SLB MPC PSX VLO OXY
NEE SO DUK AEP SRE EXC
LIN APD SHW ECL FCX NEM NUE
AMT PLD EQIX WELL SPG O
""".split()))

SECTOR = {}
for sec, names in {
"XLK":"AAPL MSFT NVDA AVGO ORCL AMD CRM ADBE QCOM TXN INTU AMAT LRCX KLAC MU PANW CRWD NOW PLTR",
"XLY":"AMZN TSLA HD LOW BKNG TJX SBUX MCD NKE CMG ORLY AZO ROST MAR",
"XLC":"GOOGL META NFLX DIS T VZ TMUS CHTR",
"XLF":"JPM BAC WFC GS MS C AXP SCHW BLK SPGI CME ICE COF USB PNC",
"XLV":"LLY UNH JNJ ABBV MRK TMO ABT DHR AMGN GILD ISRG VRTX REGN BSX MDT SYK",
"XLP":"WMT COST PG KO PEP PM MO CL MDLZ KMB GIS",
"XLI":"GE CAT DE ETN PH HON RTX LMT NOC GD UPS FDX UNP CSX WM RSG UBER",
"XLE":"XOM CVX COP EOG SLB MPC PSX VLO OXY",
"XLU":"NEE SO DUK AEP SRE EXC",
"XLB":"LIN APD SHW ECL FCX NEM NUE",
"XLRE":"AMT PLD EQIX WELL SPG O"
}.items():
    for t in names.split(): SECTOR[t] = sec
ALL_SYMBOLS = sorted(set(UNIVERSE + ["SPY"] + list(set(SECTOR.values()))))

lock = threading.Lock()
scan_thread = None

def read_state():
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception: pass
    return {"status":"idle","message":"尚未开始","updated":0,"total":len(ALL_SYMBOLS),
            "last_symbol":"","last_scan":None,"error":None,"success_count":0,"fail_count":0,"gap_seconds":current_gap}

def write_state(**kw):
    s = read_state(); s.update(kw)
    STATE_FILE.write_text(json.dumps(s, ensure_ascii=False, indent=2), encoding="utf-8")

def _rate_wait():
    global last_request_ts
    with rate_lock:
        now = time.time()
        wait = max(0.0, current_gap - (now - last_request_ts))
        if wait:
            time.sleep(wait)
        last_request_ts = time.time()

def _slow_down():
    global current_gap
    current_gap = min(MAX_GAP, max(current_gap * 1.8, current_gap + 3))

def _speed_up():
    global current_gap
    current_gap = max(MIN_GAP, current_gap * 0.96)

def tiingo_history(ticker, start="2006-01-01", retries=4):
    if not TIINGO_TOKEN:
        raise RuntimeError("TIINGO_TOKEN 未设置")

    url = f"https://api.tiingo.com/tiingo/daily/{quote(ticker)}/prices?startDate={start}&resampleFreq=daily&token={quote(TIINGO_TOKEN)}"

    for attempt in range(1, retries + 1):
        _rate_wait()
        req = Request(url, headers={"Accept":"application/json","User-Agent":"VCP-Radar-Cloud/1.1"})
        try:
            with urlopen(req, timeout=45) as r:
                arr = json.loads(r.read().decode("utf-8", errors="replace"))
            if not isinstance(arr, list) or not arr:
                raise RuntimeError(f"{ticker}: Tiingo 没有返回数据")

            d = pd.DataFrame(arr)
            d["Date"] = pd.to_datetime(d["date"], errors="coerce", utc=True).dt.tz_convert(None)
            out = pd.DataFrame({
                "Date": d["Date"],
                "Open": d["adjOpen"] if "adjOpen" in d else d["open"],
                "High": d["adjHigh"] if "adjHigh" in d else d["high"],
                "Low": d["adjLow"] if "adjLow" in d else d["low"],
                "Close": d["adjClose"] if "adjClose" in d else d["close"],
                "Volume": d["adjVolume"] if "adjVolume" in d else d["volume"],
            }).dropna().set_index("Date").sort_index()
            _speed_up()
            return out

        except HTTPError as e:
            if e.code == 429:
                _slow_down()
                write_state(message=f"Tiingo 限流，自动降速至 {current_gap:.0f}s/请求")
                if attempt < retries:
                    time.sleep(min(120, current_gap))
                    continue
            raise
        except (URLError, TimeoutError):
            if attempt < retries:
                time.sleep(min(30, 3 * attempt))
                continue
            raise

def load_cached(ticker):
    p = MARKET_DIR / f"{ticker}.csv"
    if not p.exists(): return None
    try: return pd.read_csv(p, parse_dates=["Date"]).set_index("Date").sort_index()
    except Exception: return None

def update_one(ticker):
    p = MARKET_DIR / f"{ticker}.csv"
    old = load_cached(ticker)
    if old is not None and len(old):
        start = max(pd.Timestamp("2006-01-01"), old.index.max() - pd.Timedelta(days=14)).strftime("%Y-%m-%d")
        new = tiingo_history(ticker, start)
        d = pd.concat([old, new]); d = d[~d.index.duplicated(keep="last")].sort_index()
    else:
        d = tiingo_history(ticker, "2006-01-01")
    d.to_csv(p, index_label="Date")
    return d

def perf(d, n):
    if d is None or len(d) <= n: return np.nan
    return float(d.Close.iloc[-1] / d.Close.iloc[-1-n] - 1)

def band_q(v, lo, midlo, midhi, hi):
    if pd.isna(v): return .5
    if v <= lo or v >= hi: return 0.0
    if v < midlo: return (v-lo)/(midlo-lo)
    if v <= midhi: return 1.0
    return (hi-v)/(hi-midhi)

def score_symbol(t, spy):
    d = load_cached(t)
    if d is None or len(d) < 260: return None
    d = d.copy()
    d["ma50"]=d.Close.rolling(50).mean(); d["ma150"]=d.Close.rolling(150).mean()
    d["ma200"]=d.Close.rolling(200).mean(); d["v50"]=d.Volume.rolling(50).mean()
    row=d.iloc[-1]; px=float(row.Close)
    trend=bool(px > row.ma50 > row.ma150 > row.ma200)
    x=d.iloc[-60:]; wins=[x.iloc[:20],x.iloc[20:40],x.iloc[40:]]
    cs=[]
    for w in wins:
        lo=float(w.Low.min()); hi=float(w.High.max()); cs.append((hi-lo)/lo if lo>0 else np.nan)
    c1,c2,c3=cs
    pivot=float(d.High.iloc[-40:].max()); dist=px/pivot-1
    volratio=float(row.Volume/row.v50) if row.v50>0 else np.nan
    vals=[]
    for n,w in [(63,.25),(126,.35),(252,.40)]:
        a,b=perf(d,n),perf(spy,n)
        if not pd.isna(a) and not pd.isna(b): vals.append((a-b,w))
    rs=50
    if vals:
        ex=sum(v*w for v,w in vals)/sum(w for _,w in vals)
        rs=int(round(max(0,min(100,50+ex*180))))
    secscore=50; sec=SECTOR.get(t); sd=load_cached(sec) if sec else None
    if sd is not None and len(sd)>126:
        vals=[]
        for n,w in [(63,.45),(126,.55)]:
            a,b=perf(sd,n),perf(spy,n)
            if not pd.isna(a) and not pd.isna(b): vals.append((a-b,w))
        if vals:
            e=sum(v*w for v,w in vals)/sum(w for _,w in vals)
            secscore=int(round(max(0,min(100,50+e*220))))
    base=d.Volume.iloc[-50:-10].mean()
    dry=float(d.Volume.iloc[-10:].mean()/base) if base>0 else np.nan
    r12=c2/c1 if c1 else np.nan; r23=c3/c2 if c2 else np.nan
    vq=int(round(100*(.35*band_q(c3,.035,.065,.095,.115)+.20*band_q(r12,.45,.60,.82,.95)+.30*band_q(r23,.45,.68,.88,.96)+.15*band_q(dry,.45,.62,.82,1.00))))
    score=int(round(max(0,min(100,40 + secscore*.05 + rs*.25 + vq*.30))))
    strict=bool(trend and c3<.16 and c2<c1*1.08 and c3<c2*1.08 and -.10<=dist<=.025)
    status="严格VCP" if strict else ("接近买点" if trend and -.10<=dist<=.03 else "观察")
    return {"ticker":t,"score":score,"status":status,"price":px,"pivot":pivot,"pivot_dist":dist,
            "rs":rs,"sector_score":secscore,"vcp_quality":vq,"volratio":volratio,"risk7":px*.93}

def run_scan():
    global scan_thread
    success_count = 0
    fail_count = 0
    try:
        write_state(status="updating", message="正在更新 Tiingo 数据", updated=0, total=len(ALL_SYMBOLS),
                    error=None, success_count=0, fail_count=0, gap_seconds=current_gap)

        for i,t in enumerate(ALL_SYMBOLS,1):
            write_state(last_symbol=t, updated=i-1, message=f"正在更新 {t} ({i}/{len(ALL_SYMBOLS)})",
                        success_count=success_count, fail_count=fail_count, gap_seconds=current_gap)
            try:
                update_one(t)
                success_count += 1
            except Exception as e:
                fail_count += 1
                write_state(error=f"{t}: {type(e).__name__}: {str(e)[:160]}")
            write_state(updated=i, success_count=success_count, fail_count=fail_count, gap_seconds=current_gap)

        spy = load_cached("SPY")
        if spy is None or len(spy) < 260:
            raise RuntimeError("SPY 数据不足，无法评分")

        rows = []
        for t in UNIVERSE:
            r = score_symbol(t, spy)
            if r:
                rows.append(r)

        out = pd.DataFrame(rows)
        if len(out):
            out = out.sort_values(["score","rs","vcp_quality"], ascending=False)
            out.to_csv(RESULT_FILE,index=False)

        write_state(status="ready", message=f"扫描完成：{len(rows)} 只",
                    last_scan=datetime.now(timezone.utc).isoformat(), error=None,
                    success_count=success_count, fail_count=fail_count, gap_seconds=current_gap)
    except Exception as e:
        write_state(status="error", message="扫描失败", error=f"{type(e).__name__}: {e}",
                    success_count=success_count, fail_count=fail_count, gap_seconds=current_gap)
        traceback.print_exc()
    finally:
        scan_thread = None

def start_scan():
    global scan_thread
    if scan_thread is not None and scan_thread.is_alive(): return False
    scan_thread=threading.Thread(target=run_scan,daemon=True); scan_thread.start(); return True

def scheduler_loop():
    time.sleep(8)
    s=read_state()
    if AUTO_SCAN and not s.get("last_scan"): start_scan()
    last_day=None
    while True:
        now=datetime.now(timezone.utc); day=now.date().isoformat()
        if AUTO_SCAN and now.hour==22 and now.minute>=30 and last_day!=day:
            if start_scan(): last_day=day
        time.sleep(30)

HTML = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>VCP Radar Cloud V1.1</title><style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial;background:#f3f6fb;color:#17202a;margin:0}
.wrap{max-width:1150px;margin:auto;padding:22px}.top{display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap}
.card{background:white;border-radius:16px;padding:18px;margin:14px 0;box-shadow:0 4px 18px #0000000c}
h1{margin:0;font-size:30px}.muted{color:#667085}.badge{padding:7px 11px;border-radius:999px;background:#eef4ff;font-weight:700}
button{border:0;border-radius:10px;padding:11px 16px;font-weight:700;background:#111827;color:white}
table{width:100%;border-collapse:collapse;min-width:850px}th,td{padding:11px;border-bottom:1px solid #edf0f4;text-align:left}
th{font-size:12px;color:#667085}.scroll{overflow-x:auto}.score{font-size:20px;font-weight:800}.warn{color:#a35b00}
</style></head><body><div class="wrap">
<div class="top"><div><h1>VCP Radar Cloud V1.1</h1><div class="muted">Tiingo统一数据 · 128股 · V2.2冻结评分 · 自动限流</div></div>
<form method="post" action="/scan"><button>立即更新</button></form></div>
<div class="card"><div class="top"><div><b>状态：</b>{{status}}</div><div class="badge">{{updated}} / {{total}}</div></div>
<p>{{message}}</p><div class="muted">当前：{{last_symbol}} ｜ 上次完成：{{last_scan or "尚未完成"}}</div>
<p class="muted">成功：{{success_count or 0}} ｜ 失败：{{fail_count or 0}} ｜ 当前请求间隔：{{"%.0f"|format(gap_seconds or 0)}}秒</p>
{% if error %}<p class="warn">最近错误：{{error}}</p>{% endif %}</div>
<div class="card"><h2>候选排名</h2><div class="scroll"><table><tr><th>股票</th><th>评分</th><th>状态</th><th>现价</th><th>Pivot</th><th>距Pivot</th><th>RS</th><th>VCP质量</th><th>量比</th><th>7%风险线</th></tr>
{% for r in rows %}<tr><td><b>{{r.ticker}}</b></td><td class="score">{{r.score}}</td><td>{{r.status}}</td><td>{{"%.2f"|format(r.price)}}</td><td>{{"%.2f"|format(r.pivot)}}</td><td>{{"%.1f%%"|format(r.pivot_dist*100)}}</td><td>{{r.rs}}</td><td>{{r.vcp_quality}}</td><td>{{"%.2fx"|format(r.volratio)}}</td><td>{{"%.2f"|format(r.risk7)}}</td></tr>{% endfor %}
</table></div></div></div>
<script>
async function poll(){
  try{
    const r=await fetch('/api/status',{cache:'no-store'});
    const s=await r.json();
    if(s.status==='updating'){ setTimeout(()=>location.reload(), 5000); }
    else if(s.status==='ready' || s.status==='error'){ setTimeout(()=>location.reload(), 1500); }
  }catch(e){ setTimeout(poll,5000); }
}
setTimeout(poll,5000);
</script></body></html>"""

@APP.get("/")
def home():
    s=read_state(); rows=[]
    if RESULT_FILE.exists():
        try: rows=pd.read_csv(RESULT_FILE).head(40).to_dict("records")
        except Exception: rows=[]
    return render_template_string(HTML, rows=rows, **s)

@APP.post("/scan")
def scan():
    start_scan(); return redirect(url_for("home"))

@APP.get("/api/status")
def api_status(): return jsonify(read_state())

@APP.get("/health")
def health(): return jsonify({"ok":True})

threading.Thread(target=scheduler_loop, daemon=True).start()
