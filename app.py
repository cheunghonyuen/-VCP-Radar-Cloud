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
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    DATA_DIR = Path(__file__).resolve().parent / "cloud_data"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
MARKET_DIR = DATA_DIR / "market"; MARKET_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = DATA_DIR / "state.json"
RESULT_FILE = DATA_DIR / "latest_scan.csv"
CHECKPOINT_FILE = DATA_DIR / "scan_checkpoint.json"
BACKTEST_DIR = DATA_DIR / "jlaw_backtest_results_v2"
BACKTEST_STATUS_FILE = BACKTEST_DIR / "status.json"
V3_DIR = DATA_DIR / "jlaw_v3_phase1"
V3_STATUS_FILE = V3_DIR / "status.json"
V3_STOP_FILE = V3_DIR / "STOP"
TIINGO_TOKEN = os.environ.get("TIINGO_TOKEN", "").strip()
AUTO_SCAN = os.environ.get("AUTO_SCAN", "1") == "1"
MIN_GAP = float(os.environ.get("TIINGO_MIN_GAP_SECONDS", "6"))
MAX_GAP = float(os.environ.get("TIINGO_MAX_GAP_SECONDS", "18"))
COOLDOWN_429 = int(os.environ.get("TIINGO_429_COOLDOWN_SECONDS", "60"))
AUTO_REFRESH_SECONDS = int(os.environ.get("AUTO_REFRESH_SECONDS", "5"))
current_gap = MIN_GAP
last_request_ts = 0.0
rate_lock = threading.Lock()

class TiingoRateLimitPause(Exception):
    """Stop the current scan immediately on Tiingo HTTP 429; checkpoint is kept for resume."""
    pass

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
SECTOR_NAME = {
"XLK":"科技","XLY":"可选消费","XLC":"通信","XLF":"金融","XLV":"医疗",
"XLP":"必需消费","XLI":"工业","XLE":"能源","XLU":"公用事业","XLB":"材料","XLRE":"房地产"
}
TOP_N = int(os.environ.get("TOP_N", "40"))
ALL_SYMBOLS = sorted(set(UNIVERSE + ["SPY"] + list(set(SECTOR.values()))))

lock = threading.Lock()
scan_thread = None
backtest_thread = None
v3_thread = None
v3_stop_event = threading.Event()

def storage_info():
    try:
        test = DATA_DIR / ".vcp_write_test"
        test.write_text("ok", encoding="utf-8")
        ok = test.exists()
        try: test.unlink()
        except Exception: pass
        return {"data_dir": str(DATA_DIR), "writable": bool(ok)}
    except Exception:
        return {"data_dir": str(DATA_DIR), "writable": False}

def read_state():
    s = {}
    if STATE_FILE.exists():
        try: s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception: s = {}
    defaults = {"status":"idle","message":"尚未开始","updated":0,"total":len(ALL_SYMBOLS),
                "last_symbol":"","last_scan":None,"error":None,
                "success_count":0,"fail_count":0,"gap_seconds":current_gap,
                "market_label":"UNKNOWN","market_text":"尚未判定","market_emoji":"⚪"}
    defaults.update(s)
    return defaults

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
        req = Request(url, headers={"Accept":"application/json","User-Agent":"VCP-Radar-Cloud/1.2"})
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
        except HTTPError as ex:
            if ex.code == 429:
                _slow_down()
                # Do not keep hammering Tiingo. Stop this scan immediately and let
                # the next scan resume from the persistent checkpoint.
                raise TiingoRateLimitPause(
                    f"{ticker}: Tiingo HTTP 429；本轮已自动暂停，进度已保存；当前间隔 {current_gap:.0f}s"
                ) from ex
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
    """
    Incremental mode:
    - First run: full history download from 2006-01-01
    - Later runs: request only a tiny recent window, then merge into local /data cache
    """
    p = MARKET_DIR / f"{ticker}.csv"
    old = load_cached(ticker)

    if old is not None and len(old):
        last_dt = pd.Timestamp(old.index.max()).normalize()
        # Only ask Tiingo for a short recent window. This keeps payload tiny and avoids re-downloading history.
        # 7 calendar days covers weekends/holidays; duplicate rows are replaced on merge.
        start_dt = max(pd.Timestamp("2006-01-01"), last_dt - pd.Timedelta(days=7))
        new = tiingo_history(ticker, start_dt.strftime("%Y-%m-%d"))
        d = pd.concat([old, new])
        d = d[~d.index.duplicated(keep="last")].sort_index()
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

def cache_summary():
    total = 0
    cached = 0
    latest = None
    for t in ALL_SYMBOLS:
        total += 1
        d = load_cached(t)
        if d is not None and len(d):
            cached += 1
            dt = pd.Timestamp(d.index.max())
            if latest is None or dt > latest:
                latest = dt
    return {
        "cached_count": cached,
        "cache_total": total,
        "cache_latest": latest.strftime("%Y-%m-%d") if latest is not None else "无"
    }

def market_regime(spy):
    if spy is None or len(spy) < 200:
        return {"label":"UNKNOWN","emoji":"⚪","text":"数据不足"}
    ma50 = spy.Close.rolling(50).mean().iloc[-1]
    ma200 = spy.Close.rolling(200).mean().iloc[-1]
    px = float(spy.Close.iloc[-1])
    if px > ma50 > ma200:
        return {"label":"BULL","emoji":"🟢","text":"偏多"}
    if px < ma50 < ma200:
        return {"label":"BEAR","emoji":"🔴","text":"偏空"}
    return {"label":"SIDEWAYS","emoji":"🟡","text":"震荡"}

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
    status="严格VCP" if strict else ("突破候选" if (dist>0 and dist<=.05 and volratio>=1.2) else ("接近买点" if trend and -.10<=dist<=.03 else "观察"))
    contraction_count = sum([
        1 if (not pd.isna(c1) and c1 < 0.30) else 0,
        1 if (not pd.isna(c2) and c2 < 0.22) else 0,
        1 if (not pd.isna(c3) and c3 < 0.16) else 0,
    ])
    volume_dry = bool(not pd.isna(dry) and dry < 0.85)
    near_pivot = bool(-0.10 <= dist <= 0.03)
    breakout = bool(dist > 0 and dist <= 0.05 and volratio >= 1.2)

    reasons = []
    if trend: reasons.append("长期趋势结构通过")
    if contraction_count >= 2: reasons.append(f"收缩结构 {contraction_count}/3")
    if volume_dry: reasons.append("成交量收缩")
    if rs >= 70: reasons.append(f"RS较强 {rs}")
    if near_pivot: reasons.append("接近Pivot")
    if breakout: reasons.append("放量突破")
    if not reasons: reasons.append("结构仍在观察")

    return {"ticker":t,"sector":SECTOR_NAME.get(sec, sec or "其他"),"score":score,"status":status,"price":px,"pivot":pivot,"pivot_dist":dist,
            "rs":rs,"sector_score":secscore,"vcp_quality":vq,"volratio":volratio,"risk7":px*.93,
            "trend_ok":trend,"contraction_count":contraction_count,
            "c1":c1,"c2":c2,"c3":c3,"dry_ratio":dry,
            "volume_dry":volume_dry,"near_pivot":near_pivot,"breakout":breakout,
            "reason":"；".join(reasons)}

def read_checkpoint():
    if not CHECKPOINT_FILE.exists():
        return {"done":[], "failed":[]}
    try:
        x=json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        return {"done":list(x.get("done",[])), "failed":list(x.get("failed",[]))}
    except Exception:
        return {"done":[], "failed":[]}

def save_checkpoint(done, failed):
    CHECKPOINT_FILE.write_text(json.dumps({"done":sorted(set(done)),
        "failed":sorted(set(failed)),"saved_at":datetime.now(timezone.utc).isoformat()},
        ensure_ascii=False, indent=2), encoding="utf-8")

def clear_checkpoint():
    try:
        CHECKPOINT_FILE.unlink(missing_ok=True)
    except Exception:
        pass

def run_scan():
    global scan_thread
    cp = read_checkpoint()
    done = set(cp.get("done", []))
    failed = set(cp.get("failed", []))
    # If prior run was complete, begin a fresh cycle.
    if len(done) >= len(ALL_SYMBOLS):
        done.clear(); failed.clear(); clear_checkpoint()

    success_count = len(done)
    fail_count = len(failed)
    remaining = [t for t in ALL_SYMBOLS if t not in done]
    try:
        write_state(status="updating",
                    message=f"增量更新/断点续扫：已完成 {len(done)}，剩余 {len(remaining)}",
                    updated=len(done), total=len(ALL_SYMBOLS), error=None,
                    success_count=success_count, fail_count=fail_count, gap_seconds=current_gap)

        # First pass: continue only unfinished symbols.
        for t in remaining:
            i = len(done) + 1
            write_state(last_symbol=t, updated=len(done),
                        message=f"增量更新 {t}（已完成 {len(done)}/{len(ALL_SYMBOLS)}）",
                        success_count=success_count, fail_count=fail_count, gap_seconds=current_gap)
            try:
                update_one(t)
                done.add(t)
                failed.discard(t)
                success_count = len(done)
            except TiingoRateLimitPause as ex:
                # Current symbol stays unfinished; do not mark it as a data failure.
                save_checkpoint(done, failed)
                write_state(status="paused",
                            message=f"Tiingo 限流：本轮自动暂停；已保存 {len(done)}/{len(ALL_SYMBOLS)}，稍后可继续",
                            error=str(ex), updated=len(done), success_count=len(done),
                            fail_count=len(failed), gap_seconds=current_gap)
                return
            except Exception as ex:
                failed.add(t)
                fail_count = len(failed)
                write_state(error=f"{t}: {type(ex).__name__}: {str(ex)[:160]}")
            save_checkpoint(done, failed)
            write_state(updated=len(done), success_count=success_count,
                        fail_count=len(failed), gap_seconds=current_gap)

        # Retry failures once at the end instead of blocking the whole scan.
        retry_list = [t for t in list(failed) if t not in done]
        if retry_list:
            write_state(message=f"主扫描完成，正在重试失败股票：{len(retry_list)}只")
            for t in retry_list:
                try:
                    update_one(t)
                    done.add(t)
                    failed.discard(t)
                except TiingoRateLimitPause as ex:
                    save_checkpoint(done, failed)
                    write_state(status="paused",
                                message=f"Tiingo 限流：本轮自动暂停；已保存 {len(done)}/{len(ALL_SYMBOLS)}，稍后可继续",
                                error=str(ex), updated=len(done), success_count=len(done),
                                fail_count=len(failed), gap_seconds=current_gap)
                    return
                except Exception as ex:
                    write_state(error=f"{t}: 重试仍失败: {type(ex).__name__}: {str(ex)[:140]}")
                save_checkpoint(done, failed)
                write_state(updated=len(done), success_count=len(done),
                            fail_count=len(failed), gap_seconds=current_gap)

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
            out.to_csv(RESULT_FILE, index=False)

        regime = market_regime(spy)
        status = "ready" if not failed else "ready"
        msg = f"扫描完成：{len(rows)}只；失败 {len(failed)}只" if failed else f"扫描完成：{len(rows)}只"
        write_state(status=status, message=msg, updated=len(done), total=len(ALL_SYMBOLS),
                    last_scan=datetime.now(timezone.utc).isoformat(), error=None if not failed else f"仍有失败：{', '.join(sorted(failed))}",
                    success_count=len(done), fail_count=len(failed), gap_seconds=current_gap,
                    market_label=regime["label"], market_text=regime["text"], market_emoji=regime["emoji"])
        if not failed:
            clear_checkpoint()
    except Exception as ex:
        save_checkpoint(done, failed)
        write_state(status="error", message="扫描暂停，进度已保存",
                    error=f"{type(ex).__name__}: {ex}", updated=len(done),
                    success_count=len(done), fail_count=len(failed), gap_seconds=current_gap)
        traceback.print_exc()
    finally:
        scan_thread = None

def start_scan():
    global scan_thread
    if scan_thread is not None and scan_thread.is_alive(): return False
    scan_thread=threading.Thread(target=run_scan,daemon=True); scan_thread.start(); return True


JLAW_EXCLUDED = {"NEE","SO","DUK","AEP","SRE","EXC"}

def read_backtest_status():
    defaults={
        "status":"idle","message":"尚未运行","started_at":None,"finished_at":None,
        "stocks":0,"processed":0,"current":"","error":None
    }
    if BACKTEST_STATUS_FILE.exists():
        try:
            x=json.loads(BACKTEST_STATUS_FILE.read_text(encoding="utf-8"))
            defaults.update(x)
        except Exception: pass
    return defaults

def write_backtest_status(**kw):
    BACKTEST_DIR.mkdir(parents=True,exist_ok=True)
    s=read_backtest_status(); s.update(kw)
    BACKTEST_STATUS_FILE.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")

def backtest_summary_rows():
    p=BACKTEST_DIR/"jlaw_summary_v2.csv"
    if not p.exists(): return []
    try:
        d=pd.read_csv(p)
        return d.replace({np.nan:None}).to_dict("records")
    except Exception:
        return []

def run_jlaw_backtest():
    global backtest_thread
    try:
        from jlaw_backtest_v2 import run as jlaw_run
        spy_path=MARKET_DIR/"SPY.csv"
        if not spy_path.exists(): raise RuntimeError("找不到 /data/market/SPY.csv")
        stock_paths=[]
        for t in UNIVERSE:
            if t in JLAW_EXCLUDED: continue
            p=MARKET_DIR/f"{t}.csv"
            if p.exists(): stock_paths.append(str(p))
        total=len(stock_paths)
        if not total: raise RuntimeError("没有找到可用于 J Law 回测的缓存股票")
        write_backtest_status(
            status="running",message=f"准备回测 {total} 只股票；不调用 Tiingo",
            started_at=datetime.now(timezone.utc).isoformat(),finished_at=None,
            stocks=total,processed=0,current="",error=None
        )
        def progress(done,total,ticker):
            write_backtest_status(
                status="running",
                message=f"正在回测 {done}/{total}：{ticker}",
                stocks=total,processed=done,current=ticker
            )
        jlaw_run(str(spy_path),stock_paths,str(BACKTEST_DIR),progress_cb=progress)
        write_backtest_status(
            status="done",message=f"J Law V2 回测完成：{total} 只股票",
            finished_at=datetime.now(timezone.utc).isoformat(),
            stocks=total,processed=total,current="",error=None
        )
    except Exception as ex:
        traceback.print_exc()
        write_backtest_status(
            status="error",message="J Law V2 回测失败",
            finished_at=datetime.now(timezone.utc).isoformat(),
            error=f"{type(ex).__name__}: {ex}"
        )
    finally:
        backtest_thread=None

def start_jlaw_backtest():
    global backtest_thread
    if backtest_thread is not None and backtest_thread.is_alive(): return False
    backtest_thread=threading.Thread(target=run_jlaw_backtest,daemon=True)
    backtest_thread.start()
    return True


def read_v3_status():
    s={"status":"idle","message":"尚未运行","stocks":0,"processed":0,"current":"","error":None,"started_at":None,"finished_at":None}
    if V3_STATUS_FILE.exists():
        try:s.update(json.loads(V3_STATUS_FILE.read_text(encoding="utf-8")))
        except Exception:pass
    return s

def write_v3_status(**kw):
    V3_DIR.mkdir(parents=True,exist_ok=True)
    s=read_v3_status();s.update(kw)
    V3_STATUS_FILE.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")

def v3_result_dir():
    s=read_v3_status()
    return (V3_DIR/"quick") if s.get("mode")=="quick" else V3_DIR

def v3_rows():
    p=v3_result_dir()/"jlaw_v3_summary.csv"
    if not p.exists(): return []
    try:
        d=pd.read_csv(p)
        return d.where(pd.notnull(d), None).to_dict("records")
    except Exception:
        return []

def v3_diagnostics():
    base=v3_result_dir()
    def count_csv(name):
        p=base/name
        if not p.exists():
            return {"exists":False,"rows":None,"size":0}
        try:
            size=p.stat().st_size
            if size==0:
                return {"exists":True,"rows":0,"size":0}
            d=pd.read_csv(p)
            return {"exists":True,"rows":len(d),"size":size}
        except pd.errors.EmptyDataError:
            return {"exists":True,"rows":0,"size":p.stat().st_size}
        except Exception as ex:
            return {"exists":True,"rows":None,"size":p.stat().st_size,"error":str(ex)}

    funnel={}
    fp=base/"jlaw_v3_funnel_total.csv"
    if fp.exists():
        try:
            fd=pd.read_csv(fp)
            if len(fd):
                funnel={k:(int(v) if pd.notna(v) else 0) for k,v in fd.iloc[0].to_dict().items()}
        except Exception:
            funnel={}

    supply_stats=[]
    sp=base/"jlaw_v3_supply_stats.csv"
    if sp.exists():
        try:
            supply_stats=pd.read_csv(sp).where(pd.notnull(pd.read_csv(sp)),None).to_dict("records")
        except Exception:
            try:
                sd=pd.read_csv(sp)
                supply_stats=sd.where(pd.notnull(sd),None).to_dict("records")
            except Exception:
                supply_stats=[]
    return {
        "summary":count_csv("jlaw_v3_summary.csv"),
        "signals":count_csv("jlaw_v3_signals.csv"),
        "trades":count_csv("jlaw_v3_trades.csv"),
        "funnel":funnel,
        "supply_stats":supply_stats,
        "folder":"quick" if base.name=="quick" else "full"
    }

def run_v3(mode="full"):
    global v3_thread
    try:
        from jlaw_v3_phase1 import run as v3run
        v3_stop_event.clear()
        try:
            if V3_STOP_FILE.exists(): V3_STOP_FILE.unlink()
        except Exception:
            pass
        quick = (mode == "quick")
        label = "快速验证 22只" if quick else "完整回测 122只"
        write_v3_status(
            status="running", mode=mode,
            message=f"正在运行 J Law V3 Phase 1 · {label}",
            started_at=datetime.now(timezone.utc).isoformat(),
            finished_at=None,error=None,processed=0,current=""
        )
        def prog(done,total,t):
            if v3_stop_event.is_set():
                raise RuntimeError("__V3_STOPPED__")
            write_v3_status(
                status="running",mode=mode,
                message=f"{label}：{done}/{total} · {t}",
                stocks=total,processed=done,current=t
            )
        v3run(
            str(MARKET_DIR), str(V3_DIR),
            progress_cb=prog,
            quick=quick, quick_n=22, quick_lookback=420,
            stop_file=str(V3_STOP_FILE)
        )
        if V3_STOP_FILE.exists() or v3_stop_event.is_set():
            raise RuntimeError("__V3_STOPPED__")
        s=read_v3_status()
        write_v3_status(
            status="done",mode=mode,
            message=f"J Law V3 Phase 1 · {label}完成",
            stocks=s.get("stocks",0),processed=s.get("stocks",0),
            current="",finished_at=datetime.now(timezone.utc).isoformat(),error=None
        )
    except Exception as ex:
        if str(ex) == "__V3_STOPPED__":
            s=read_v3_status()
            write_v3_status(
                status="stopped",mode=mode,
                message="已停止当前 V3 回测",
                stocks=s.get("stocks",0),processed=s.get("processed",0),
                current="",finished_at=datetime.now(timezone.utc).isoformat(),error=None
            )
        else:
            traceback.print_exc()
            write_v3_status(
                status="error",mode=mode,message="V3 回测失败",
                error=f"{type(ex).__name__}: {ex}",
                finished_at=datetime.now(timezone.utc).isoformat()
            )
    finally:
        v3_thread=None
        v3_stop_event.clear()

def start_v3(mode="full"):
    global v3_thread
    if v3_thread is not None and v3_thread.is_alive():
        return False
    v3_thread=threading.Thread(target=run_v3,args=(mode,),daemon=True)
    v3_thread.start()
    return True

def stop_v3():
    # Persistent stop flag: survives page refresh and is visible to the engine
    V3_DIR.mkdir(parents=True,exist_ok=True)
    try:
        V3_STOP_FILE.write_text("stop",encoding="utf-8")
    except Exception:
        pass
    v3_stop_event.set()
    s=read_v3_status()
    write_v3_status(
        status="stopped",
        mode=s.get("mode","full"),
        message="已停止 V3 回测",
        stocks=s.get("stocks",0),
        processed=s.get("processed",0),
        current="",
        finished_at=datetime.now(timezone.utc).isoformat(),
        error=None
    )
    return True

def recover_stale_v3_status():
    # Railway Restart kills the thread but /data/status.json survives.
    # Convert ghost running/stopping states to interrupted on boot.
    s=read_v3_status()
    if s.get("status") in ("running","stopping"):
        write_v3_status(
            status="interrupted",
            mode=s.get("mode","full"),
            message="上一次回测因服务重启/中断而停止，可重新开始",
            stocks=s.get("stocks",0),
            processed=s.get("processed",0),
            current="",
            finished_at=datetime.now(timezone.utc).isoformat(),
            error=None
        )

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
<title>VCP Radar Cloud Final</title><style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial;background:#f3f6fb;color:#17202a;margin:0}
.wrap{max-width:1180px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(175px,1fr));gap:12px}
.card{background:white;border-radius:16px;padding:16px;margin:12px 0;box-shadow:0 4px 18px #0000000c}
h1{margin:0;font-size:28px}.muted{color:#667085}.badge{padding:7px 11px;border-radius:999px;background:#eef4ff;font-weight:700}
button{border:0;border-radius:10px;padding:10px 13px;font-weight:700;background:#111827;color:white;margin:2px}
button.alt{background:#eef2f7;color:#17202a}.warn{color:#a35b00}.kpi{font-size:26px;font-weight:800}
.progress{height:11px;background:#edf1f7;border-radius:999px;overflow:hidden;margin:12px 0}.progress>div{height:100%;background:#111827}
table{width:100%;border-collapse:collapse;min-width:980px}th,td{padding:10px;border-bottom:1px solid #edf0f4;text-align:left}
th{font-size:12px;color:#667085}.scroll{overflow-x:auto}.score{font-size:20px;font-weight:800}.detail{background:#f8fafc;border-radius:12px;padding:12px;line-height:1.7}
</style></head><body><div class="wrap">
<div class="top"><div><h1>VCP Radar Cloud · Final</h1><div class="muted">Tiingo统一数据 · V2.2冻结评分 · 增量更新 · 本地缓存 · 手机版</div></div>
<form method="post" action="/scan"><button>立即更新</button></form></div>

<div class="grid">
<div class="card"><div class="muted">持久储存</div><div class="kpi">{{storage_dir}}</div><div class="muted">可写：{{"是" if storage_writable else "否"}}</div></div>
<div class="card"><div class="muted">市场环境</div><div class="kpi">{{market_emoji}} {{market_text}}</div></div>
<div class="card"><div class="muted">本地缓存</div><div class="kpi">{{cached_count}} / {{cache_total}}</div><div class="muted">最新：{{cache_latest}}</div></div>
<div class="card"><div class="muted">90+ 顶级候选</div><div class="kpi">{{count90}}</div></div>
<div class="card"><div class="muted">严格 VCP</div><div class="kpi">{{count_strict}}</div></div>
<div class="card"><div class="muted">突破候选</div><div class="kpi">{{count_breakout}}</div></div>
</div>

<div class="card"><div class="top"><div><b>状态：</b>{{status}}</div><div class="badge">{{updated}} / {{total}}</div></div>
<p>{{message}}</p><div class="progress"><div style="width:{{ (updated/total*100) if total else 0 }}%"></div></div>
<div class="muted">当前：{{last_symbol}} ｜ 成功：{{success_count}} ｜ 失败：{{fail_count}} ｜ 请求间隔：{{"%.0f"|format(gap_seconds)}}秒</div>
<div class="muted">上次完成：{{last_scan or "尚未完成"}}</div>
{% if error %}<p class="warn">最近错误：{{error}}</p>{% endif %}</div>

{% if sector_rows %}
<div class="card"><h2>板块强弱</h2><div class="scroll"><table style="min-width:520px">
<tr><th>板块</th><th>候选数</th><th>平均评分</th><th>90+数量</th></tr>
{% for s in sector_rows %}<tr><td><b>{{s.sector}}</b></td><td>{{s.count}}</td><td>{{"%.1f"|format(s.avg_score)}}</td><td>{{s.count90}}</td></tr>{% endfor %}
</table></div></div>
{% endif %}

<div class="card"><div class="top"><h2>Top 候选</h2><div>
<button type="button" class="alt" onclick="setFilter('all')">全部</button>
<button type="button" class="alt" onclick="setFilter('90')">90+</button>
<button type="button" class="alt" onclick="setFilter('80')">80–89</button>
<button type="button" class="alt" onclick="setFilter('strict')">严格VCP</button>
<button type="button" class="alt" onclick="setFilter('near')">接近买点</button>
<button type="button" class="alt" onclick="setFilter('breakout')">突破候选</button>
</div></div>
<div class="scroll"><table>
<tr><th>股票</th><th>评分</th><th>板块</th><th>状态</th><th>现价</th><th>Pivot</th><th>距Pivot</th><th>RS</th><th>VCP质量</th><th>量比</th><th>7%风险线</th><th>详情</th></tr>
{% for r in rows %}
<tr class="rank-row" data-score="{{r.score}}" data-strict="{{1 if r.status=='严格VCP' else 0}}" data-near="{{1 if r.near_pivot else 0}}" data-breakout="{{1 if r.breakout else 0}}">
<td><b>{{r.ticker}}</b></td><td class="score">{{r.score}}</td><td>{{r.sector}}</td><td>{{r.status}}</td><td>{{"%.2f"|format(r.price)}}</td><td>{{"%.2f"|format(r.pivot)}}</td>
<td>{{"%.1f%%"|format(r.pivot_dist*100)}}</td><td>{{r.rs}}</td><td>{{r.vcp_quality}}</td><td>{{"%.2fx"|format(r.volratio)}}</td><td>{{"%.2f"|format(r.risk7)}}</td>
<td><button type="button" onclick="toggleDetail('{{r.ticker}}')">查看</button></td></tr>
<tr id="detail-{{r.ticker}}" style="display:none"><td colspan="12"><div class="detail">
<b>{{r.ticker}} · 为什么入选</b><br>{{r.reason}}<br>
收缩幅度：C1 {{"%.1f%%"|format(r.c1*100)}} ｜ C2 {{"%.1f%%"|format(r.c2*100)}} ｜ C3 {{"%.1f%%"|format(r.c3*100)}}<br>
收缩次数：{{r.contraction_count}}/3 ｜ 成交量收缩：{{"是" if r.volume_dry else "否"}} ｜ 近10日量/基准量：{{"%.2fx"|format(r.dry_ratio)}}<br>
趋势结构：{{"通过" if r.trend_ok else "未通过"}} ｜ 接近Pivot：{{"是" if r.near_pivot else "否"}} ｜ 放量突破：{{"是" if r.breakout else "否"}}<br>
板块分：{{r.sector_score}} ｜ RS：{{r.rs}} ｜ VCP质量：{{r.vcp_quality}}
</div></td></tr>{% endfor %}
</table></div></div>


<div class="card">
<div class="top">
<div>
<h2 style="margin:0">J Law 历史回测 V2</h2>
<div class="muted">使用现有 /data/market 缓存 · 不下载 · 不调用 Tiingo</div>
</div>
<form method="post" action="/backtest">
<button id="bt-btn" {% if bt_status=="running" %}disabled style="opacity:.45;cursor:not-allowed"{% endif %}>
{% if bt_status=="running" %}正在回测…{% else %}开始回测{% endif %}
</button>
</form>
</div>

{% if bt_status=="running" %}
<div class="progress"><div style="width:{{ ((bt_processed/bt_stocks*100) if bt_stocks else 0) }}%"></div></div>
<div><b>{{bt_processed}} / {{bt_stocks}}</b>　{{"%.0f"|format((bt_processed/bt_stocks*100) if bt_stocks else 0)}}%</div>
{% endif %}
<p><b>状态：</b>{{bt_status}}　{{bt_message}}</p>
{% if bt_error %}<p class="warn">回测错误：{{bt_error}}</p>{% endif %}
{% if bt_started_at %}<div class="muted">开始：{{bt_started_at}}{% if bt_finished_at %} ｜ 完成：{{bt_finished_at}}{% endif %}</div>{% endif %}

{% if bt_rows %}
<div class="scroll"><table style="min-width:1250px">
<tr>
<th>模型</th><th>止盈/退出</th><th>止损</th><th>交易数</th><th>胜率</th>
<th>平均盈利</th><th>平均亏损</th><th>盈亏比</th><th>平均收益</th>
<th>PF</th><th>止损率</th><th>平均持有</th><th>最大连亏</th>
</tr>
{% for b in bt_rows %}
<tr>
<td><b>{{b.model}}</b></td>
<td>{{b.target_label}}</td>
<td>{% if b.stop_pct is not none %}{{"%.1f%%"|format(b.stop_pct*100)}}{% else %}—{% endif %}</td>
<td>{{b.trades}}</td>
<td>{{"%.1f%%"|format((b.win_rate or 0)*100)}}</td>
<td>{{"%.2f%%"|format((b.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((b.avg_loss or 0)*100)}}</td>
<td>{{"%.2f"|format(b.payoff_ratio or 0)}}</td>
<td>{{"%.2f%%"|format((b.avg_return or 0)*100)}}</td>
<td>{{"%.2f"|format(b.profit_factor or 0)}}</td>
<td>{{"%.1f%%"|format((b.stop_rate or 0)*100)}}</td>
<td>{{"%.1f"|format(b.avg_hold or 0)}}天</td>
<td>{{b.max_consecutive_losses}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}

<div class="muted" style="margin-top:10px">
本轮同时测试：固定 -7% / +15,+20,+25,+30,+40,+50；J Law 中位数 -4.5%/+13%；以及 Native V1（结构止损≤8%、+20%卖一半、余仓跟30周趋势/Stage 2）。
Native V1 是公开规则的量化近似，不假装等同于全部主观判断。
</div>
</div>


<div class="card">
<div class="top"><div><h2 style="margin:0">J Law V3.1 · VCP Structure Engine</h2>
<div class="muted">只测试选股质量：Market Gate + Stage 2 + Sector/Group + Leader + 真实Swing收缩 + Supply Dry-Up + Tight Pivot；退出固定沿用 -7% / +30% / 60日</div></div>
<div>
<form method="post" action="/v3/quick" style="display:inline">
<button {% if v3_status in ["running","stopping"] %}disabled style="opacity:.45"{% endif %}>
{% if v3_status in ["running","stopping"] and v3_mode=="quick" %}⚡ 快速验证中…{% else %}⚡ 快速验证 22只{% endif %}
</button></form>
<form method="post" action="/v3/full" style="display:inline">
<button {% if v3_status in ["running","stopping"] %}disabled style="opacity:.45"{% endif %}>
{% if v3_status in ["running","stopping"] and v3_mode=="full" %}完整回测中…{% else %}完整回测 122只{% endif %}
</button></form>
{% if v3_status in ["running","stopping"] %}
<form method="post" action="/v3/stop" style="display:inline">
<button style="background:#7f1d1d">■ 停止回测</button>
</form>
{% endif %}
</div></div>
{% if v3_status in ["running","stopping"] %}<div class="progress"><div style="width:{{((v3_processed/v3_stocks*100) if v3_stocks else 0)}}%"></div></div>
<div><b>{{v3_processed}} / {{v3_stocks}}</b> · {{"%.0f"|format((v3_processed/v3_stocks*100) if v3_stocks else 0)}}%</div>{% endif %}
<p><b>状态：</b>{{v3_status}}　{{v3_message}}</p>
{% if v3_error %}<p class="warn">{{v3_error}}</p>{% endif %}
{% if v3_status=="done" %}
<div style="background:#f6f8fb;border-radius:12px;padding:12px;margin:10px 0">
<b>本次结果文件：</b>
{% if v3_diag.folder=="quick" %}⚡ 22只快速验证{% else %}完整回测{% endif %} ｜
信号 {{v3_diag.signals.rows if v3_diag.signals.rows is not none else "读取失败"}} 条 ｜ 
交易 {{v3_diag.trades.rows if v3_diag.trades.rows is not none else "读取失败"}} 笔 ｜ 
汇总 {{v3_diag.summary.rows if v3_diag.summary.rows is not none else "读取失败"}} 行
{% if v3_diag.funnel %}
<div style="margin-top:10px;line-height:1.8">
<b>Gate 漏斗：</b>
历史检查 {{v3_diag.funnel.get("bars_checked",0)}} →
市场 {{v3_diag.funnel.get("market_pass",0)}} →
Stage 2 {{v3_diag.funnel.get("stage2_pass",0)}} →
RS {{v3_diag.funnel.get("rs_pass",0)}} →
板块 {{v3_diag.funnel.get("sector_pass",0)}} →
Group {{v3_diag.funnel.get("group_pass",0)}} →
Supply {{v3_diag.funnel.get("supply_pass",0)}} →
Leader {{v3_diag.funnel.get("leader_pass",0)}} →
突破 {{v3_diag.funnel.get("breakout_pass",0)}}
</div>
{% endif %}

{% if v3_diag.supply_stats %}
<div style="margin-top:12px">
<b>Supply 分布诊断：</b>
<div class="scroll"><table style="min-width:760px">
<tr><th>指标</th><th>P10</th><th>P25</th><th>中位数</th><th>P75</th><th>P90</th><th>最大</th></tr>
{% for r in v3_diag.supply_stats %}
<tr>
<td>{{r.metric}}</td>
<td>{{"%.3f"|format(r.p10 or 0)}}</td>
<td>{{"%.3f"|format(r.p25 or 0)}}</td>
<td><b>{{"%.3f"|format(r.median or 0)}}</b></td>
<td>{{"%.3f"|format(r.p75 or 0)}}</td>
<td>{{"%.3f"|format(r.p90 or 0)}}</td>
<td>{{"%.3f"|format(r.max or 0)}}</td>
</tr>
{% endfor %}
</table></div>
<div class="muted" style="margin-top:8px">
V3.1 暂定门槛：Structure/Supply Score ≥40 且 Final Pivot Tightness &lt;16%。先验证真实 Swing 收缩引擎能否正常产生候选，再做最终校准。
</div>
</div>
{% endif %}
{% if (v3_diag.trades.rows==0) %}<div class="warn" style="margin-top:8px">V3 本轮经过 A+ Gate 后为 0 笔交易，所以没有成绩表。这不是结果丢失，而是筛选条件过严。</div>{% endif %}
</div>
{% endif %}
{% if v3_results %}
<div class="scroll"><table style="min-width:1050px"><tr><th>等级</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>平均盈利</th><th>平均亏损</th><th>盈亏比</th><th>PF</th><th>目标命中率</th><th>止损率</th><th>平均持有</th></tr>
{% for b in v3_results %}<tr><td><b>{{b.grade}}</b></td><td>{{b.trades}}</td><td>{{"%.1f%%"|format((b.win_rate or 0)*100)}}</td><td>{{"%.2f%%"|format((b.avg_return or 0)*100)}}</td><td>{{"%.2f%%"|format((b.avg_win or 0)*100)}}</td><td>{{"%.2f%%"|format((b.avg_loss or 0)*100)}}</td><td>{{"%.2f"|format(b.payoff_ratio or 0)}}</td><td>{{"%.2f"|format(b.profit_factor or 0)}}</td><td>{{"%.1f%%"|format((b.target_hit_rate or 0)*100)}}</td><td>{{"%.1f%%"|format((b.stop_rate or 0)*100)}}</td><td>{{"%.1f"|format(b.avg_hold or 0)}}天</td></tr>{% endfor %}
</table></div>{% endif %}
<div class="muted">目标：与旧 baseline（约234笔、胜率43.6%、PF 1.55、平均收益约2.03%）做同口径比较。Fundamental 历史数据尚未加入，所以这是 V3 Phase 1，不是最终 J Law Pure。</div>
</div>
<div class="card muted" style="font-size:13px">本系统是研究/筛选工具，不保证未来收益。V2.2评分仍需继续扩大样本与独立验证。</div>
</div>
<script>
function setFilter(kind){
 document.querySelectorAll('.rank-row').forEach(row=>{
  const score=Number(row.dataset.score||0);
  const show=kind==='all'||(kind==='90'&&score>=90)||(kind==='80'&&score>=80&&score<90)||(kind==='strict'&&row.dataset.strict==='1')||(kind==='near'&&row.dataset.near==='1')||(kind==='breakout'&&row.dataset.breakout==='1');
  row.style.display=show?'':'none';
  const ticker=row.querySelector('td b')?.textContent; const d=ticker?document.getElementById('detail-'+ticker):null; if(d&&!show)d.style.display='none';
 });
}
function toggleDetail(ticker){const el=document.getElementById('detail-'+ticker);if(el)el.style.display=(el.style.display==='none'||!el.style.display)?'':'none';}
async function poll(){try{const r=await fetch('/api/status',{cache:'no-store'});const s=await r.json();const b=await fetch('/api/backtest-status',{cache:'no-store'});const bs=await b.json();const vr=await fetch('/api/v3-status',{cache:'no-store'});const vs=await vr.json();if(s.status==='updating'||bs.status==='running'||(vs.status==='running'||vs.status==='stopping'))setTimeout(()=>location.reload(),3000);}catch(e){}}
setTimeout(poll,5000);
</script></body></html>"""

@APP.get("/")
def home():
    s=read_state()
    rows=[]; sector_rows=[]
    count90=count_strict=count_breakout=0
    if RESULT_FILE.exists():
        try:
            df=pd.read_csv(RESULT_FILE)
            defaults={"sector":"其他","near_pivot":False,"breakout":False,"trend_ok":False,"contraction_count":0,
                      "c1":0.0,"c2":0.0,"c3":0.0,"dry_ratio":1.0,"volume_dry":False,
                      "reason":"旧结果，请点“立即更新”生成完整详情","sector_score":50}
            for k,v in defaults.items():
                if k not in df.columns: df[k]=v
            count90=int((df["score"]>=90).sum())
            count_strict=int((df["status"]=="严格VCP").sum())
            count_breakout=int(df["breakout"].astype(bool).sum())
            if len(df):
                grp=df.groupby("sector").agg(count=("ticker","count"),avg_score=("score","mean"),
                    count90=("score",lambda x:int((x>=90).sum()))).reset_index()
                sector_rows=grp.sort_values(["avg_score","count90"],ascending=False).to_dict("records")
            rows=df.head(TOP_N).to_dict("records")
        except Exception:
            rows=[]
    cs=cache_summary()
    si=storage_info()
    bt=read_backtest_status()
    bt_rows=backtest_summary_rows()
    return render_template_string(HTML,rows=rows,sector_rows=sector_rows,
        count90=count90,count_strict=count_strict,count_breakout=count_breakout,
        cached_count=cs["cached_count"],cache_total=cs["cache_total"],cache_latest=cs["cache_latest"],storage_dir=si["data_dir"],storage_writable=si["writable"],
        bt_status=bt.get("status","idle"),bt_message=bt.get("message","尚未运行"),
        bt_error=bt.get("error"),bt_started_at=bt.get("started_at"),bt_finished_at=bt.get("finished_at"),
        bt_stocks=bt.get("stocks",0),bt_processed=bt.get("processed",0),bt_rows=bt_rows,
        v3_status=read_v3_status().get("status","idle"),v3_message=read_v3_status().get("message","尚未运行"),
        v3_error=read_v3_status().get("error"),v3_stocks=read_v3_status().get("stocks",0),
        v3_processed=read_v3_status().get("processed",0),v3_mode=read_v3_status().get("mode","full"),v3_results=v3_rows(),v3_diag=v3_diagnostics(),
        **s)

@APP.post("/scan")
def scan():
    start_scan(); return redirect(url_for("home"))

@APP.post("/v3/quick")
def v3_quick_start():
    start_v3("quick")
    return redirect(url_for("home"))

@APP.post("/v3/full")
def v3_full_start():
    start_v3("full")
    return redirect(url_for("home"))

@APP.post("/v3/stop")
def v3_stop():
    stop_v3()
    return redirect(url_for("home"))

@APP.get("/api/v3-status")
def api_v3_status():
    return jsonify(read_v3_status())

@APP.post("/backtest")
def backtest_start():
    start_jlaw_backtest()
    return redirect(url_for("home"))

@APP.get("/api/backtest-status")
def api_backtest_status():
    return jsonify(read_backtest_status())

@APP.get("/api/status")
def api_status(): return jsonify(read_state())

@APP.get("/api/results")
def api_results():
    if not RESULT_FILE.exists(): return jsonify([])
    try:
        df=pd.read_csv(RESULT_FILE).head(TOP_N)
        return jsonify(df.replace({np.nan:None}).to_dict("records"))
    except Exception:
        return jsonify([])

@APP.get("/health")
def health():
    info=storage_info()
    return jsonify({"ok":True,"version":"volume-ready-1.4-429-pause","data_dir":info["data_dir"],"writable":info["writable"]})

recover_stale_v3_status()
threading.Thread(target=scheduler_loop, daemon=True).start()
