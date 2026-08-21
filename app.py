import os, json, time, threading, traceback, io, math
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string, redirect, url_for, send_file

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



def v3_shadow_results():
    base=v3_result_dir()
    p=base/"jlaw_v3_shadow_summary.csv"
    if not p.exists() or p.stat().st_size==0:
        return []
    try:
        d=pd.read_csv(p)
        return d.where(pd.notnull(d),None).to_dict("records")
    except Exception:
        return []


def v3_shadow_trade_details():
    base=v3_result_dir()
    p=base/"jlaw_v3_shadow_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return []
    try:
        d=pd.read_csv(p)
        if d.empty:return []
        for c in ["entry","volratio","quality","rs","supply_score","return_pct","mfe","mae",
                  "stop_pct","target_pct","atr_pct","hold_days"]:
            if c in d.columns:d[c]=pd.to_numeric(d[c],errors="coerce")
        d["return_display"]=d["return_pct"]*100
        d["mfe_display"]=d["mfe"]*100
        d["mae_display"]=d["mae"]*100
        d["stop_display"]=d["stop_pct"]*100
        d["target_display"]=d["target_pct"]*100
        d["atr_display"]=d["atr_pct"]*100
        keep=["ticker","date","mode","volratio","atr_display","stop_display","target_display",
              "mfe_display","mae_display","hold_days","reason","return_display","be_armed"]
        keep=[c for c in keep if c in d.columns]
        d=d[keep].sort_values(["ticker","mode"])
        return d.where(pd.notnull(d),None).to_dict("records")
    except Exception:
        return []


V4_WEIGHT_SCHEMES = {
    # supply, setup(quality), volume, leader, sector, rs
    "SUPPLY_HEAVY":      dict(supply=0.30, setup=0.20, volume=0.15, leader=0.15, sector=0.10, rs=0.10),
    "VOLUME_HEAVY":      dict(supply=0.20, setup=0.15, volume=0.30, leader=0.15, sector=0.10, rs=0.10),
    "SUPPLY_VOLUME":     dict(supply=0.27, setup=0.16, volume=0.27, leader=0.14, sector=0.10, rs=0.06),
    "SETUP_LEADER":      dict(supply=0.23, setup=0.24, volume=0.16, leader=0.22, sector=0.10, rs=0.05),
    "BALANCED_ELITE":    dict(supply=0.25, setup=0.20, volume=0.20, leader=0.15, sector=0.10, rs=0.10),
}

def _v4_factor_frame():
    base=v3_result_dir()
    p=base/"jlaw_v3_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame()
    try:
        d=pd.read_csv(p)
        if d.empty:return d
        for c in ["supply_score","quality","volratio","leader_score","sector_score","rs","return_pct","hold_days"]:
            if c in d.columns:d[c]=pd.to_numeric(d[c],errors="coerce")
        need=["supply_score","quality","volratio","leader_score","sector_score","rs","return_pct"]
        if any(c not in d.columns for c in need):
            return pd.DataFrame()

        # Fixed, pre-declared transforms. No outcome variable is used to score a trade.
        d["f_supply"]=d["supply_score"].clip(0,100)
        d["f_setup"]=d["quality"].clip(0,100)

        # Volume confirmation: 1.2x is the old floor; 2.5x+ gets max score.
        d["f_volume"]=((d["volratio"]-1.20)/(2.50-1.20)*100).clip(0,100)

        d["f_leader"]=d["leader_score"].clip(0,100)
        d["f_sector"]=d["sector_score"].clip(0,100)
        d["f_rs"]=d["rs"].clip(0,100)

        for name,w in V4_WEIGHT_SCHEMES.items():
            d["score_"+name]=(
                d["f_supply"]*w["supply"] +
                d["f_setup"]*w["setup"] +
                d["f_volume"]*w["volume"] +
                d["f_leader"]*w["leader"] +
                d["f_sector"]*w["sector"] +
                d["f_rs"]*w["rs"]
            )
        return d
    except Exception:
        return pd.DataFrame()

def _v4_metrics(g):
    if g is None or g.empty:
        return dict(trades=0, win_rate=np.nan, avg_return=np.nan, pf=np.nan,
                    avg_win=np.nan, avg_loss=np.nan, avg_hold=np.nan, max_losing_streak=0)
    r=pd.to_numeric(g["return_pct"],errors="coerce").dropna()
    if r.empty:
        return dict(trades=0, win_rate=np.nan, avg_return=np.nan, pf=np.nan,
                    avg_win=np.nan, avg_loss=np.nan, avg_hold=np.nan, max_losing_streak=0)
    wins=r[r>0]; losses=r[r<0]
    pf=(wins.sum()/(-losses.sum())) if len(losses) and -losses.sum()>0 else (np.inf if len(wins) else 0)
    # sequence-based losing streak in original trade order
    streak=mx=0
    for x in r.tolist():
        if x<0:
            streak+=1; mx=max(mx,streak)
        else:
            streak=0
    return dict(
        trades=len(r),
        win_rate=(r>0).mean(),
        avg_return=r.mean(),
        pf=pf,
        avg_win=wins.mean() if len(wins) else np.nan,
        avg_loss=losses.mean() if len(losses) else np.nan,
        avg_hold=pd.to_numeric(g.get("hold_days"),errors="coerce").mean() if "hold_days" in g.columns else np.nan,
        max_losing_streak=mx
    )

def v4_weight_sweep():
    d=_v4_factor_frame()
    if d.empty:return []
    rows=[]
    # Compare exactly the sample sizes the user asked for, plus 100 and all.
    cuts=[10,20,30,50,100]
    for name in V4_WEIGHT_SCHEMES:
        sc="score_"+name
        ranked=d.sort_values(sc,ascending=False,kind="mergesort")
        for n in cuts:
            g=ranked.head(min(n,len(ranked)))
            m=_v4_metrics(g)
            rows.append(dict(
                scheme=name, selection=f"TOP{n}", cutoff=float(g[sc].min()) if len(g) else np.nan,
                **m
            ))
        # Also include score threshold slices so future weights can be tuned without code changes.
        for th in [60,65,70,75]:
            g=d[d[sc]>=th].sort_values(sc,ascending=False)
            if len(g)==0: continue
            m=_v4_metrics(g)
            rows.append(dict(
                scheme=name, selection=f"SCORE>={th}", cutoff=th, **m
            ))
    return rows

def v4_best_by_size():
    rows=v4_weight_sweep()
    if not rows:return []
    d=pd.DataFrame(rows)
    out=[]
    for sel in ["TOP10","TOP20","TOP30","TOP50","TOP100"]:
        g=d[d.selection==sel].copy()
        if g.empty:continue
        # Primary rank = avg return, tie-break PF then win rate.
        g=g.sort_values(["avg_return","pf","win_rate"],ascending=False)
        r=g.iloc[0].to_dict()
        out.append(r)
    return out


V41_SCHEMES = {
    "SWEET_CORE": dict(
        volume=[(1.60,2.50,100),(1.50,1.60,70),(2.50,3.00,35),(1.20,1.50,30),(3.00,99,10)],
        quality=[(76,82,100),(73,76,55),(82,85,35),(0,73,25),(85,101,10)],
        sector=[(65,80,100),(60,65,50),(80,101,15),(0,60,25)],
        leader=[(70,90,100),(60,70,50),(90,101,45),(0,60,25)],
        supply=[(50,75,100),(40,50,55),(75,90,70),(90,101,35),(0,40,20)],
        weights=dict(volume=.25,quality=.22,sector=.20,leader=.18,supply=.15)
    ),
    "VOLUME_SECTOR": dict(
        volume=[(1.60,2.50,100),(1.50,1.60,70),(2.50,3.00,30),(1.20,1.50,25),(3.00,99,5)],
        quality=[(76,82,90),(73,76,50),(82,85,30),(0,73,20),(85,101,10)],
        sector=[(65,80,100),(60,65,45),(80,101,10),(0,60,20)],
        leader=[(70,90,85),(60,70,45),(90,101,40),(0,60,20)],
        supply=[(50,75,75),(40,50,45),(75,90,60),(90,101,30),(0,40,20)],
        weights=dict(volume=.32,quality=.15,sector=.28,leader=.15,supply=.10)
    ),
    "QUALITY_LEADER": dict(
        volume=[(1.50,2.50,95),(1.20,1.50,35),(2.50,3.00,25),(3.00,99,5)],
        quality=[(76,82,100),(73,76,55),(82,85,30),(0,73,20),(85,101,10)],
        sector=[(65,80,90),(60,65,45),(80,101,15),(0,60,20)],
        leader=[(70,90,100),(60,70,45),(90,101,45),(0,60,20)],
        supply=[(50,75,80),(40,50,50),(75,90,65),(90,101,30),(0,40,20)],
        weights=dict(volume=.20,quality=.28,sector=.17,leader=.25,supply=.10)
    ),
    "ROBUST_BALANCED": dict(
        volume=[(1.55,2.40,100),(1.40,1.55,60),(2.40,2.80,55),(1.20,1.40,25),(2.80,99,10)],
        quality=[(75,82,100),(72,75,55),(82,84,45),(0,72,25),(84,101,15)],
        sector=[(64,79,100),(60,64,55),(79,82,45),(82,101,15),(0,60,25)],
        leader=[(68,90,100),(60,68,55),(90,101,55),(0,60,25)],
        supply=[(48,78,100),(40,48,55),(78,90,70),(90,101,40),(0,40,20)],
        weights=dict(volume=.24,quality=.22,sector=.20,leader=.19,supply=.15)
    ),
}

def _sweet_score(x, bands):
    try: x=float(x)
    except Exception: return 0.0
    for lo,hi,score in bands:
        if lo <= x < hi:
            return float(score)
    return 0.0

def _v41_frame():
    base=v3_result_dir()
    p=base/"jlaw_v3_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return pd.DataFrame()
    d=pd.read_csv(p)
    if d.empty:return d
    for c in ["supply_score","quality","volratio","leader_score","sector_score","return_pct","hold_days"]:
        if c in d.columns:d[c]=pd.to_numeric(d[c],errors="coerce")
    need=["supply_score","quality","volratio","leader_score","sector_score","return_pct"]
    if any(c not in d.columns for c in need):
        return pd.DataFrame()

    for name,cfg in V41_SCHEMES.items():
        fv=d["volratio"].apply(lambda x:_sweet_score(x,cfg["volume"]))
        fq=d["quality"].apply(lambda x:_sweet_score(x,cfg["quality"]))
        fs=d["sector_score"].apply(lambda x:_sweet_score(x,cfg["sector"]))
        fl=d["leader_score"].apply(lambda x:_sweet_score(x,cfg["leader"]))
        fsp=d["supply_score"].apply(lambda x:_sweet_score(x,cfg["supply"]))
        w=cfg["weights"]
        d["v41_"+name]=fv*w["volume"]+fq*w["quality"]+fs*w["sector"]+fl*w["leader"]+fsp*w["supply"]
    return d

def _v41_metrics(g):
    if g is None or g.empty:
        return dict(trades=0,win_rate=np.nan,avg_return=np.nan,pf=np.nan,avg_win=np.nan,avg_loss=np.nan,avg_hold=np.nan,max_losing_streak=0)
    r=pd.to_numeric(g["return_pct"],errors="coerce").dropna()
    if r.empty:return dict(trades=0,win_rate=np.nan,avg_return=np.nan,pf=np.nan,avg_win=np.nan,avg_loss=np.nan,avg_hold=np.nan,max_losing_streak=0)
    wins=r[r>0]; losses=r[r<0]
    pf=(wins.sum()/(-losses.sum())) if len(losses) and -losses.sum()>0 else (np.inf if len(wins) else 0)
    streak=mx=0
    for x in r.tolist():
        if x<0:
            streak+=1; mx=max(mx,streak)
        else: streak=0
    return dict(
        trades=len(r),win_rate=(r>0).mean(),avg_return=r.mean(),
        pf=pf,avg_win=wins.mean() if len(wins) else np.nan,avg_loss=losses.mean() if len(losses) else np.nan,
        avg_hold=pd.to_numeric(g.get("hold_days"),errors="coerce").mean() if "hold_days" in g.columns else np.nan,
        max_losing_streak=mx
    )

def v41_sweet_spot_sweep():
    d=_v41_frame()
    if d.empty:return []
    rows=[]
    for name in V41_SCHEMES:
        col="v41_"+name
        ranked=d.sort_values(col,ascending=False,kind="mergesort")
        for n in [10,20,30,50,75,100,150]:
            g=ranked.head(min(n,len(ranked)))
            m=_v41_metrics(g)
            rows.append(dict(scheme=name,selection=f"TOP{n}",cutoff=float(g[col].min()) if len(g) else np.nan,**m))
        for th in [65,70,75,80,85]:
            g=d[d[col]>=th].sort_values(col,ascending=False)
            if len(g)==0: continue
            m=_v41_metrics(g)
            rows.append(dict(scheme=name,selection=f"SCORE>={th}",cutoff=float(th),**m))
    return rows

def v41_best_rows():
    rows=v41_sweet_spot_sweep()
    if not rows:return []
    d=pd.DataFrame(rows)
    out=[]
    for sel in ["TOP10","TOP20","TOP30","TOP50","TOP75","TOP100","TOP150"]:
        g=d[d.selection==sel].copy()
        if g.empty: continue
        g=g.sort_values(["avg_return","pf","win_rate"],ascending=False)
        out.append(g.iloc[0].to_dict())
    return out

def v41_regime_check():
    d=_v41_frame()
    if d.empty or "date" not in d.columns:return []
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    rows=[]
    for name in V41_SCHEMES:
        col="v41_"+name
        top=d.sort_values(col,ascending=False).head(min(50,len(d)))
        for label,mask in [
            ("<=2021", top["date"].dt.year<=2021),
            (">=2022", top["date"].dt.year>=2022)
        ]:
            g=top[mask]
            if len(g)==0:continue
            m=_v41_metrics(g)
            rows.append(dict(scheme=name,period=label,**m))
    return rows


def _v42_spy_regimes():
    """Historical SPY regime table using only information available on each date."""
    try:
        spy=load_cached("SPY")
        if spy is None or len(spy)<220:
            return pd.DataFrame()
        s=spy.copy().sort_index()
        s["ma20"]=s.Close.rolling(20).mean()
        s["ma50"]=s.Close.rolling(50).mean()
        s["ma200"]=s.Close.rolling(200).mean()
        s["ma50_old"]=s["ma50"].shift(20)
        s["ma200_old"]=s["ma200"].shift(20)
        s["ret20"]=s.Close/s.Close.shift(20)-1
        s["ret60"]=s.Close/s.Close.shift(60)-1

        def classify(r):
            vals=[r.Close,r.ma20,r.ma50,r.ma200,r.ma50_old,r.ma200_old]
            if any(pd.isna(v) for v in vals): return "UNKNOWN"
            strong=(r.Close>r.ma20>r.ma50>r.ma200 and
                    r.ma50>r.ma50_old and r.ma200>r.ma200_old and
                    (r.ret20 if pd.notna(r.ret20) else 0)>0)
            if strong:return "STRONG_BULL"
            bull=(r.Close>r.ma200 and r.ma200>r.ma200_old)
            if bull:return "BULL"
            weak=(r.Close>r.ma200 or r.ma50>r.ma200)
            if weak:return "TRANSITION"
            return "DEFENSIVE"

        s["regime"]=s.apply(classify,axis=1)
        s["spy_ret20"]=s["ret20"]
        s["spy_ret60"]=s["ret60"]
        return s[["regime","spy_ret20","spy_ret60"]].copy()
    except Exception:
        return pd.DataFrame()

def _v42_frame():
    d=_v41_frame()
    if d.empty or "date" not in d.columns:return pd.DataFrame()
    d=d.copy()
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    rg=_v42_spy_regimes()
    if rg.empty:
        d["regime"]="UNKNOWN"; d["spy_ret20"]=np.nan; d["spy_ret60"]=np.nan
        return d
    # Match each trade to the latest SPY session on/before entry date.
    left=d.sort_values("date").copy()
    right=rg.reset_index().rename(columns={rg.index.name or "index":"date"})
    if "date" not in right.columns:
        right=right.rename(columns={right.columns[0]:"date"})
    right["date"]=pd.to_datetime(right["date"],errors="coerce")
    right=right.sort_values("date")
    return pd.merge_asof(left,right,on="date",direction="backward")

def v42_regime_summary():
    d=_v42_frame()
    if d.empty:return []
    rows=[]
    for regime in ["STRONG_BULL","BULL","TRANSITION","DEFENSIVE","UNKNOWN"]:
        g=d[d["regime"]==regime].copy()
        if g.empty:continue
        m=_v41_metrics(g.sort_values("date"))
        rows.append(dict(regime=regime,selection="ALL_BASE",**m))
        for name in V41_SCHEMES:
            col="v41_"+name
            if col not in g.columns:continue
            for n in [10,20,30,50]:
                h=g.sort_values(col,ascending=False).head(min(n,len(g))).sort_values("date")
                if h.empty:continue
                mm=_v41_metrics(h)
                rows.append(dict(regime=regime,selection=f"{name}_TOP{n}",**mm))
    return rows

def v42_regime_best():
    rows=v42_regime_summary()
    if not rows:return []
    z=pd.DataFrame(rows)
    out=[]
    for regime in ["STRONG_BULL","BULL","TRANSITION","DEFENSIVE"]:
        g=z[(z.regime==regime)&(z.selection!="ALL_BASE")].copy()
        # Avoid tiny selections when regime itself is small: require >=10.
        g=g[g.trades>=10]
        if g.empty:continue
        # Prefer a robust blend, not just tiny-sample return.
        g["robust_rank"]=(g["avg_return"]*100) + np.minimum(g["pf"],5)*0.8 + g["win_rate"]*2
        r=g.sort_values(["robust_rank","trades"],ascending=[False,False]).iloc[0].to_dict()
        out.append(r)
    return out

def v42_drop_one_ablation():
    """Score-only ablation on the same 385 trades.
    This does NOT claim to be a full gate ablation because rejected candidates are not in the trade CSV.
    """
    d=_v42_frame()
    if d.empty:return []
    # Start from ROBUST_BALANCED sweet scores, then remove one component and re-normalize weights.
    cfg=V41_SCHEMES["ROBUST_BALANCED"]
    comps={}
    comps["volume"]=d["volratio"].apply(lambda x:_sweet_score(x,cfg["volume"]))
    comps["quality"]=d["quality"].apply(lambda x:_sweet_score(x,cfg["quality"]))
    comps["sector"]=d["sector_score"].apply(lambda x:_sweet_score(x,cfg["sector"]))
    comps["leader"]=d["leader_score"].apply(lambda x:_sweet_score(x,cfg["leader"]))
    comps["supply"]=d["supply_score"].apply(lambda x:_sweet_score(x,cfg["supply"]))
    w=cfg["weights"]

    rows=[]
    variants=["NONE","volume","quality","sector","leader","supply"]
    for drop in variants:
        active=[k for k in w if k!=drop]
        denom=sum(w[k] for k in active)
        score=sum(comps[k]*(w[k]/denom) for k in active)
        tmp=d.copy()
        tmp["_score"]=score
        for n in [30,50,100,150]:
            g=tmp.sort_values("_score",ascending=False).head(min(n,len(tmp))).sort_values("date")
            m=_v41_metrics(g)
            rows.append(dict(drop=("FULL" if drop=="NONE" else f"DROP_{drop.upper()}"),
                             selection=f"TOP{n}",**m))
    return rows

def v42_ablation_best():
    rows=v42_drop_one_ablation()
    if not rows:return []
    z=pd.DataFrame(rows)
    out=[]
    for sel in ["TOP30","TOP50","TOP100","TOP150"]:
        g=z[z.selection==sel].copy()
        if g.empty:continue
        full=g[g["drop"]=="FULL"]
        base_avg=float(full.iloc[0].avg_return) if len(full) else np.nan
        base_pf=float(full.iloc[0].pf) if len(full) else np.nan
        for _,r in g.iterrows():
            rr=r.to_dict()
            rr["delta_avg"]=float(r.avg_return-base_avg) if pd.notna(base_avg) else np.nan
            rr["delta_pf"]=float(r.pf-base_pf) if pd.notna(base_pf) else np.nan
            out.append(rr)
    return out


def v43_adaptive_candidates():
    """
    V4.3 regime-adaptive ranking, using only pre-trade factor values and the
    contemporaneous SPY regime. No outcome data is used in scoring.
    Design comes from V4.2 evidence:
      - STRONG_BULL: Quality/Leader + Volume are useful for the elite tail.
      - BULL: Volume/Sector and Robust Balanced are stronger.
      - TRANSITION/DEFENSIVE: do not promote trades in this research layer.
    """
    d=_v42_frame()
    if d.empty:
        return pd.DataFrame()

    out=[]
    for _,r in d.iterrows():
        regime=r.get("regime","UNKNOWN")
        if regime=="STRONG_BULL":
            # blend QUALITY_LEADER and ROBUST_BALANCED; volume remains important
            s1=float(r.get("v41_QUALITY_LEADER",0) or 0)
            s2=float(r.get("v41_ROBUST_BALANCED",0) or 0)
            s3=float(r.get("v41_VOLUME_SECTOR",0) or 0)
            score=0.45*s1+0.35*s2+0.20*s3
            gate="TRADE"
        elif regime=="BULL":
            # favor volume/sector + balanced robustness
            s1=float(r.get("v41_VOLUME_SECTOR",0) or 0)
            s2=float(r.get("v41_ROBUST_BALANCED",0) or 0)
            s3=float(r.get("v41_SWEET_CORE",0) or 0)
            score=0.45*s1+0.35*s2+0.20*s3
            gate="TRADE"
        elif regime in ("TRANSITION","DEFENSIVE"):
            score=0.0
            gate="BLOCK"
        else:
            score=0.0
            gate="REVIEW"

        rr=r.to_dict()
        rr["v43_score"]=score
        rr["v43_gate"]=gate
        out.append(rr)

    return pd.DataFrame(out)

def v43_adaptive_sweep():
    d=v43_adaptive_candidates()
    if d.empty:return []

    rows=[]
    tradeable=d[d["v43_gate"]=="TRADE"].copy()
    for scope,base in [("ALL_TRADEABLE",tradeable),
                       ("STRONG_BULL",tradeable[tradeable["regime"]=="STRONG_BULL"]),
                       ("BULL",tradeable[tradeable["regime"]=="BULL"])]:
        if base.empty:continue
        ranked=base.sort_values("v43_score",ascending=False,kind="mergesort")
        for n in [10,20,30,50,75,100,150]:
            g=ranked.head(min(n,len(ranked))).sort_values("date")
            if g.empty:continue
            m=_v41_metrics(g)
            rows.append(dict(scope=scope,selection=f"TOP{n}",
                             cutoff=float(g["v43_score"].min()),**m))
        for th in [70,75,80,85,90]:
            g=base[base["v43_score"]>=th].sort_values("date")
            if g.empty:continue
            m=_v41_metrics(g)
            rows.append(dict(scope=scope,selection=f"SCORE>={th}",
                             cutoff=float(th),**m))
    return rows

def v43_year_stability():
    d=v43_adaptive_candidates()
    if d.empty:return []
    d=d[d["v43_gate"]=="TRADE"].copy()
    d["year"]=pd.to_datetime(d["date"],errors="coerce").dt.year
    # Use the top 50 adaptive-ranked trades as the main elite research basket.
    top=d.sort_values("v43_score",ascending=False).head(min(50,len(d))).copy()
    rows=[]
    for start,end,label in [(2007,2011,"2007-2011"),
                            (2012,2016,"2012-2016"),
                            (2017,2021,"2017-2021"),
                            (2022,2026,"2022-2026")]:
        g=top[(top.year>=start)&(top.year<=end)].sort_values("date")
        if g.empty:continue
        m=_v41_metrics(g)
        rows.append(dict(period=label,**m))
    return rows

def v43_factor_policy():
    # Human-readable policy distilled from V4.2. This is display-only.
    return [
        dict(regime="STRONG_BULL", primary="Quality/Leader + Volume", secondary="Balanced/Sector",
             action="只取高分精英，避免单纯追最高绝对分"),
        dict(regime="BULL", primary="Volume/Sector + Balanced", secondary="Sweet Core",
             action="可适度放宽数量，但仍以甜蜜区间为主"),
        dict(regime="TRANSITION", primary="Market Risk", secondary="-",
             action="研究层默认不推送"),
        dict(regime="DEFENSIVE", primary="Capital Protection", secondary="-",
             action="研究层默认不推送"),
    ]


def _v44_candidate_scores(d):
    """Return candidate score columns already defined before outcomes are evaluated."""
    cands=[]
    for name in ["SWEET_CORE","VOLUME_SECTOR","QUALITY_LEADER","ROBUST_BALANCED"]:
        col="v41_"+name
        if col in d.columns:
            cands.append((name,col))
    if "v43_score" in d.columns:
        cands.append(("ADAPTIVE","v43_score"))
    return cands

def v44_walk_forward():
    """
    Anchored walk-forward research:
      1) choose the best model + score cutoff using ONLY the training window,
      2) freeze it,
      3) evaluate the next period.
    Candidate model definitions themselves were created during prior research, so this is
    a robustness check, not a pristine untouched holdout.
    """
    d=v43_adaptive_candidates()
    if d.empty or "date" not in d.columns:return []
    d=d.copy()
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    d["year"]=d["date"].dt.year

    windows=[
        (2016,2017,2019,"<=2016 -> 2017-2019"),
        (2019,2020,2022,"<=2019 -> 2020-2022"),
        (2022,2023,2024,"<=2022 -> 2023-2024"),
        (2024,2025,2026,"<=2024 -> 2025-2026"),
    ]
    rows=[]
    candidates=_v44_candidate_scores(d)
    for train_end,test_start,test_end,label in windows:
        train=d[d.year<=train_end].copy()
        test=d[(d.year>=test_start)&(d.year<=test_end)].copy()
        if train.empty or test.empty:continue

        best=None
        # Use cutoffs broad enough to avoid tiny in-sample tricks.
        for name,col in candidates:
            for th in [65,70,75,80,85]:
                tr=train[train[col]>=th].copy()
                if len(tr)<25:continue
                m=_v41_metrics(tr.sort_values("date"))
                # conservative objective: require PF>1 and reward return, PF, sample size
                obj=(m["avg_return"]*100) + min(m["pf"],3.0)*0.8 + min(len(tr),150)/150*0.35
                cand=dict(model=name,col=col,threshold=th,train_trades=len(tr),
                          train_win=m["win_rate"],train_avg=m["avg_return"],train_pf=m["pf"],objective=obj)
                if best is None or cand["objective"]>best["objective"]:
                    best=cand

        if best is None:continue
        te=test[test[best["col"]]>=best["threshold"]].copy()
        tm=_v41_metrics(te.sort_values("date"))
        rows.append(dict(
            window=label,model=best["model"],threshold=best["threshold"],
            train_trades=best["train_trades"],train_win=best["train_win"],
            train_avg=best["train_avg"],train_pf=best["train_pf"],
            test_trades=tm["trades"],test_win=tm["win_rate"],
            test_avg=tm["avg_return"],test_pf=tm["pf"],
            test_max_losing_streak=tm["max_losing_streak"]
        ))
    return rows

def v44_score_band_check():
    """Check whether higher score bands are monotonically better; useful against score overfit."""
    d=v43_adaptive_candidates()
    if d.empty:return []
    rows=[]
    bands=[(0,60),(60,70),(70,80),(80,85),(85,90),(90,101)]
    for name,col in _v44_candidate_scores(d):
        for lo,hi in bands:
            g=d[(d[col]>=lo)&(d[col]<hi)].copy()
            if g.empty:continue
            m=_v41_metrics(g.sort_values("date"))
            rows.append(dict(model=name,band=f"{lo}-{hi}",**m))
    return rows

def v44_recent_validation():
    """
    Fixed-rule validation on the recent era only. No retuning inside 2022-2026.
    Compare a few predeclared rules side by side.
    """
    d=v43_adaptive_candidates()
    if d.empty:return []
    d=d.copy()
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    recent=d[d["date"].dt.year>=2022].copy()
    if recent.empty:return []
    rules=[
        ("ADAPTIVE>=85", recent["v43_score"]>=85),
        ("ADAPTIVE>=90", recent["v43_score"]>=90),
        ("VOL_SECTOR>=80", recent.get("v41_VOLUME_SECTOR",0)>=80),
        ("ROBUST>=85", recent.get("v41_ROBUST_BALANCED",0)>=85),
        ("QUALITY_LEADER>=85", recent.get("v41_QUALITY_LEADER",0)>=85),
    ]
    rows=[]
    for name,mask in rules:
        g=recent[mask].copy().sort_values("date")
        if g.empty:continue
        m=_v41_metrics(g)
        rows.append(dict(rule=name,**m))
    return rows


def v45_champion_challenger():
    """
    Compare robust candidate rules against the original V3 BASE on identical historical trades.
    This is still post-filter research on the 385 trades; it does not reconstruct trades rejected by older gates.
    """
    d=v43_adaptive_candidates()
    if d.empty:return []
    d=d.copy()
    d["date"]=pd.to_datetime(d["date"],errors="coerce")

    rules=[
        ("V3_BASE_ALL", pd.Series(True,index=d.index)),
        ("ADAPTIVE>=70", d["v43_score"]>=70),
        ("ADAPTIVE>=85", d["v43_score"]>=85),
        ("VOL_SECTOR>=70", d["v41_VOLUME_SECTOR"]>=70),
        ("VOL_SECTOR>=80", d["v41_VOLUME_SECTOR"]>=80),
        ("ROBUST>=85", d["v41_ROBUST_BALANCED"]>=85),
        ("QUALITY_LEADER>=85", d["v41_QUALITY_LEADER"]>=85),
    ]
    rows=[]
    for name,mask in rules:
        g=d[mask].copy().sort_values("date")
        if g.empty:continue
        m=_v41_metrics(g)
        # Recent and old split shown side by side
        old=g[g["date"].dt.year<=2021]
        rec=g[g["date"].dt.year>=2022]
        om=_v41_metrics(old) if not old.empty else {}
        rm=_v41_metrics(rec) if not rec.empty else {}
        rows.append(dict(
            rule=name,**m,
            old_trades=om.get("trades",0),old_avg=om.get("avg_return",np.nan),old_pf=om.get("pf",np.nan),
            recent_trades=rm.get("trades",0),recent_avg=rm.get("avg_return",np.nan),recent_pf=rm.get("pf",np.nan)
        ))
    return rows

def v45_walkforward_verdict():
    wf=v44_walk_forward()
    if not wf:return []
    rows=[]
    for r in wf:
        n=int(r.get("test_trades",0) or 0)
        pf=float(r.get("test_pf",0) or 0)
        avg=float(r.get("test_avg",0) or 0)
        if n < 10:
            verdict="TOO_SMALL"
        elif pf >= 1.5 and avg > 0:
            verdict="PASS"
        elif pf >= 1.0 and avg >= 0:
            verdict="MARGINAL"
        else:
            verdict="FAIL"
        rr=dict(r)
        rr["verdict"]=verdict
        rows.append(rr)
    return rows

def v45_research_decision():
    """
    Conservative decision layer: no promotion to engine unless walk-forward robustness is adequate.
    """
    wf=v45_walkforward_verdict()
    if not wf:
        return dict(status="NO_DATA",reason="No walk-forward results")
    valid=[r for r in wf if int(r.get("test_trades",0) or 0)>=10]
    passes=[r for r in valid if r.get("verdict")=="PASS"]
    fails=[r for r in valid if r.get("verdict")=="FAIL"]
    if len(valid)>=2 and len(passes)>=2 and len(fails)==0:
        return dict(status="PROMOTE_CANDIDATE",reason="Multiple non-tiny walk-forward windows passed.")
    return dict(
        status="KEEP_RESEARCH",
        reason=f"Walk-forward is mixed: {len(passes)} pass, {len(fails)} fail, {len(wf)-len(valid)} tiny-sample windows. Do not replace the baseline yet."
    )


def v46_engine_summary():
    base=v3_result_dir()
    p=base/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:
        d=pd.read_csv(p)
    except Exception:
        return []
    if d.empty:return []
    for c in ["return_pct","hold_days","v46_score"]:
        if c in d.columns:d[c]=pd.to_numeric(d[c],errors="coerce")
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    rows=[]
    for label,g in [
        ("ALL",d),
        ("<=2021",d[d.date.dt.year<=2021]),
        ("2022+",d[d.date.dt.year>=2022]),
        ("SCORE>=85",d[d.get("v46_score",0)>=85] if "v46_score" in d.columns else d.iloc[0:0]),
        ("SCORE>=90",d[d.get("v46_score",0)>=90] if "v46_score" in d.columns else d.iloc[0:0]),
    ]:
        if g.empty:continue
        m=_v41_metrics(g.sort_values("date"))
        rows.append(dict(selection=label,**m))
    return rows


def v461_diag():
    base=v3_result_dir()
    p=base/"jlaw_v46_diag.json"
    out=dict(
        engine_version="NOT_DETECTED",
        base_signal_rows=0,
        regime_tradeable_rows=0,
        score_ge70_rows=0,
        v46_pass_rows=0,
        v46_signal_rows=0,
        v46_trade_rows=0,
        stocks_with_v46_signal=0,
        stocks_with_v46_trade=0,
        csv_signals_written=False,
        csv_trades_written=False,
        csv_summary_written=False,
        signals_csv_size=0,
        trades_csv_size=0,
        summary_csv_size=0,
        result_folder=str(base),
        last_error="",
        diag_file_exists=p.exists()
    )
    if p.exists():
        try:
            import json
            got=json.loads(p.read_text(encoding="utf-8"))
            if isinstance(got,dict): out.update(got)
        except Exception as e:
            out["last_error"]=f"diag read error: {type(e).__name__}: {e}"

    # Independently inspect physical files so we can detect stale/missing engine output.
    for key,fn in [
        ("signals_csv","jlaw_v46_signals.csv"),
        ("trades_csv","jlaw_v46_trades.csv"),
        ("summary_csv","jlaw_v46_summary.csv"),
    ]:
        fp=base/fn
        out[key+"_exists"]=fp.exists()
        out[key+"_size"]=fp.stat().st_size if fp.exists() else 0

    # Human-readable status.
    if out.get("engine_version")=="NOT_DETECTED":
        out["status"]="ENGINE_NOT_ACTIVE"
        out["diagnosis"]="当前结果目录没有 V4.6.1 诊断文件：很可能发动机代码未生效，或本次并未真正执行新版发动机。"
    elif int(out.get("base_signal_rows",0))==0:
        out["status"]="NO_BASE_SIGNALS"
        out["diagnosis"]="新版发动机已生效，但它本次看到的 BASE signals 为0。需要检查历史信号生成链路。"
    elif int(out.get("regime_tradeable_rows",0))==0:
        out["status"]="REGIME_BLOCKED_ALL"
        out["diagnosis"]="有 BASE signals，但没有任何 STRONG_BULL/BULL 行，问题集中在 Regime 判定。"
    elif int(out.get("score_ge70_rows",0))==0:
        out["status"]="SCORE_BLOCKED_ALL"
        out["diagnosis"]="Regime 有可交易信号，但 V4.6 score 没有任何一笔达到70，问题集中在评分计算。"
    elif int(out.get("v46_pass_rows",0))==0:
        out["status"]="PASS_FLAG_ERROR"
        out["diagnosis"]="score>=70 已经存在，但 v46_pass 仍为0，说明 pass flag 逻辑有问题。"
    elif int(out.get("v46_signal_rows",0))>0 and int(out.get("v46_trade_rows",0))==0:
        out["status"]="TRADE_SIM_EMPTY"
        out["diagnosis"]="V4.6 signals 已生成，但 trade() 没有产生交易，问题集中在交易模拟/索引匹配。"
    elif not out.get("trades_csv_exists",False):
        out["status"]="CSV_WRITE_MISSING"
        out["diagnosis"]="内存中有 V4.6 结果，但 trades CSV 不存在，问题集中在文件写出。"
    else:
        out["status"]="OK"
        out["diagnosis"]="V4.6.1 发动机与诊断链路正常，可读取真实 Challenger 结果。"
    return out

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


def v3_trade_details():
    base=v3_result_dir()
    p=base/"jlaw_v3_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return []
    try:
        d=pd.read_csv(p)
        if d.empty: return []
        for c in ["entry","pivot","return_pct","mfe","mae","volratio","rs","sector_score",
                  "leader_score","supply_score","group_breadth","quality","hold_days"]:
            if c in d.columns:
                d[c]=pd.to_numeric(d[c],errors="coerce")
        if "entry" in d.columns and "pivot" in d.columns:
            d["extension_pct"]=(d["entry"]/d["pivot"]-1)*100
        else:
            d["extension_pct"]=np.nan
        if "mfe" in d.columns:
            d["mfe_pct"]=d["mfe"]*100
        else:
            d["mfe_pct"]=np.nan
        if "mae" in d.columns:
            d["mae_pct"]=d["mae"]*100
        else:
            d["mae_pct"]=np.nan
        if "return_pct" in d.columns:
            d["return_display"]=d["return_pct"]*100
        else:
            d["return_display"]=np.nan

        def diagnose(r):
            mfe=r.get("mfe_pct")
            ext=r.get("extension_pct")
            vr=r.get("volratio")
            hold=r.get("hold_days")
            reason=str(r.get("reason",""))
            if "STOP" in reason and pd.notna(mfe) and mfe < 2:
                return "突破后几乎没有跟进，快速失败"
            if pd.notna(ext) and ext > 2:
                return "入场离 Pivot 偏远，追价风险"
            if pd.notna(vr) and vr < 1.5:
                return "突破量偏弱，需重点检查"
            if "STOP" in reason and pd.notna(hold) and hold <= 6:
                return "短期假突破/失败突破特征"
            return "需结合图形复核"
        d["diagnosis"]=d.apply(diagnose,axis=1)
        return d.where(pd.notnull(d),None).to_dict("records")
    except Exception:
        return []

def v3_signal_details():
    base=v3_result_dir()
    p=base/"jlaw_v3_signals.csv"
    if not p.exists() or p.stat().st_size==0:
        return []
    try:
        d=pd.read_csv(p)
        if d.empty:return []
        for c in ["entry","pivot","volratio","rs","sector_score","leader_score","supply_score","quality"]:
            if c in d.columns:d[c]=pd.to_numeric(d[c],errors="coerce")
        if "entry" in d.columns and "pivot" in d.columns:
            d["extension_pct"]=(d["entry"]/d["pivot"]-1)*100
        return d.where(pd.notnull(d),None).to_dict("records")
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


def _pdf_escape(s):
    s=str(s)
    return s.replace("\\","\\\\").replace("(","\\(").replace(")","\\)")

def _ascii_cell(v):
    """Keep the exported PDF font-safe and compact."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s=str(v)
    # Common Chinese labels/reasons -> compact ASCII where helpful.
    repl={
        "是":"Y","否":"N","完整回测":"FULL","快速验证":"QUICK",
        "科技":"TECH","金融":"FIN","医疗":"HLTH","工业":"IND",
        "能源":"ENERGY","材料":"MAT","通信":"COMM","房地产":"RE",
        "可选消费":"DISC","必需消费":"STAP","公用事业":"UTIL",
    }
    for a,b in repl.items():
        s=s.replace(a,b)
    # PDF built-in Courier is ASCII-safe. Replace remaining non-ASCII.
    return s.encode("ascii","replace").decode("ascii")

def _fmt_num(v, pct=False, digits=2):
    try:
        x=float(v)
        if math.isnan(x) or math.isinf(x):
            return ""
        if pct:
            return f"{x*100:.{digits}f}%"
        return f"{x:.{digits}f}"
    except Exception:
        return _ascii_cell(v)

def _table_lines(df, cols, widths, headers=None, transforms=None):
    if df is None or df.empty:
        return ["(no rows)"]
    headers=headers or cols
    transforms=transforms or {}
    def clip(s,w):
        s=_ascii_cell(s)
        return s[:w].ljust(w)
    out=[" ".join(clip(h,w) for h,w in zip(headers,widths))]
    out.append("-"*min(145, sum(widths)+len(widths)-1))
    for _,r in df.iterrows():
        vals=[]
        for c,w in zip(cols,widths):
            v=r.get(c,"")
            if c in transforms:
                try:v=transforms[c](v)
                except Exception:v=""
            vals.append(clip(v,w))
        out.append(" ".join(vals))
    return out

def _make_text_pdf(lines, title="VCP Radar Export"):
    """Tiny dependency-free multi-page PDF using built-in Courier."""
    # A4 landscape points
    W,H=842,595
    left=24; top=H-30; bottom=24
    font_size=6.2
    leading=7.4
    per_page=max(1,int((top-bottom)/leading))
    pages=[lines[i:i+per_page] for i in range(0,len(lines),per_page)] or [["(empty report)"]]

    objects=[]
    # 1 catalog, 2 pages root, 3 font
    objects.append("<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(None)  # pages root filled later
    objects.append("<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")

    page_ids=[]
    for page_no, plines in enumerate(pages,1):
        content_id=len(objects)+2
        page_id=len(objects)+1
        page_ids.append(page_id)

        content=["BT",f"/F1 {font_size:.1f} Tf",f"{leading:.1f} TL",f"{left} {top} Td"]
        for idx,line in enumerate(plines):
            if idx:
                content.append("T*")
            content.append(f"({_pdf_escape(line)}) Tj")
        # page footer
        content += ["ET","BT","/F1 6 Tf",f"{W-105} 12 Td",
                    f"({_pdf_escape(f'Page {page_no}/{len(pages)}')}) Tj","ET"]
        stream="\n".join(content).encode("latin-1","replace")
        page_obj=f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {W} {H}] /Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>"
        content_obj=b"<< /Length "+str(len(stream)).encode()+b" >>\nstream\n"+stream+b"\nendstream"
        objects.append(page_obj)
        objects.append(content_obj)

    kids=" ".join(f"{pid} 0 R" for pid in page_ids)
    objects[1]=f"<< /Type /Pages /Count {len(page_ids)} /Kids [ {kids} ] >>"

    buf=io.BytesIO()
    buf.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets=[0]
    for i,obj in enumerate(objects,1):
        offsets.append(buf.tell())
        buf.write(f"{i} 0 obj\n".encode())
        if isinstance(obj,bytes):
            buf.write(obj)
        else:
            buf.write(str(obj).encode("latin-1","replace"))
        buf.write(b"\nendobj\n")
    xref=buf.tell()
    buf.write(f"xref\n0 {len(objects)+1}\n".encode())
    buf.write(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        buf.write(f"{off:010d} 00000 n \n".encode())
    buf.write(f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
    buf.seek(0)
    return buf

def build_v3_export_pdf():
    base=v3_result_dir()
    st=read_v3_status()
    lines=[]
    lines += [
        "VCP RADAR CLOUD - V3.2 ANALYSIS EXPORT",
        f"Generated UTC: {datetime.now(timezone.utc).isoformat()}",
        f"Mode: {_ascii_cell(st.get('mode',''))}   Status: {_ascii_cell(st.get('status',''))}",
        f"Stocks: {st.get('stocks',0)}   Processed: {st.get('processed',0)}",
        f"Result folder: {_ascii_cell(str(base))}",
        "",
    ]

    # Gate funnel
    p=base/"jlaw_v3_funnel_total.csv"
    lines += ["=== GATE FUNNEL ==="]
    if p.exists():
        try:
            d=pd.read_csv(p)
            if len(d):
                r=d.iloc[0]
                order=["checks","market","stage2","rs","sector","group","supply","leader","breakout"]
                lines.append(" -> ".join(f"{k.upper()}={int(r.get(k,0))}" for k in order))
        except Exception as e:
            lines.append(f"read error: {_ascii_cell(e)}")
    else:
        lines.append("(missing)")
    lines.append("")

    # Supply distribution
    lines += ["=== SUPPLY DISTRIBUTION ==="]
    p=base/"jlaw_v3_supply_stats.csv"
    if p.exists():
        try:
            d=pd.read_csv(p)
            cols=[c for c in ["metric","min","p10","p25","median","p75","p90","max"] if c in d.columns]
            if not cols: cols=list(d.columns)
            widths=[18]+[11]*(len(cols)-1)
            lines += _table_lines(d,cols,widths)
        except Exception as e: lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    # V3 summary
    lines += ["=== V3 BASE SUMMARY ==="]
    p=base/"jlaw_v3_summary.csv"
    if p.exists():
        try:
            d=pd.read_csv(p)
            cols=[c for c in ["grade","trades","win_rate","avg_return","avg_win","avg_loss","payoff_ratio","profit_factor","target_hit_rate","stop_rate","avg_hold"] if c in d.columns]
            widths=[7,7,9,10,10,10,9,8,10,9,9][:len(cols)]
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=True) if _c in ["win_rate","avg_return","avg_win","avg_loss","target_hit_rate","stop_rate"] else _fmt_num(v)) for c in cols}
            lines += _table_lines(d,cols,widths,transforms=tr)
        except Exception as e: lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    # Shadow summary - the most important table for model comparison
    lines += ["=== V3.2 SHADOW SUMMARY ==="]
    p=base/"jlaw_v3_shadow_summary.csv"
    if p.exists():
        try:
            d=pd.read_csv(p)
            cols=[c for c in ["mode","trades","win_rate","avg_return","profit_factor","avg_win","avg_loss","avg_hold","target_hit_rate","stop_rate","be_rate","avg_stop","avg_target","avg_mfe","avg_mae"] if c in d.columns]
            widths=[12,7,9,10,7,10,10,8,9,9,8,9,9,9,9][:len(cols)]
            pctcols={"win_rate","avg_return","avg_win","avg_loss","target_hit_rate","stop_rate","be_rate","avg_stop","avg_target","avg_mfe","avg_mae"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(d,cols,widths,transforms=tr)
        except Exception as e: lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")









    lines += ["=== V4.6.1 SELF CHECK ==="]
    try:
        dg=v461_diag()
        for k in ["status","diagnosis","engine_version","base_signal_rows","regime_tradeable_rows",
                  "score_ge70_rows","v46_pass_rows","v46_signal_rows","v46_trade_rows",
                  "stocks_with_v46_signal","stocks_with_v46_trade","signals_csv_exists","signals_csv_size",
                  "trades_csv_exists","trades_csv_size","summary_csv_exists","summary_csv_size","last_error"]:
            lines.append(f"{k}: {_ascii_cell(dg.get(k,''))}")
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.6 SHADOW GATE ENGINE ==="]
    try:
        e46=pd.DataFrame(v46_engine_summary())
        if not e46.empty:
            cols=["selection","trades","win_rate","avg_return","pf","avg_win","avg_loss","avg_hold","max_losing_streak"]
            widths=[12,7,9,10,7,10,10,8,8]
            pctcols={"win_rate","avg_return","avg_win","avg_loss"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(e46,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.5 CHAMPION CHALLENGER ==="]
    try:
        cc=pd.DataFrame(v45_champion_challenger())
        if not cc.empty:
            cols=["rule","trades","win_rate","avg_return","pf","max_losing_streak","old_trades","old_avg","old_pf","recent_trades","recent_avg","recent_pf"]
            widths=[22,7,9,10,7,8,8,9,7,8,9,7]
            pctcols={"win_rate","avg_return","old_avg","recent_avg"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(cc,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.5 WALKFORWARD VERDICT ==="]
    try:
        vv=pd.DataFrame(v45_walkforward_verdict())
        if not vv.empty:
            cols=["window","model","threshold","test_trades","test_win","test_avg","test_pf","verdict"]
            widths=[20,15,8,8,9,9,7,10]
            pctcols={"test_win","test_avg"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(vv,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.4 WALK FORWARD ==="]
    try:
        wf=pd.DataFrame(v44_walk_forward())
        if not wf.empty:
            cols=["window","model","threshold","train_trades","train_win","train_avg","train_pf","test_trades","test_win","test_avg","test_pf","test_max_losing_streak"]
            widths=[19,15,8,8,9,9,7,8,9,9,7,8]
            pctcols={"train_win","train_avg","test_win","test_avg"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(wf,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.4 RECENT 2022-2026 ==="]
    try:
        rv=pd.DataFrame(v44_recent_validation())
        if not rv.empty:
            cols=["rule","trades","win_rate","avg_return","pf","avg_win","avg_loss","max_losing_streak"]
            widths=[20,7,9,10,7,10,10,8]
            pctcols={"win_rate","avg_return","avg_win","avg_loss"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(rv,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.3 ADAPTIVE ELITE ==="]
    try:
        ad=pd.DataFrame(v43_adaptive_sweep())
        if not ad.empty:
            cols=["scope","selection","trades","win_rate","avg_return","pf","avg_win","avg_loss","max_losing_streak","cutoff"]
            widths=[15,11,7,9,10,7,10,10,8,8]
            pctcols={"win_rate","avg_return","avg_win","avg_loss"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(ad,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.3 ADAPTIVE TOP50 YEAR STABILITY ==="]
    try:
        ys=pd.DataFrame(v43_year_stability())
        if not ys.empty:
            cols=["period","trades","win_rate","avg_return","pf","max_losing_streak"]
            widths=[12,7,9,10,7,8]
            pctcols={"win_rate","avg_return"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(ys,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.2 REGIME LAB ==="]
    try:
        rg=pd.DataFrame(v42_regime_summary())
        if not rg.empty:
            cols=["regime","selection","trades","win_rate","avg_return","pf","max_losing_streak"]
            widths=[14,28,7,9,10,7,8]
            pctcols={"win_rate","avg_return"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(rg,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.2 SCORE ABLATION ==="]
    try:
        ab=pd.DataFrame(v42_ablation_best())
        if not ab.empty:
            cols=["drop","selection","trades","win_rate","avg_return","pf","delta_avg","delta_pf","max_losing_streak"]
            widths=[14,10,7,9,10,7,10,8,8]
            pctcols={"win_rate","avg_return","delta_avg"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(ab,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    # V4.1 Sweet Spot nonlinear factor sweep
    lines += ["=== V4.1 SWEET SPOT SWEEP ==="]
    try:
        vs=pd.DataFrame(v41_sweet_spot_sweep())
        if not vs.empty:
            cols=["scheme","selection","trades","win_rate","avg_return","pf","avg_win","avg_loss","avg_hold","max_losing_streak","cutoff"]
            cols=[c for c in cols if c in vs.columns]
            widths=[18,11,7,9,10,7,10,10,8,8,8][:len(cols)]
            pctcols={"win_rate","avg_return","avg_win","avg_loss"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(vs,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.1 TOP50 PERIOD CHECK ==="]
    try:
        rg=pd.DataFrame(v41_regime_check())
        if not rg.empty:
            cols=["scheme","period","trades","win_rate","avg_return","pf","max_losing_streak"]
            widths=[18,9,7,9,10,7,8]
            pctcols={"win_rate","avg_return"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(rg,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    # V4 Elite weight sweep
    lines += ["=== V4 ELITE WEIGHT SWEEP ==="]
    try:
        vs=pd.DataFrame(v4_weight_sweep())
        if not vs.empty:
            cols=["scheme","selection","trades","win_rate","avg_return","pf","avg_win","avg_loss","avg_hold","max_losing_streak","cutoff"]
            cols=[c for c in cols if c in vs.columns]
            widths=[16,11,7,9,10,7,10,10,8,8,8][:len(cols)]
            pctcols={"win_rate","avg_return","avg_win","avg_loss"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(vs,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    # Full signal list
    lines += ["=== ALL BREAKOUT SIGNALS ==="]
    p=base/"jlaw_v3_signals.csv"
    if p.exists():
        try:
            d=pd.read_csv(p)
            cols=[c for c in ["ticker","date","grade","entry","pivot","volratio","rs","sector_score","leader_score","supply_score","quality"] if c in d.columns]
            widths=[7,11,5,9,9,8,5,7,7,7,7][:len(cols)]
            lines += _table_lines(d,cols,widths)
        except Exception as e: lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    # Full executed trades
    lines += ["=== ALL BASE TRADES ==="]
    p=base/"jlaw_v3_trades.csv"
    if p.exists():
        try:
            d=pd.read_csv(p)
            cols=[c for c in ["ticker","date","grade","entry","pivot","volratio","rs","supply_score","quality","return_pct","reason","hold_days","mfe","mae"] if c in d.columns]
            widths=[7,11,5,9,9,7,5,7,7,9,13,7,9,9][:len(cols)]
            pctcols={"return_pct","mfe","mae"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(d,cols,widths,transforms=tr)
        except Exception as e: lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    # Full shadow trades: large, but intentionally complete for sharing/analysis.
    lines += ["=== ALL SHADOW TRADES (COMPLETE) ==="]
    p=base/"jlaw_v3_shadow_trades.csv"
    if p.exists():
        try:
            d=pd.read_csv(p)
            cols=[c for c in ["ticker","date","mode","volratio","atr_pct","stop_pct","target_pct","mfe","mae","hold_days","reason","return_pct","be_armed"] if c in d.columns]
            widths=[7,11,12,7,8,8,8,8,8,7,13,9,5][:len(cols)]
            pctcols={"atr_pct","stop_pct","target_pct","mfe","mae","return_pct"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(d,cols,widths,transforms=tr)
        except Exception as e: lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")
    lines.append("END OF EXPORT")
    return _make_text_pdf(lines)


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

details.section-fold{
  margin-top:14px;
  border:1px solid #e6eaf0;
  border-radius:16px;
  background:#fff;
  overflow:hidden;
}
details.section-fold > summary{
  list-style:none;
  cursor:pointer;
  padding:16px 18px;
  font-weight:800;
  font-size:20px;
  display:flex;
  align-items:center;
  justify-content:space-between;
  background:#f8fafc;
}
details.section-fold > summary::-webkit-details-marker{display:none}
details.section-fold > summary:after{
  content:"＋";
  font-size:24px;
  font-weight:700;
  color:#64748b;
}
details.section-fold[open] > summary:after{content:"－"}
details.section-fold .fold-body{padding:14px 18px 18px}

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
<a href="/v3/export-pdf" style="text-decoration:none"><button type="button" class="alt" {% if v3_status!="done" %}disabled style="opacity:.45"{% endif %}>导出PDF报告</button></a>
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
<details class="section-fold"><summary>Supply 分布诊断</summary><div class="fold-body"><b>Supply 分布诊断：</b>
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

{% if v3_trades %}
<details class="section-fold"><summary>突破交易逐笔诊断</summary><div class="fold-body"><h3 style="margin-top:0">突破交易逐笔诊断</h3>
<div class="muted" style="margin-bottom:8px">直接读取刚才已经完成的回测结果，不需要重新跑。重点看这几笔为什么在约5天内止损。</div>
<div class="scroll"><table style="min-width:1400px">
<tr>
<th>股票</th><th>日期</th><th>等级</th><th>入场</th><th>Pivot</th><th>距Pivot</th>
<th>量比</th><th>RS</th><th>板块</th><th>Leader</th><th>Supply</th><th>质量</th>
<th>MFE</th><th>MAE</th><th>持有</th><th>退出</th><th>收益</th><th>诊断</th>
</tr>
{% for r in v3_trades %}
<tr>
<td><b>{{r.ticker}}</b></td>
<td>{{r.date}}</td>
<td>{{r.grade}}</td>
<td>{{"%.2f"|format(r.entry or 0)}}</td>
<td>{{"%.2f"|format(r.pivot or 0)}}</td>
<td>{{"%.2f%%"|format(r.extension_pct or 0)}}</td>
<td>{{"%.2f"|format(r.volratio or 0)}}</td>
<td>{{"%.0f"|format(r.rs or 0)}}</td>
<td>{{"%.0f"|format(r.sector_score or 0)}}</td>
<td>{{"%.0f"|format(r.leader_score or 0)}}</td>
<td>{{"%.0f"|format(r.supply_score or 0)}}</td>
<td>{{"%.0f"|format(r.quality or 0)}}</td>
<td>{{"%.2f%%"|format(r.mfe_pct or 0)}}</td>
<td>{{"%.2f%%"|format(r.mae_pct or 0)}}</td>
<td>{{"%.0f"|format(r.hold_days or 0)}}天</td>
<td>{{r.reason}}</td>
<td><b>{{"%.2f%%"|format(r.return_display or 0)}}</b></td>
<td>{{r.diagnosis}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}

</div></details>
{% if v3_signals and (v3_signals|length) > (v3_trades|length) %}
<div style="margin-top:14px;background:#f6f8fb;padding:12px;border-radius:12px">
<b>信号数多于实际交易：</b>本轮有 {{v3_signals|length}} 个突破信号、{{v3_trades|length}} 笔实际交易。
这是因为同一股票在上一笔尚未退出时出现的重复信号会被去重，不代表数据丢失。
</div>
{% endif %}


{% if v3_shadow %}
<details class="section-fold" open><summary>V3.2 Shadow Test · 同一信号并排测试</summary><div class="fold-body"><h3 style="margin-top:0">V3.2 Shadow Test · 同一信号并排测试</h3>
<div class="muted" style="margin-bottom:8px">
不改变前面的 Market / Stage 2 / RS / Group / Supply 筛选，只比较突破量门槛、Breakeven 利润保护和 ATR 自适应止损/止盈。
</div>
<div class="scroll"><table style="min-width:1350px">
<tr><th>模型</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>平均持有</th><th>目标命中</th><th>止损率</th><th>BE退出</th><th>平均止损</th><th>平均目标</th><th>平均MFE</th></tr>
{% for r in v3_shadow %}
<tr>
<td><b>{{r.mode}}</b></td>
<td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.profit_factor or 0)}}</td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{"%.1f"|format(r.avg_hold or 0)}}天</td>
<td>{{"%.1f%%"|format((r.target_hit_rate or 0)*100)}}</td>
<td>{{"%.1f%%"|format((r.stop_rate or 0)*100)}}</td>
<td>{{"%.1f%%"|format((r.be_rate or 0)*100)}}</td>
<td>{{"%.1f%%"|format((r.avg_stop or 0)*100)}}</td>
<td>{{"%.1f%%"|format((r.avg_target or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_mfe or 0)*100)}}</td>
</tr>
{% endfor %}
</table></div>
<div class="muted" style="margin-top:8px">
BASE=原始量比≥1.2/-7%/+30%；VOL15/VOL18=提高突破量门槛；BE8/BE10=浮盈达到8%/10%后止损提到成本；ATR=按股票自身ATR动态调整止损和目标；组合模型同时使用两层规则。
</div>

{% if v3_shadow_trades %}
</div></details>
<details class="section-fold"><summary>Shadow Test · 逐笔结果</summary><div class="fold-body"><h3 style="margin-top:0">Shadow Test · 逐笔结果</h3>
<div class="muted" style="margin-bottom:8px">
这张表直接解释为什么某个模型改善/没有改善。尤其看 AVGO 的 BE 是否已触发，以及 ATR 到底把哪一笔变成盈利。
</div>
<div class="scroll"><table style="min-width:1350px">
<tr><th>股票</th><th>模型</th><th>量比</th><th>ATR%</th><th>止损%</th><th>目标%</th><th>MFE</th><th>MAE</th><th>持有</th><th>BE已触发</th><th>退出</th><th>收益</th></tr>
{% for r in v3_shadow_trades %}
<tr>
<td><b>{{r.ticker}}</b></td><td>{{r.mode}}</td>
<td>{{"%.2f"|format(r.volratio or 0)}}</td>
<td>{{"%.2f%%"|format(r.atr_display or 0)}}</td>
<td>{{"%.1f%%"|format(r.stop_display or 0)}}</td>
<td>{{"%.1f%%"|format(r.target_display or 0)}}</td>
<td>{{"%.2f%%"|format(r.mfe_display or 0)}}</td>
<td>{{"%.2f%%"|format(r.mae_display or 0)}}</td>
<td>{{"%.0f"|format(r.hold_days or 0)}}天</td>
<td>{{"是" if r.be_armed else "否"}}</td>
<td>{{r.reason}}</td>
<td><b>{{"%.2f%%"|format(r.return_display or 0)}}</b></td>
</tr>
{% endfor %}
</table></div>
{% endif %}
</div></details>
{% endif %}









{% if v461_diag %}
<details class="section-fold" open>
<summary>V4.6.1 Self Check · 发动机自检</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.6.1 Self Check</h3>
<div style="margin:8px 0 12px">
<b>状态：</b> {{v461_diag.status}}<br>
<b>诊断：</b> {{v461_diag.diagnosis}}
</div>
<div class="scroll"><table style="min-width:1080px">
<tr>
<th>Engine</th><th>BASE signals</th><th>Regime可交易</th><th>Score≥70</th><th>Pass</th>
<th>V4.6 signals</th><th>V4.6 trades</th><th>有信号股票</th><th>有交易股票</th>
<th>signals CSV</th><th>trades CSV</th><th>错误</th>
</tr>
<tr>
<td><b>{{v461_diag.engine_version}}</b></td>
<td>{{v461_diag.base_signal_rows}}</td>
<td>{{v461_diag.regime_tradeable_rows}}</td>
<td>{{v461_diag.score_ge70_rows}}</td>
<td>{{v461_diag.v46_pass_rows}}</td>
<td>{{v461_diag.v46_signal_rows}}</td>
<td>{{v461_diag.v46_trade_rows}}</td>
<td>{{v461_diag.stocks_with_v46_signal}}</td>
<td>{{v461_diag.stocks_with_v46_trade}}</td>
<td>{{v461_diag.signals_csv_exists}} / {{v461_diag.signals_csv_size}} B</td>
<td>{{v461_diag.trades_csv_exists}} / {{v461_diag.trades_csv_size}} B</td>
<td>{{v461_diag.last_error}}</td>
</tr>
</table></div>
</div>
</details>
{% endif %}

{% if v46_engine %}
<details class="section-fold" open>
<summary>V4.6 Shadow Gate · 发动机真实重跑结果</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.6 Shadow Gate</h3>
<div class="muted" style="margin-bottom:10px">
这是发动机重新逐日扫描后产生的 Challenger 交易，不再只是对旧385笔做 App 端排序。
V3 BASE 仍然保留，不会被覆盖。
</div>
<div class="scroll"><table style="min-width:900px">
<tr><th>范围</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>平均持有</th><th>最大连亏</th></tr>
{% for r in v46_engine %}
<tr><td><b>{{r.selection}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td><b>{{"%.2f"|format(r.pf or 0)}}</b></td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{"%.1f"|format(r.avg_hold or 0)}}天</td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v45_cc %}
<details class="section-fold" open>
<summary>V4.5 Champion vs Challenger · 是否真的赢过基准</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.5 Champion vs Challenger</h3>
<div class="muted" style="margin-bottom:10px">
把 V3 BASE 保留为 Champion，所有新规则只是 Challenger。新规则只有在滚动验证也稳定后才有资格替换基准。
</div>
<div style="margin:8px 0 14px"><b>研究结论：</b> {{v45_decision.status}} — {{v45_decision.reason}}</div>
<div class="scroll"><table style="min-width:1260px">
<tr><th>规则</th><th>总笔数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th>
<th>≤2021笔数</th><th>≤2021平均</th><th>≤2021 PF</th>
<th>2022+笔数</th><th>2022+平均</th><th>2022+ PF</th></tr>
{% for r in v45_cc %}
<tr>
<td><b>{{r.rule}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td>
<td>{{r.old_trades}}</td><td>{{"%.2f%%"|format((r.old_avg or 0)*100)}}</td><td>{{"%.2f"|format(r.old_pf or 0)}}</td>
<td>{{r.recent_trades}}</td><td><b>{{"%.2f%%"|format((r.recent_avg or 0)*100)}}</b></td><td><b>{{"%.2f"|format(r.recent_pf or 0)}}</b></td>
</tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v45_wf %}
<details class="section-fold" open>
<summary>V4.5 Walk-Forward Verdict · PASS / FAIL / 样本过小</summary>
<div class="fold-body">
<div class="scroll"><table style="min-width:1080px">
<tr><th>窗口</th><th>模型</th><th>门槛</th><th>测试笔数</th><th>测试胜率</th><th>测试平均</th><th>测试PF</th><th>结论</th></tr>
{% for r in v45_wf %}
<tr>
<td><b>{{r.window}}</b></td><td>{{r.model}}</td><td>{{r.threshold}}</td><td>{{r.test_trades}}</td>
<td>{{"%.1f%%"|format((r.test_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.test_avg or 0)*100)}}</td>
<td>{{"%.2f"|format(r.test_pf or 0)}}</td><td><b>{{r.verdict}}</b></td>
</tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v44_walk %}
<details class="section-fold" open>
<summary>V4.4 Walk-Forward Lab · 滚动验证</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.4 Walk-Forward Lab</h3>
<div class="muted" style="margin-bottom:10px">
每一轮只用过去数据挑“模型 + 分数门槛”，随后冻结规则，再测试后面的年份。
注意：候选模型本身来自前几轮研究，所以这是强度/稳定性检查，不是完全未碰过的终极样本外测试。
</div>
<div class="scroll"><table style="min-width:1180px">
<tr><th>窗口</th><th>训练选中模型</th><th>门槛</th><th>训练笔数</th><th>训练胜率</th><th>训练平均</th><th>训练PF</th><th>测试笔数</th><th>测试胜率</th><th>测试平均</th><th>测试PF</th><th>测试最大连亏</th></tr>
{% for r in v44_walk %}
<tr>
<td><b>{{r.window}}</b></td><td>{{r.model}}</td><td>{{r.threshold}}</td><td>{{r.train_trades}}</td>
<td>{{"%.1f%%"|format((r.train_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.train_avg or 0)*100)}}</td>
<td>{{"%.2f"|format(r.train_pf or 0)}}</td>
<td>{{r.test_trades}}</td>
<td><b>{{"%.1f%%"|format((r.test_win or 0)*100)}}</b></td>
<td><b>{{"%.2f%%"|format((r.test_avg or 0)*100)}}</b></td>
<td><b>{{"%.2f"|format(r.test_pf or 0)}}</b></td>
<td>{{r.test_max_losing_streak}}</td>
</tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v44_recent %}
<details class="section-fold" open>
<summary>V4.4 Recent Era · 2022-2026 固定规则对比</summary>
<div class="fold-body">
<div class="scroll"><table style="min-width:860px">
<tr><th>规则</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>最大连亏</th></tr>
{% for r in v44_recent %}
<tr><td><b>{{r.rule}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v44_bands %}
<details class="section-fold">
<summary>V4.4 Score Bands · 分数区间是否真的越高越好</summary>
<div class="fold-body">
<div class="scroll"><table style="min-width:900px">
<tr><th>模型</th><th>分数区间</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v44_bands %}
<tr><td><b>{{r.model}}</b></td><td>{{r.band}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</td>
<td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v43_rows %}
<details class="section-fold" open>
<summary>V4.3 Adaptive Elite · Regime 自适应精选</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.3 Adaptive Elite</h3>
<div class="muted" style="margin-bottom:10px">
STRONG_BULL 与 BULL 不再使用同一套评分。TRANSITION / DEFENSIVE 在研究层默认 BLOCK。
这仍然是对现有385笔的再排序研究，不是完整 Gate 重跑。
</div>

<div class="scroll"><table style="min-width:980px">
<tr><th>范围</th><th>选择</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>最大连亏</th><th>最低分</th></tr>
{% for r in v43_rows %}
<tr>
<td><b>{{r.scope}}</b></td><td>{{r.selection}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{r.max_losing_streak}}</td>
<td>{{"%.1f"|format(r.cutoff or 0)}}</td>
</tr>
{% endfor %}
</table></div>

{% if v43_years %}
<h4 style="margin-top:16px">Adaptive Top50 年代稳定性</h4>
<div class="scroll"><table style="min-width:760px">
<tr><th>时期</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v43_years %}
<tr><td>{{r.period}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
{% endif %}

<h4 style="margin-top:16px">Regime 策略</h4>
<div class="scroll"><table style="min-width:860px">
<tr><th>Regime</th><th>主因子</th><th>辅助</th><th>动作</th></tr>
{% for r in v43_policy %}
<tr><td><b>{{r.regime}}</b></td><td>{{r.primary}}</td><td>{{r.secondary}}</td><td>{{r.action}}</td></tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v42_regime %}
<details class="section-fold" open>
<summary>V4.2 Regime Lab · 市场环境 × Sweet Spot</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.2 Regime Lab</h3>
<div class="muted" style="margin-bottom:10px">
按每笔交易当时的 SPY 日线状态划分 STRONG_BULL / BULL / TRANSITION / DEFENSIVE。
只使用当时及以前的数据，不读取未来行情。
</div>
<div class="scroll"><table style="min-width:900px">
<tr><th>Regime</th><th>选择</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v42_regime %}
<tr>
<td><b>{{r.regime}}</b></td><td>{{r.selection}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td>
</tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v42_ablation %}
<details class="section-fold">
<summary>V4.2 Score Ablation · 逐个拿掉 factor</summary>
<div class="fold-body">
<h3 style="margin-top:0">ROBUST_BALANCED 逐因子消融</h3>
<div class="muted" style="margin-bottom:10px">
这是“评分层”的消融：分别拿掉 Volume / Quality / Sector / Leader / Supply，再重排 Top30/50/100/150。
注意：它不是完整 Gate 消融，因为被旧 Gate 拒绝的候选不在当前385笔交易表里；完整 Gate 消融以后需要发动机重新跑。
</div>
<div class="scroll"><table style="min-width:980px">
<tr><th>版本</th><th>选择</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均收益变化</th><th>PF变化</th><th>最大连亏</th></tr>
{% for r in v42_ablation %}
<tr>
<td><b>{{r.drop}}</b></td><td>{{r.selection}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td>
<td>{{"%+.2f%%"|format((r.delta_avg or 0)*100)}}</td>
<td>{{"%+.2f"|format(r.delta_pf or 0)}}</td>
<td>{{r.max_losing_streak}}</td>
</tr>
{% endfor %}
</table></div>
</div>
</details>
{% endif %}

{% if v41_rows %}
<details class="section-fold" open>
<summary>V4.1 Sweet Spot Engine · 非线性区间测试</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.1 Sweet Spot Engine</h3>
<div class="muted" style="margin-bottom:10px">
这次不再假设 factor 越高越好，而是按“最佳区间”打分。重点测试 Volume、Quality、Sector、Leader、Supply 的甜蜜区间组合。
</div>

{% if v41_best %}
<h4>不同精选数量下，目前最佳方案</h4>
<div class="scroll"><table style="min-width:980px">
<tr><th>精选数量</th><th>方案</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>最大连亏</th><th>最低分</th></tr>
{% for r in v41_best %}
<tr>
<td><b>{{r.selection}}</b></td><td><b>{{r.scheme}}</b></td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{r.max_losing_streak}}</td>
<td>{{"%.1f"|format(r.cutoff or 0)}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}

<h4 style="margin-top:16px">全部 Sweet Spot 测试</h4>
<div class="scroll"><table style="min-width:1160px">
<tr><th>方案</th><th>选择</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>平均持有</th><th>最大连亏</th><th>门槛分</th></tr>
{% for r in v41_rows %}
<tr>
<td><b>{{r.scheme}}</b></td><td>{{r.selection}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{"%.1f"|format(r.avg_hold or 0)}}天</td>
<td>{{r.max_losing_streak}}</td>
<td>{{"%.1f"|format(r.cutoff or 0)}}</td>
</tr>
{% endfor %}
</table></div>

{% if v41_regime %}
<h4 style="margin-top:16px">Top50 年代稳定性检查</h4>
<div class="scroll"><table style="min-width:760px">
<tr><th>方案</th><th>时期</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v41_regime %}
<tr>
<td><b>{{r.scheme}}</b></td><td>{{r.period}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}
</div>
</details>
{% endif %}

{% if v4_sweep %}
<details class="section-fold" open>
<summary>V4 Elite Score · 权重压力测试</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4 Elite Score · 多套权重并排测试</h3>
<div class="muted" style="margin-bottom:10px">
不改原V3发动机、不重新回测。直接用已有交易在入场前可见的 Supply、Setup/Quality、Volume、Leader、Sector、RS 重新打分。
这是参数研究，不是最终规则；同一历史样本上挑权重会有过拟合风险，最终还要做年份/样本外验证。
</div>

{% if v4_best %}
<h4>每个交易数量下，目前表现最佳的权重方案</h4>
<div class="scroll"><table style="min-width:980px">
<tr><th>精选数量</th><th>最佳权重</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>最大连亏</th><th>最低Elite分</th></tr>
{% for r in v4_best %}
<tr>
<td><b>{{r.selection}}</b></td>
<td><b>{{r.scheme}}</b></td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{r.max_losing_streak}}</td>
<td>{{"%.1f"|format(r.cutoff or 0)}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}

<h4 style="margin-top:16px">全部权重方案</h4>
<div class="scroll"><table style="min-width:1180px">
<tr><th>方案</th><th>选择</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>平均盈利</th><th>平均亏损</th><th>平均持有</th><th>最大连亏</th><th>门槛分</th></tr>
{% for r in v4_sweep %}
<tr>
<td><b>{{r.scheme}}</b></td><td>{{r.selection}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td>{{"%.2f"|format(r.pf or 0)}}</td>
<td>{{"%.2f%%"|format((r.avg_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_loss or 0)*100)}}</td>
<td>{{"%.1f"|format(r.avg_hold or 0)}}天</td>
<td>{{r.max_losing_streak}}</td>
<td>{{"%.1f"|format(r.cutoff or 0)}}</td>
</tr>
{% endfor %}
</table></div>

<div class="muted" style="margin-top:10px">
SUPPLY_HEAVY=Supply 30%；VOLUME_HEAVY=Volume 30%；SUPPLY_VOLUME=Supply/Volume 各27%；
SETUP_LEADER=Setup 24% + Leader 22%；BALANCED_ELITE=Supply25/Setup20/Volume20/Leader15/Sector10/RS10。
</div>
</div>
</details>
{% endif %}

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
        v3_processed=read_v3_status().get("processed",0),v3_mode=read_v3_status().get("mode","full"),v3_results=v3_rows(),v3_diag=v3_diagnostics(),v3_trades=v3_trade_details(),v3_signals=v3_signal_details(),v3_shadow=v3_shadow_results(),v3_shadow_trades=v3_shadow_trade_details(),v4_sweep=v4_weight_sweep(),v4_best=v4_best_by_size(),v41_rows=v41_sweet_spot_sweep(),v41_best=v41_best_rows(),v41_regime=v41_regime_check(),v42_regime=v42_regime_summary(),v42_regime_best=v42_regime_best(),v42_ablation=v42_ablation_best(),v43_rows=v43_adaptive_sweep(),v43_years=v43_year_stability(),v43_policy=v43_factor_policy(),v44_walk=v44_walk_forward(),v44_bands=v44_score_band_check(),v44_recent=v44_recent_validation(),v45_cc=v45_champion_challenger(),v45_wf=v45_walkforward_verdict(),v45_decision=v45_research_decision(),v46_engine=v46_engine_summary(),v461_diag=v461_diag(),
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

@APP.get("/v3/export-pdf")
def v3_export_pdf():
    try:
        pdf=build_v3_export_pdf()
        mode=read_v3_status().get("mode","full")
        stamp=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        name=f"VCP_Radar_V3_2_{mode}_{stamp}.pdf"
        return send_file(pdf,mimetype="application/pdf",as_attachment=True,download_name=name)
    except Exception as e:
        return f"PDF export failed: {e}",500

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
