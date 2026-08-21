#!/usr/bin/env python3
# J Law Pure Backtest Auto V1.1
# Reuses /data/market cache from VCP Radar; downloads only missing histories from Tiingo.
import os, time, json
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import pandas as pd

from jlaw_backtest_v1 import run

TOKEN = os.environ.get("TIINGO_TOKEN","").strip()
DATA_DIR = Path(os.environ.get("DATA_DIR","/data"))
MARKET_DIR = DATA_DIR / "market"
OUT_DIR = DATA_DIR / "jlaw_backtest_results"
MARKET_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Same stock universe as the current radar, but J Law teaching excludes Utilities.
UNIVERSE = sorted(set("""
AAPL MSFT NVDA AVGO ORCL AMD CRM ADBE QCOM TXN INTU AMAT LRCX KLAC MU PANW CRWD NOW PLTR
AMZN TSLA HD LOW BKNG TJX SBUX MCD NKE CMG ORLY AZO ROST MAR
GOOGL META NFLX DIS T VZ TMUS CHTR
JPM BAC WFC GS MS C AXP SCHW BLK SPGI CME ICE COF USB PNC
LLY UNH JNJ ABBV MRK TMO ABT DHR AMGN GILD ISRG VRTX REGN BSX MDT SYK
WMT COST PG KO PEP PM MO CL MDLZ KMB GIS
GE CAT DE ETN PH HON RTX LMT NOC GD UPS FDX UNP CSX WM RSG UBER
XOM CVX COP EOG SLB MPC PSX VLO OXY
LIN APD SHW ECL FCX NEM NUE
AMT PLD EQIX WELL SPG O
""".split()))
# Intentionally excluded Utilities: NEE SO DUK AEP SRE EXC.
# ETFs are not part of this stock universe. SPY is benchmark only.

def cache_path(t): return MARKET_DIR / f"{t}.csv"

def cache_ok(t):
    p=cache_path(t)
    if not p.exists(): return False
    try:
        d=pd.read_csv(p,usecols=["Date"])
        # Need enough history for 200MA + 52-week high/low + multi-year backtest.
        return len(d) >= 700
    except Exception:
        return False

def tiingo_download(ticker,start="2006-01-01",retries=5):
    if not TOKEN:
        raise RuntimeError("TIINGO_TOKEN 未设置，而且本地缓存不足。")
    url=f"https://api.tiingo.com/tiingo/daily/{quote(ticker)}/prices?startDate={start}&resampleFreq=daily&token={quote(TOKEN)}"
    gap=float(os.environ.get("BACKTEST_TIINGO_GAP_SECONDS","12"))
    cooldown=int(os.environ.get("BACKTEST_429_COOLDOWN_SECONDS","90"))
    for attempt in range(1,retries+1):
        req=Request(url,headers={"Accept":"application/json","User-Agent":"VCP-Radar-JLaw-Backtest/1.1"})
        try:
            with urlopen(req,timeout=60) as r:
                arr=json.loads(r.read().decode("utf-8"))
            if not isinstance(arr,list) or not arr:
                raise RuntimeError(f"{ticker}: Tiingo 没有返回历史数据")
            d=pd.DataFrame(arr)
            out=pd.DataFrame({
                "Date":pd.to_datetime(d["date"],utc=True,errors="coerce").dt.tz_convert(None),
                "Open":d["adjOpen"] if "adjOpen" in d else d["open"],
                "High":d["adjHigh"] if "adjHigh" in d else d["high"],
                "Low":d["adjLow"] if "adjLow" in d else d["low"],
                "Close":d["adjClose"] if "adjClose" in d else d["close"],
                "Volume":d["adjVolume"] if "adjVolume" in d else d["volume"],
            }).dropna().sort_values("Date")
            out.to_csv(cache_path(ticker),index=False)
            time.sleep(gap)
            return
        except HTTPError as e:
            if e.code==429 and attempt<retries:
                print(f"{ticker}: 429限流，等待{cooldown}秒后重试")
                time.sleep(cooldown); cooldown=min(cooldown*2,600); continue
            raise
        except (URLError,TimeoutError) as e:
            if attempt<retries:
                time.sleep(20*attempt); continue
            raise

def ensure_history():
    need=["SPY"]+UNIVERSE
    missing=[t for t in need if not cache_ok(t)]
    print(f"历史缓存：{len(need)-len(missing)}/{len(need)} 已可用")
    if missing:
        print(f"需要补下载 {len(missing)} 只：", " ".join(missing))
    for i,t in enumerate(missing,1):
        print(f"[{i}/{len(missing)}] 下载 {t}")
        tiingo_download(t)
    return [str(cache_path(t)) for t in UNIVERSE if cache_ok(t)]

def write_status(message, **extra):
    p=OUT_DIR/"status.json"
    x={"message":message,"time":pd.Timestamp.utcnow().isoformat(),**extra}
    p.write_text(json.dumps(x,ensure_ascii=False,indent=2),encoding="utf-8")

def main():
    write_status("preparing")
    stocks=ensure_history()
    if not cache_ok("SPY"):
        raise RuntimeError("SPY历史数据仍不可用")
    print(f"\n开始 J Law Pure Backtest：{len(stocks)} 只股票")
    print("统一出口：-7% stop；+15/+20/+25/+30%；最长60交易日")
    print("Utilities 已排除；ETF 不在股票池。")
    print("注意：Sales Growth/Acceleration 仍需 point-in-time 基本面数据，当前不会伪造。")
    write_status("running",stocks=len(stocks))
    run(str(cache_path("SPY")), stocks, str(OUT_DIR))
    write_status("done",stocks=len(stocks),
                 summary=str(OUT_DIR/"jlaw_summary.csv"),
                 trades=str(OUT_DIR/"jlaw_trades.csv"),
                 signals=str(OUT_DIR/"jlaw_signals.csv"))
    print("\n完成。结果保存在：", OUT_DIR)
    print("查看：jlaw_summary.csv / jlaw_trades.csv / jlaw_signals.csv")

if __name__=="__main__":
    main()
