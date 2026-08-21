#!/usr/bin/env python3
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

TARGETS=[0.15,0.20,0.25,0.30]
STOP=0.07
MAX_HOLD=60

def load_ohlcv(path):
    d=pd.read_csv(path); d.columns=[str(c).strip().lower() for c in d.columns]
    dc="date" if "date" in d.columns else "datetime"
    d=d[[dc,"open","high","low","close","volume"]].rename(columns={dc:"date"})
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    for c in ["open","high","low","close","volume"]: d[c]=pd.to_numeric(d[c],errors="coerce")
    return d.dropna().sort_values("date").drop_duplicates("date").reset_index(drop=True)

def indicators(d):
    x=d.copy()
    for n in [20,50,150,200]: x[f"ma{n}"]=x.close.rolling(n).mean()
    x["ma200_20ago"]=x.ma200.shift(20); x["vol50"]=x.volume.rolling(50).mean()
    x["high52"]=x.high.rolling(252).max(); x["low52"]=x.low.rolling(252).min()
    x["above52low"]=x.close/x.low52-1; x["below52high"]=x.close/x.high52-1
    return x

def add_rs(d,spy):
    z=d.merge(spy[["date","close"]].rename(columns={"close":"spy"}),on="date",how="left")
    z["rsline"]=z.close/z.spy; z["rsline_up"]=z.rsline>z.rsline.shift(20)
    ex=0
    weights=[(63,.25),(126,.35),(252,.40)]
    valid=pd.Series(0.0,index=z.index); wsum=pd.Series(0.0,index=z.index)
    for n,w in weights:
        e=(z.close/z.close.shift(n)-1)-(z.spy/z.spy.shift(n)-1)
        valid=valid+e.fillna(0)*w; wsum=wsum+e.notna().astype(float)*w
    z["rs_proxy"]=(50+(valid/wsum.replace(0,np.nan))*180).clip(0,100)
    return z

def detect(raw,spy,ticker):
    d=add_rs(indicators(raw),spy); sig=[]
    for i in range(260,len(d)):
        r=d.iloc[i]
        vals=[r.ma50,r.ma150,r.ma200,r.ma200_20ago,r.above52low,r.below52high,r.rs_proxy,r.vol50]
        if any(pd.isna(v) for v in vals): continue
        # J Law Stage 2 strong screen confirmed from the teaching screenshots.
        if not (r.close>=12 and r.close>r.ma50>r.ma150>r.ma200 and r.ma200>r.ma200_20ago
                and r.above52low>=.30 and r.below52high>=-.15 and r.rs_proxy>=89
                and r.vol50>=25000):
            continue
        base=d.iloc[i-60:i]; thirds=np.array_split(base,3); c=[]
        for q in thirds:
            hi=float(q.high.max()); lo=float(q.low.min()); c.append((hi-lo)/lo if lo>0 else np.nan)
        if any(pd.isna(c) for c in c): continue
        c1,c2,c3=c
        if not (c2<c1 and c3<c2 and c3<.10): continue
        prior=float(base.volume.iloc[:40].mean()); recent=float(base.volume.iloc[-10:].mean())
        vdu=recent/prior if prior>0 else np.nan
        if not np.isfinite(vdu) or vdu>=.85: continue
        pivot=float(d.high.iloc[i-20:i].max())
        if not (d.close.iloc[i-1]<=pivot and r.close>pivot): continue
        extension=float(r.close/pivot-1)
        if extension>.05: continue
        vr=float(r.volume/r.vol50) if r.vol50>0 else np.nan
        sig.append(dict(ticker=ticker,date=r.date,idx=i,entry=float(r.close),pivot=pivot,
                        extension=extension,c1=c1,c2=c2,c3=c3,vdu=vdu,vol_ratio=vr,
                        hvb_120=bool(vr>=1.20),rs_proxy=float(r.rs_proxy),
                        rsline_up=bool(r.rsline_up)))
    return d,pd.DataFrame(sig)

def backtest(d,sigs,target):
    rows=[]; last_exit=-1
    for _,s in sigs.iterrows():
        i=int(s.idx)
        if i<=last_exit: continue
        entry=float(s.entry); stop=entry*(1-STOP); take=entry*(1+target)
        exit_px=exit_i=reason=None; mfe=0.; mae=0.
        for j in range(i+1,min(i+1+MAX_HOLD,len(d))):
            o,h,l=map(float,[d.loc[j,"open"],d.loc[j,"high"],d.loc[j,"low"]])
            mfe=max(mfe,h/entry-1); mae=min(mae,l/entry-1)
            if o<=stop: exit_px,exit_i,reason=o,j,"STOP_GAP"; break
            if o>=take: exit_px,exit_i,reason=o,j,"TARGET_GAP"; break
            # Conservative when daily bar could have touched both.
            if l<=stop: exit_px,exit_i,reason=stop,j,"STOP"; break
            if h>=take: exit_px,exit_i,reason=take,j,"TARGET"; break
        if exit_px is None:
            exit_i=min(i+MAX_HOLD,len(d)-1); exit_px=float(d.loc[exit_i,"close"]); reason="TIME"
        row={k:s[k] for k in s.index if k!="idx"}
        row.update(target_pct=target,stop_pct=STOP,exit_date=d.loc[exit_i,"date"],exit=exit_px,
                   return_pct=exit_px/entry-1,reason=reason,hold_days=exit_i-i,mfe=mfe,mae=mae)
        rows.append(row); last_exit=exit_i
    return pd.DataFrame(rows)

def stats(g):
    gains=g.loc[g.return_pct>0,"return_pct"].sum(); losses=-g.loc[g.return_pct<0,"return_pct"].sum()
    return dict(trades=len(g),win_rate=(g.return_pct>0).mean(),
                target_hit_rate=g.reason.str.startswith("TARGET").mean(),
                stop_rate=g.reason.str.startswith("STOP").mean(),
                avg_return=g.return_pct.mean(),profit_factor=gains/losses if losses>0 else np.inf,
                avg_hold=g.hold_days.mean(),avg_mfe=g.mfe.mean(),avg_mae=g.mae.mean())

def run(spy_path,stock_paths,outdir):
    out=Path(outdir); out.mkdir(parents=True,exist_ok=True); spy=load_ohlcv(spy_path)
    sigall=[]; trall=[]
    for p in stock_paths:
        ticker=Path(p).stem.upper(); d,s=detect(load_ohlcv(p),spy,ticker)
        if not s.empty: sigall.append(s)
        for target in TARGETS:
            tr=backtest(d,s,target)
            if not tr.empty: trall.append(tr)
    signals=pd.concat(sigall,ignore_index=True) if sigall else pd.DataFrame()
    trades=pd.concat(trall,ignore_index=True) if trall else pd.DataFrame()
    signals.to_csv(out/"jlaw_signals.csv",index=False); trades.to_csv(out/"jlaw_trades.csv",index=False)
    summary=[]
    if not trades.empty:
        for target,g in trades.groupby("target_pct"):
            r={"target_pct":target,"stop_pct":STOP,"max_hold":MAX_HOLD}; r.update(stats(g)); summary.append(r)
    sm=pd.DataFrame(summary); sm.to_csv(out/"jlaw_summary.csv",index=False)
    print("J Law Pure Backtest V1.0")
    print("Signals:",len(signals),"Trade rows:",len(trades))
    print(sm.to_string(index=False) if len(sm) else "No qualifying trades")
    print("NOTE: Sales growth / ETF / Utilities point-in-time fundamentals are not fabricated in V1.")

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--spy",required=True); ap.add_argument("--stocks",nargs="+",required=True)
    ap.add_argument("--outdir",default="jlaw_backtest_results")
    a=ap.parse_args(); run(a.spy,a.stocks,a.outdir)
