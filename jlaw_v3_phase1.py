#!/usr/bin/env python3
from pathlib import Path
import numpy as np, pandas as pd

STOP=0.07; TARGET=0.30; MAX_HOLD=60

SECTORS={
"XLK":"AAPL MSFT NVDA AVGO ORCL AMD CRM ADBE QCOM TXN INTU AMAT LRCX KLAC MU PANW CRWD NOW PLTR",
"XLY":"AMZN TSLA HD LOW BKNG TJX SBUX MCD NKE CMG ORLY AZO ROST MAR",
"XLC":"GOOGL META NFLX DIS T VZ TMUS CHTR",
"XLF":"JPM BAC WFC GS MS C AXP SCHW BLK SPGI CME ICE COF USB PNC",
"XLV":"LLY UNH JNJ ABBV MRK TMO ABT DHR AMGN GILD ISRG VRTX REGN BSX MDT SYK",
"XLP":"WMT COST PG KO PEP PM MO CL MDLZ KMB GIS",
"XLI":"GE CAT DE ETN PH HON RTX LMT NOC GD UPS FDX UNP CSX WM RSG UBER",
"XLE":"XOM CVX COP EOG SLB MPC PSX VLO OXY",
"XLB":"LIN APD SHW ECL FCX NEM NUE",
"XLRE":"AMT PLD EQIX WELL SPG O"}
T2S={t:s for s,n in SECTORS.items() for t in n.split()}

def load(p):
    d=pd.read_csv(p); d.columns=[str(c).lower() for c in d.columns]
    dc="date" if "date" in d else "datetime"
    d=d[[dc,"open","high","low","close","volume"]].rename(columns={dc:"date"})
    d["date"]=pd.to_datetime(d.date,errors="coerce")
    for c in ["open","high","low","close","volume"]: d[c]=pd.to_numeric(d[c],errors="coerce")
    return d.dropna().sort_values("date").drop_duplicates("date").reset_index(drop=True)

def ind(d):
    x=d.copy()
    for n in [20,50,150,200]: x[f"ma{n}"]=x.close.rolling(n).mean()
    x["ma50old"]=x.ma50.shift(10); x["ma200old"]=x.ma200.shift(20); x["v50"]=x.volume.rolling(50).mean()
    x["hi252"]=x.high.rolling(252).max(); x["lo252"]=x.low.rolling(252).min()
    return x

def rs(stock,bench):
    z=stock[["date","close"]].merge(bench[["date","close"]].rename(columns={"close":"b"}),on="date",how="left")
    vals=[]
    for n,w in [(21,.15),(63,.25),(126,.30),(252,.30)]:
        if len(z)>n:
            a=z.close.iloc[-1]/z.close.iloc[-1-n]-1; b=z.b.iloc[-1]/z.b.iloc[-1-n]-1
            if np.isfinite(a) and np.isfinite(b): vals.append((a-b,w))
    if not vals:return 50
    ex=sum(v*w for v,w in vals)/sum(w for _,w in vals)
    return int(round(max(0,min(100,50+180*ex))))

def market_ok(spy):
    x=ind(spy); r=x.iloc[-1]
    if any(pd.isna([r.ma50,r.ma200,r.ma200old])): return False,0
    score=int(r.close>r.ma50>r.ma200)*2+int(r.ma200>r.ma200old)+int(r.ma50>r.ma50old)
    return score>=3,score

def stage2(x):
    r=x.iloc[-1]
    vals=[r.ma50,r.ma150,r.ma200,r.ma200old,r.hi252,r.lo252]
    if any(pd.isna(v) for v in vals):return False
    return bool(r.close>=12 and r.close>r.ma50>r.ma150>r.ma200 and r.ma200>r.ma200old
                and r.close>=1.30*r.lo252 and r.close>=0.85*r.hi252)

def supply(x):
    b=x.iloc[-60:]; z=np.array_split(b,3); c=[]
    for q in z:
        lo=float(q.low.min()); hi=float(q.high.max()); c.append((hi-lo)/lo if lo>0 else np.nan)
    c1,c2,c3=c
    base=float(x.volume.iloc[-50:-10].mean()); dry=float(x.volume.iloc[-10:].mean()/base) if base>0 else np.nan
    pivot=float(x.high.iloc[-15:].max()); plow=float(x.low.iloc[-15:].min())
    tight=(pivot-plow)/plow if plow>0 else np.nan
    vr=float(x.volume.iloc[-1]/x.v50.iloc[-1]) if x.v50.iloc[-1]>0 else np.nan
    s=0
    if np.isfinite(c1+c2+c3):
        s+=12 if c2<c1 else 0; s+=12 if c3<c2 else 0; s+=14 if c3<.10 else (7 if c3<.13 else 0)
    if np.isfinite(dry): s+=18 if dry<.65 else (12 if dry<.80 else (6 if dry<.90 else 0))
    if np.isfinite(tight): s+=18 if tight<.06 else (10 if tight<.10 else 0)
    return min(s,100),pivot,tight,vr,c1,c2,c3,dry

def historical_signals(t,raw,spy,secdf,all_data):
    d=ind(raw); out=[]
    peers=[p for p,s in T2S.items() if s==T2S.get(t) and p in all_data]
    for i in range(260,len(d)):
        date=d.loc[i,"date"]; x=d.iloc[:i+1]
        sp=spy[spy.date<=date]
        ok,ms=market_ok(sp)
        if not ok or not stage2(x): continue
        rsv=rs(x,sp)
        if rsv<72: continue

        secscore=50
        if secdf is not None:
            sd=secdf[secdf.date<=date]
            if len(sd)>=260: secscore=rs(sd,sp)
        if secscore<58: continue

        strong=top=valid=0
        for p in peers:
            pd0=all_data[p]; ps=pd0[pd0.date<=date]
            if len(ps)<260: continue
            valid+=1; pr=rs(ps,sp); strong+=pr>=70; top+=pr>=85
        breadth=100*strong/valid if valid else 0
        if breadth<30: continue

        ss,piv,tight,vr,c1,c2,c3,dry=supply(x)
        if ss<55 or not np.isfinite(tight) or tight>=.12: continue

        leader=min(100,int(round(min(40,max(0,(rsv-60)*1.5))+min(25,max(0,secscore-55))+min(20,breadth*.2)+(15 if top>=2 else (8 if top>=1 else 0)))))
        if leader<55: continue

        prev=float(x.close.iloc[-2]); close=float(x.close.iloc[-1]); ext=close/piv-1
        if not(prev<=piv and close>piv and ext<=.03 and vr>=1.20): continue

        quality=int(round(.20*(ms/4*100)+.20*secscore+.25*leader+.25*ss+.10*100))
        grade="A+" if quality>=78 else "A"
        out.append(dict(ticker=t,date=date,idx=i,entry=close,pivot=piv,grade=grade,quality=quality,
                        rs=rsv,sector_score=secscore,leader_score=leader,supply_score=ss,
                        group_breadth=breadth,volratio=vr))
    return d,pd.DataFrame(out)

def trade(d,sigs):
    rows=[]; last=-1
    for _,s in sigs.iterrows():
        i=int(s.idx)
        if i<=last: continue
        en=float(s.entry); st=en*(1-STOP); tg=en*(1+TARGET)
        exi=None; px=None; why=None; mfe=0.; mae=0.
        for j in range(i+1,min(i+1+MAX_HOLD,len(d))):
            o,h,l=map(float,[d.loc[j,"open"],d.loc[j,"high"],d.loc[j,"low"]])
            mfe=max(mfe,h/en-1); mae=min(mae,l/en-1)
            if o<=st: exi=j;px=o;why="STOP_GAP";break
            if o>=tg: exi=j;px=o;why="TARGET_GAP";break
            if l<=st: exi=j;px=st;why="STOP";break
            if h>=tg: exi=j;px=tg;why="TARGET";break
        if exi is None:
            exi=min(i+MAX_HOLD,len(d)-1);px=float(d.loc[exi,"close"]);why="TIME"
        row={k:s[k] for k in s.index if k!="idx"}
        row.update(exit_date=d.loc[exi,"date"],return_pct=px/en-1,reason=why,hold_days=exi-i,mfe=mfe,mae=mae)
        rows.append(row);last=exi
    return pd.DataFrame(rows)

def summary(g):
    wins=g[g.return_pct>0].return_pct; losses=g[g.return_pct<0].return_pct
    return dict(trades=len(g),win_rate=(g.return_pct>0).mean(),avg_return=g.return_pct.mean(),
                avg_win=wins.mean() if len(wins) else np.nan,avg_loss=losses.mean() if len(losses) else np.nan,
                payoff_ratio=(wins.mean()/abs(losses.mean())) if len(wins) and len(losses) else np.nan,
                profit_factor=(wins.sum()/(-losses.sum())) if len(losses) else np.inf,
                target_hit_rate=g.reason.astype(str).str.startswith("TARGET").mean(),
                stop_rate=g.reason.astype(str).str.startswith("STOP").mean(),avg_hold=g.hold_days.mean())

def run(market_dir,outdir,progress_cb=None):
    md=Path(market_dir); out=Path(outdir); out.mkdir(parents=True,exist_ok=True)
    files={p.stem.upper():p for p in md.glob("*.csv")}
    spy=load(files["SPY"])
    all_data={}
    for t,p in files.items():
        try: all_data[t]=load(p)
        except: pass
    stocks=[t for t in T2S if t in all_data]
    sigs=[]; trades=[]
    for n,t in enumerate(stocks,1):
        sd=all_data.get(T2S.get(t))
        d,s=historical_signals(t,all_data[t],spy,sd,all_data)
        if not s.empty:
            sigs.append(s); tr=trade(d,s)
            if not tr.empty: trades.append(tr)
        if progress_cb: progress_cb(n,len(stocks),t)
    S=pd.concat(sigs,ignore_index=True) if sigs else pd.DataFrame()
    T=pd.concat(trades,ignore_index=True) if trades else pd.DataFrame()
    S.to_csv(out/"jlaw_v3_signals.csv",index=False);T.to_csv(out/"jlaw_v3_trades.csv",index=False)
    rows=[]
    if not T.empty:
        for grade,g in T.groupby("grade"):
            r={"grade":grade};r.update(summary(g));rows.append(r)
        r={"grade":"ALL"};r.update(summary(T));rows.append(r)
    sm=pd.DataFrame(rows);sm.to_csv(out/"jlaw_v3_summary.csv",index=False)
    return sm,T,S
