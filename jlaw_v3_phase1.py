#!/usr/bin/env python3
from pathlib import Path
import numpy as np, pandas as pd

STOP=0.07; TARGET=0.30; MAX_HOLD=60
ENGINE_VERSION="V3.2.1_NO_LOOKAHEAD"

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
    prev_close=x.close.shift(1)
    tr=pd.concat([(x.high-x.low),(x.high-prev_close).abs(),(x.low-prev_close).abs()],axis=1).max(axis=1)
    x["atr14"]=tr.rolling(14).mean()
    x["atr_pct"]=x["atr14"]/x["close"]
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


def _swing_points(df, left=2, right=2):
    """Return local swing highs/lows using only surrounding bars.
    Used only for historical backtest after the bar exists, so no future leak
    relative to the current evaluation date.
    """
    highs=[]; lows=[]
    h=df["high"].to_numpy(); l=df["low"].to_numpy()
    n=len(df)
    for i in range(left, n-right):
        wh=h[i-left:i+right+1]
        wl=l[i-left:i+right+1]
        if np.isfinite(h[i]) and h[i] >= np.nanmax(wh):
            highs.append(i)
        if np.isfinite(l[i]) and l[i] <= np.nanmin(wl):
            lows.append(i)
    return highs,lows

def _find_contractions(df, lookback=90):
    """Identify actual peak->trough pullbacks rather than splitting time into thirds.
    Returns recent contraction percentages from oldest to newest.
    """
    w=df.iloc[-lookback:].reset_index(drop=True) if len(df)>lookback else df.reset_index(drop=True)
    highs,lows=_swing_points(w,2,2)
    pairs=[]
    for hi in highs:
        later=[lo for lo in lows if lo>hi]
        if not later: 
            continue
        lo=later[0]
        # require at least one rebound attempt after the trough
        later_hi=[hh for hh in highs if hh>lo]
        if not later_hi and lo > len(w)-6:
            continue
        ph=float(w.loc[hi,"high"]); pl=float(w.loc[lo,"low"])
        if ph<=0 or pl<=0 or pl>=ph:
            continue
        dd=(ph-pl)/ph
        # ignore micro noise and absurd crash legs
        if 0.025 <= dd <= 0.65:
            pairs.append((hi,lo,dd))
    # de-duplicate overlapping highs/lows and keep chronologically latest distinct legs
    cleaned=[]
    last_lo=-1
    for p in pairs:
        if p[0] <= last_lo:
            continue
        cleaned.append(p)
        last_lo=p[1]
    vals=[p[2] for p in cleaned[-4:]]
    return vals, w

def supply(x):
    contractions,w=_find_contractions(x, lookback=90)
    # last 3 actual contractions, oldest->newest
    c1=c2=c3=np.nan
    if len(contractions)>=3:
        c1,c2,c3=contractions[-3:]
    elif len(contractions)==2:
        c1,c2=contractions[-2:]
    elif len(contractions)==1:
        c1=contractions[-1]

    # volume dry-up: compare recent 10d volume to prior 40d baseline
    base=float(x.volume.iloc[-50:-10].mean()) if len(x)>=50 else np.nan
    dry=float(x.volume.iloc[-10:].mean()/base) if np.isfinite(base) and base>0 else np.nan

    # pivot formed from PRIOR bars only; focus on recent resistance cluster
    prior=x.iloc[-16:-1] if len(x)>=16 else x.iloc[:-1]
    pivot=float(prior.high.max()) if len(prior) else np.nan

    # tightness is based on the FINAL 10 prior bars, not the whole contraction history
    tight_zone=x.iloc[-11:-1] if len(x)>=11 else x.iloc[:-1]
    if len(tight_zone):
        zhi=float(tight_zone.high.max()); zlo=float(tight_zone.low.min())
        tight=(zhi-zlo)/zlo if zlo>0 else np.nan
    else:
        tight=np.nan

    vr=float(x.volume.iloc[-1]/x.v50.iloc[-1]) if x.v50.iloc[-1]>0 else np.nan

    s=0
    # structure quality: reward actual sequential shrinking pullbacks
    if np.isfinite(c1) and np.isfinite(c2):
        if c2 < c1: s += 18
    if np.isfinite(c2) and np.isfinite(c3):
        if c3 < c2: s += 22
    if np.isfinite(c3):
        if c3 <= .08: s += 18
        elif c3 <= .12: s += 12
        elif c3 <= .16: s += 6
    elif np.isfinite(c2):
        if c2 <= .12: s += 10
        elif c2 <= .16: s += 5

    # volume dry-up
    if np.isfinite(dry):
        if dry < .65: s += 18
        elif dry < .80: s += 12
        elif dry < .95: s += 6

    # final pivot tightness
    if np.isfinite(tight):
        if tight < .06: s += 14
        elif tight < .10: s += 10
        elif tight < .14: s += 6

    return min(s,100),pivot,tight,vr,c1,c2,c3,dry


def breakout_sanity_check():
    """Tiny mathematical sanity check: a current close can exceed a prior pivot."""
    prior_highs=pd.Series([100.0]*15)
    pivot=float(prior_highs.max())
    close=101.0
    assert close>pivot, "Pivot sanity check failed"

def empty_funnel():
    return {
        "bars_checked":0,
        "market_pass":0,
        "stage2_pass":0,
        "rs_pass":0,
        "sector_pass":0,
        "group_pass":0,
        "supply_pass":0,
        "leader_pass":0,
        "breakout_pass":0,
    }


# ---------------- V4.6 SHADOW GATE ----------------
# Important: V4.6 does NOT replace V3 BASE. It runs beside it as a Challenger.
# Same entry day information only; same -7% / +30% / 60d exit for fair comparison.

V46_SCHEMES = {
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

def _v46_band(x,bands):
    try: x=float(x)
    except Exception: return 0.0
    for lo,hi,score in bands:
        if lo <= x < hi:
            return float(score)
    return 0.0

def _v46_scheme_score(name,vr,quality,sector,leader,supply):
    cfg=V46_SCHEMES[name]; w=cfg["weights"]
    return (
        _v46_band(vr,cfg["volume"])*w["volume"] +
        _v46_band(quality,cfg["quality"])*w["quality"] +
        _v46_band(sector,cfg["sector"])*w["sector"] +
        _v46_band(leader,cfg["leader"])*w["leader"] +
        _v46_band(supply,cfg["supply"])*w["supply"]
    )

def _v46_regime(spy_hist):
    x=ind(spy_hist)
    if len(x)<220:return "UNKNOWN"
    r=x.iloc[-1]
    need=[r.close,r.ma20,r.ma50,r.ma200,r.ma50old,r.ma200old]
    if any(pd.isna(v) for v in need):return "UNKNOWN"
    ret20=(r.close/x.close.iloc[-21]-1) if len(x)>=21 else np.nan
    if (r.close>r.ma20>r.ma50>r.ma200 and
        r.ma50>r.ma50old and r.ma200>r.ma200old and
        pd.notna(ret20) and ret20>0):
        return "STRONG_BULL"
    if r.close>r.ma200 and r.ma200>r.ma200old:
        return "BULL"
    if r.close>r.ma200 or r.ma50>r.ma200:
        return "TRANSITION"
    return "DEFENSIVE"

def _v46_adaptive_score(regime,vr,quality,sector,leader,supply):
    sweet=_v46_scheme_score("SWEET_CORE",vr,quality,sector,leader,supply)
    vs=_v46_scheme_score("VOLUME_SECTOR",vr,quality,sector,leader,supply)
    ql=_v46_scheme_score("QUALITY_LEADER",vr,quality,sector,leader,supply)
    rb=_v46_scheme_score("ROBUST_BALANCED",vr,quality,sector,leader,supply)
    if regime=="STRONG_BULL":
        return 0.45*ql + 0.35*rb + 0.20*vs
    if regime=="BULL":
        return 0.45*vs + 0.35*rb + 0.20*sweet
    return 0.0

def historical_signals(t,raw,spy,secdf,all_data,stop_file=None):
    d=ind(raw); out=[]; funnel=empty_funnel(); diag_rows=[]
    peers=[p for p,s in T2S.items() if s==T2S.get(t) and p in all_data]
    for i in range(260,len(d)):
        if stop_file and Path(stop_file).exists():
            raise RuntimeError("__V3_STOPPED__")
        funnel["bars_checked"]+=1
        date=d.loc[i,"date"]; x=d.iloc[:i+1]
        sp=spy[spy.date<=date]
        ok,ms=market_ok(sp)
        if not ok: continue
        funnel["market_pass"]+=1
        if not stage2(x): continue
        funnel["stage2_pass"]+=1
        rsv=rs(x,sp)
        if rsv<72: continue
        funnel["rs_pass"]+=1

        secscore=50
        if secdf is not None:
            sd=secdf[secdf.date<=date]
            if len(sd)>=260: secscore=rs(sd,sp)
        if secscore<58: continue
        funnel["sector_pass"]+=1

        strong=top=valid=0
        for p in peers:
            if stop_file and Path(stop_file).exists():
                raise RuntimeError("__V3_STOPPED__")
            pd0=all_data[p]; ps=pd0[pd0.date<=date]
            if len(ps)<260: continue
            valid+=1; pr=rs(ps,sp); strong+=pr>=70; top+=pr>=85
        breadth=100*strong/valid if valid else 0
        if breadth<30: continue
        funnel["group_pass"]+=1

        ss,piv,tight,vr,c1,c2,c3,dry=supply(x)
        diag_rows.append(dict(
            ticker=t,date=date,rs=rsv,sector_score=secscore,group_breadth=breadth,
            supply_score=ss,pivot_tight=tight,volratio=vr,c1=c1,c2=c2,c3=c3,dry=dry
        ))
        if ss<40 or not np.isfinite(tight) or tight>=.16: continue
        funnel["supply_pass"]+=1

        leader=min(100,int(round(min(40,max(0,(rsv-60)*1.5))+min(25,max(0,secscore-55))+min(20,breadth*.2)+(15 if top>=2 else (8 if top>=1 else 0)))))
        if leader<55: continue
        funnel["leader_pass"]+=1

        prev=float(x.close.iloc[-2]); close=float(x.close.iloc[-1]); ext=close/piv-1
        if not(prev<=piv and close>piv and ext<=.03 and vr>=1.20): continue
        funnel["breakout_pass"]+=1

        quality=int(round(.20*(ms/4*100)+.20*secscore+.25*leader+.25*ss+.10*100))
        grade="A+" if quality>=78 else "A"
        regime=_v46_regime(sp)
        v46_score=_v46_adaptive_score(regime,vr,quality,secscore,leader,ss)
        out.append(dict(ticker=t,date=date,idx=i,entry=close,pivot=piv,grade=grade,quality=quality,
                        rs=rsv,sector_score=secscore,leader_score=leader,supply_score=ss,
                        group_breadth=breadth,volratio=vr,regime=regime,v46_score=v46_score,
                        v46_pass=bool(regime in ("STRONG_BULL","BULL") and v46_score>=70)))
    return d,pd.DataFrame(out),funnel,pd.DataFrame(diag_rows)

def trade(d,sigs):
    rows=[]; last=-1
    for _,s in sigs.iterrows():
        i=int(s.idx)
        if i<=last: continue
        en=float(s.entry); st=en*(1-STOP); tg=en*(1+TARGET)
        exi=None; px=None; why=None; mfe=0.; mae=0.
        for j in range(i+1,min(i+1+MAX_HOLD,len(d))):
            o,h,l=map(float,[d.loc[j,"open"],d.loc[j,"high"],d.loc[j,"low"]])
            # Opening gap exits happen before any intraday high/low can belong to the trade.
            if o<=st: exi=j;px=o;why="STOP_GAP";break
            if o>=tg: exi=j;px=o;why="TARGET_GAP";break

            # Conservative daily-OHLC ordering: if both stop and target are touched
            # in the same bar, count the stop first because intraday sequence is unknown.
            mfe=max(mfe,h/en-1); mae=min(mae,l/en-1)
            if l<=st: exi=j;px=st;why="STOP";break
            if h>=tg: exi=j;px=tg;why="TARGET";break
        if exi is None:
            exi=min(i+MAX_HOLD,len(d)-1);px=float(d.loc[exi,"close"]);why="TIME"
        row={k:s[k] for k in s.index if k!="idx"}
        row.update(exit_date=d.loc[exi,"date"],return_pct=px/en-1,reason=why,hold_days=exi-i,mfe=mfe,mae=mae)
        rows.append(row);last=exi
    return pd.DataFrame(rows)


def _simulate_exit(d, i, entry, mode):
    """Simulate one exit policy from the same entry signal."""
    # Defaults
    stop_pct=0.07
    target_pct=0.30
    be_trigger=None

    atr_pct=float(d.loc[i,"atr_pct"]) if "atr_pct" in d.columns and pd.notna(d.loc[i,"atr_pct"]) else np.nan

    if mode=="BASE":
        pass
    elif mode=="VOL15":
        # same exit, entry filter handled outside
        pass
    elif mode=="VOL18":
        pass
    elif mode=="BE8":
        be_trigger=0.08
    elif mode=="BE10":
        be_trigger=0.10
    elif mode=="VOL15_BE8":
        be_trigger=0.08
    elif mode=="ATR":
        # Adaptive stop/target based on current ATR%
        # clamp avoids absurdly tight/wide levels
        if np.isfinite(atr_pct):
            stop_pct=float(np.clip(2.2*atr_pct,0.045,0.10))
            target_pct=float(np.clip(4.5*atr_pct,0.16,0.40))
    elif mode=="ATR_BE8":
        if np.isfinite(atr_pct):
            stop_pct=float(np.clip(2.2*atr_pct,0.045,0.10))
            target_pct=float(np.clip(4.5*atr_pct,0.16,0.40))
        be_trigger=0.08

    stop=entry*(1-stop_pct)
    target=entry*(1+target_pct)
    live_stop=stop
    mfe=0.; mae=0.; be_armed=False

    for j in range(i+1,min(i+1+MAX_HOLD,len(d))):
        o=float(d.loc[j,"open"]); h=float(d.loc[j,"high"]); l=float(d.loc[j,"low"])

        # 1) Opening gap is known first. If it exits the trade, do NOT count
        # that day's later high/low in MFE/MAE.
        if o<=live_stop:
            return j,o,"STOP_GAP_BE" if be_armed and live_stop>=entry else "STOP_GAP",o/entry-1,mfe,mae,stop_pct,target_pct,be_armed
        if o>=target:
            return j,o,"TARGET_GAP",o/entry-1,mfe,mae,stop_pct,target_pct,be_armed

        # 2) Existing stop/target are evaluated using this bar.
        # With daily OHLC we do not know whether high or low happened first,
        # so use a conservative stop-first assumption.
        mfe=max(mfe,h/entry-1); mae=min(mae,l/entry-1)
        if l<=live_stop:
            return j,live_stop,"STOP_BE" if be_armed and live_stop>=entry else "STOP",live_stop/entry-1,mfe,mae,stop_pct,target_pct,be_armed
        if h>=target:
            return j,target,"TARGET",target/entry-1,mfe,mae,stop_pct,target_pct,be_armed

        # 3) Breakeven trigger becomes active only for the NEXT bar.
        # This avoids pretending we know the intraday sequence from daily OHLC.
        if be_trigger is not None and h >= entry*(1+be_trigger):
            be_armed=True
            live_stop=max(live_stop,entry)

    j=min(i+MAX_HOLD,len(d)-1)
    px=float(d.loc[j,"close"])
    return j,px,"TIME",px/entry-1,mfe,mae,stop_pct,target_pct,be_armed

def shadow_tests(d,sigs):
    """Run multiple exit/entry policies against the exact same signal set."""
    modes=[
        ("BASE",1.20),
        ("VOL15",1.50),
        ("VOL18",1.80),
        ("BE8",1.20),
        ("BE10",1.20),
        ("VOL15_BE8",1.50),
        ("ATR",1.20),
        ("ATR_BE8",1.20),
    ]
    rows=[]
    for mode,min_vr in modes:
        last=-1
        for _,s in sigs.iterrows():
            i=int(s.idx)
            if i<=last: continue
            vr=float(s.volratio) if pd.notna(s.volratio) else 0
            if vr<min_vr: continue
            entry=float(s.entry)
            exi,px,reason,ret,mfe,mae,sp,tp,be=_simulate_exit(d,i,entry,mode)
            rows.append(dict(
                mode=mode,ticker=s.ticker,date=s.date,grade=s.grade,entry=entry,
                volratio=vr,quality=s.quality,rs=s.rs,supply_score=s.supply_score,
                return_pct=ret,reason=reason,hold_days=exi-i,mfe=mfe,mae=mae,
                stop_pct=sp,target_pct=tp,be_armed=be,
                atr_pct=float(d.loc[i,"atr_pct"]) if pd.notna(d.loc[i,"atr_pct"]) else np.nan
            ))
            last=exi
    return pd.DataFrame(rows)

def shadow_summary(df):
    rows=[]
    if df.empty:return pd.DataFrame()
    for mode,g in df.groupby("mode"):
        wins=g[g.return_pct>0].return_pct; losses=g[g.return_pct<0].return_pct
        rows.append(dict(
            mode=mode,trades=len(g),
            win_rate=(g.return_pct>0).mean(),
            avg_return=g.return_pct.mean(),
            avg_win=wins.mean() if len(wins) else np.nan,
            avg_loss=losses.mean() if len(losses) else np.nan,
            profit_factor=(wins.sum()/(-losses.sum())) if len(losses) and -losses.sum()>0 else (np.inf if len(wins) else 0),
            avg_hold=g.hold_days.mean(),
            target_hit_rate=g.reason.astype(str).str.startswith("TARGET").mean(),
            stop_rate=g.reason.astype(str).str.startswith("STOP").mean(),
            be_rate=g.reason.astype(str).eq("STOP_BE").mean(),
            avg_stop=g.stop_pct.mean(),
            avg_target=g.target_pct.mean(),
            avg_mfe=g.mfe.mean(),
            avg_mae=g.mae.mean()
        ))
    return pd.DataFrame(rows)

def summary(g):
    wins=g[g.return_pct>0].return_pct; losses=g[g.return_pct<0].return_pct
    return dict(trades=len(g),win_rate=(g.return_pct>0).mean(),avg_return=g.return_pct.mean(),
                avg_win=wins.mean() if len(wins) else np.nan,avg_loss=losses.mean() if len(losses) else np.nan,
                payoff_ratio=(wins.mean()/abs(losses.mean())) if len(wins) and len(losses) else np.nan,
                profit_factor=(wins.sum()/(-losses.sum())) if len(losses) else np.inf,
                target_hit_rate=g.reason.astype(str).str.startswith("TARGET").mean(),
                stop_rate=g.reason.astype(str).str.startswith("STOP").mean(),avg_hold=g.hold_days.mean())

def run(market_dir,outdir,progress_cb=None,quick=False,quick_n=15,quick_lookback=420,stop_file=None):
    md=Path(market_dir); out=Path(outdir); out=(out/"quick" if quick else out); out.mkdir(parents=True,exist_ok=True)
    files={p.stem.upper():p for p in md.glob("*.csv")}
    spy=load(files["SPY"])
    all_data={}
    for t,p in files.items():
        try: all_data[t]=load(p)
        except: pass
    stocks=[t for t in T2S if t in all_data]
    if quick: stocks=stocks[:max(1,int(quick_n))]
    breakout_sanity_check()
    sigs=[]; trades=[]; funnels=[]; supply_diags=[]; shadow_frames=[]; v46_sigs=[]; v46_trades=[]
    v46_diag=dict(
        engine_version="V4.6.1-DIAG",
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
        last_error=""
    )
    for n,t in enumerate(stocks,1):
        if stop_file and Path(stop_file).exists():
            raise RuntimeError("__V3_STOPPED__")
        sd=all_data.get(T2S.get(t))
        raw_t=all_data[t]; spy_t=spy; sd_t=sd
        if quick:
            raw_t=raw_t.tail(max(300,int(quick_lookback))).reset_index(drop=True)
            if len(raw_t):
                start_date=raw_t["date"].iloc[0]
                spy_t=spy[spy["date"]>=start_date].reset_index(drop=True)
                if sd is not None:
                    sd_t=sd[sd["date"]>=start_date].reset_index(drop=True)
        d,s,f,sdg=historical_signals(t,raw_t,spy_t,sd_t,all_data,stop_file=stop_file)
        if not sdg.empty: supply_diags.append(sdg)
        f={"ticker":t,**f}; funnels.append(f)
        if not s.empty:
            sigs.append(s); tr=trade(d,s)
            if not tr.empty: trades.append(tr)

            # V4.6 diagnostics on the exact signal rows seen by the engine.
            v46_diag["base_signal_rows"] += int(len(s))
            if "regime" in s.columns:
                v46_diag["regime_tradeable_rows"] += int(s["regime"].isin(["STRONG_BULL","BULL"]).sum())
            if "v46_score" in s.columns:
                v46_diag["score_ge70_rows"] += int((pd.to_numeric(s["v46_score"],errors="coerce")>=70).sum())
            if "v46_pass" in s.columns:
                v46_diag["v46_pass_rows"] += int((s["v46_pass"]==True).sum())

            # V4.6 Challenger: same original signal universe, adaptive score >=70.
            s46=s[s["v46_pass"]==True].copy() if "v46_pass" in s.columns else pd.DataFrame()
            if not s46.empty:
                v46_sigs.append(s46)
                v46_diag["stocks_with_v46_signal"] += 1
                v46_diag["v46_signal_rows"] += int(len(s46))
                tr46=trade(d,s46)
                if not tr46.empty:
                    v46_trades.append(tr46)
                    v46_diag["stocks_with_v46_trade"] += 1
                    v46_diag["v46_trade_rows"] += int(len(tr46))

            sh=shadow_tests(d,s)
            if not sh.empty: shadow_frames.append(sh)
        if progress_cb: progress_cb(n,len(stocks),t)
    S=pd.concat(sigs,ignore_index=True) if sigs else pd.DataFrame()
    T=pd.concat(trades,ignore_index=True) if trades else pd.DataFrame()
    S.to_csv(out/"jlaw_v3_signals.csv",index=False);T.to_csv(out/"jlaw_v3_trades.csv",index=False)

    S46=pd.concat(v46_sigs,ignore_index=True) if v46_sigs else pd.DataFrame()
    T46=pd.concat(v46_trades,ignore_index=True) if v46_trades else pd.DataFrame()

    # Persist diagnostics even if V4.6 has zero rows. This prevents another blind full run.
    try:
        S46.to_csv(out/"jlaw_v46_signals.csv",index=False)
        v46_diag["csv_signals_written"]=(out/"jlaw_v46_signals.csv").exists()
        T46.to_csv(out/"jlaw_v46_trades.csv",index=False)
        v46_diag["csv_trades_written"]=(out/"jlaw_v46_trades.csv").exists()
        if not T46.empty:
            pd.DataFrame([summary(T46)]).to_csv(out/"jlaw_v46_summary.csv",index=False)
        else:
            pd.DataFrame(columns=["trades","win_rate","avg_return","profit_factor"]).to_csv(out/"jlaw_v46_summary.csv",index=False)
        v46_diag["csv_summary_written"]=(out/"jlaw_v46_summary.csv").exists()
    except Exception as e:
        v46_diag["last_error"]=f"V46 write error: {type(e).__name__}: {e}"

    # Reconcile final row counts from actual in-memory frames.
    v46_diag["v46_signal_rows"]=int(len(S46))
    v46_diag["v46_trade_rows"]=int(len(T46))
    v46_diag["signals_csv_size"]=int((out/"jlaw_v46_signals.csv").stat().st_size) if (out/"jlaw_v46_signals.csv").exists() else 0
    v46_diag["trades_csv_size"]=int((out/"jlaw_v46_trades.csv").stat().st_size) if (out/"jlaw_v46_trades.csv").exists() else 0
    v46_diag["summary_csv_size"]=int((out/"jlaw_v46_summary.csv").stat().st_size) if (out/"jlaw_v46_summary.csv").exists() else 0
    v46_diag["result_folder"]=str(out)

    try:
        import json as _json
        (out/"jlaw_v46_diag.json").write_text(_json.dumps(v46_diag,indent=2,default=str),encoding="utf-8")
    except Exception as e:
        # last-resort plain text file
        try:
            (out/"jlaw_v46_diag.txt").write_text(str(v46_diag)+"\nDIAG_WRITE_ERROR="+repr(e),encoding="utf-8")
        except Exception:
            pass

    F=pd.DataFrame(funnels)
    F.to_csv(out/"jlaw_v3_funnel.csv",index=False)
    SD=pd.concat(supply_diags,ignore_index=True) if supply_diags else pd.DataFrame()
    SD.to_csv(out/"jlaw_v3_supply_diag.csv",index=False)
    if not SD.empty:
        stats=[]
        for c in ["supply_score","pivot_tight","dry","c1","c2","c3","volratio"]:
            s=pd.to_numeric(SD[c],errors="coerce").dropna()
            if len(s):
                stats.append(dict(
                    metric=c,count=len(s),min=float(s.min()),p10=float(s.quantile(.10)),
                    p25=float(s.quantile(.25)),median=float(s.quantile(.50)),
                    p75=float(s.quantile(.75)),p90=float(s.quantile(.90)),max=float(s.max())
                ))
        pd.DataFrame(stats).to_csv(out/"jlaw_v3_supply_stats.csv",index=False)
    if not F.empty:
        totals={c:int(F[c].sum()) for c in F.columns if c!="ticker"}
        pd.DataFrame([totals]).to_csv(out/"jlaw_v3_funnel_total.csv",index=False)
    SH=pd.concat(shadow_frames,ignore_index=True) if shadow_frames else pd.DataFrame()
    SH.to_csv(out/"jlaw_v3_shadow_trades.csv",index=False)
    SHS=shadow_summary(SH)
    SHS.to_csv(out/"jlaw_v3_shadow_summary.csv",index=False)

    rows=[]
    if not T.empty:
        for grade,g in T.groupby("grade"):
            r={"grade":grade};r.update(summary(g));rows.append(r)
        r={"grade":"ALL"};r.update(summary(T));rows.append(r)
    sm=pd.DataFrame(rows);sm.to_csv(out/"jlaw_v3_summary.csv",index=False)
    return sm,T,S
