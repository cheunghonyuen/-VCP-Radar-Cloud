#!/usr/bin/env python3
"""
J Law Backtest V2.0

Three comparable test families:
A) Fixed exit benchmark:
   stop -7%, targets +15/+20/+25/+30/+40/+50%, max hold 60 trading days.
B) J Law midpoint benchmark:
   stop -4.5%, target +13%, max hold 60 trading days.
C) J Law Native V1 (quantified approximation of confirmed public teaching):
   - entry at J Law-style VCP/Pivot breakout
   - structural initial stop, capped at 8% risk
   - at +20% sell 50%
   - remaining 50% runs until 30-week trend / Stage 2 weakens
   - no fixed 60-day time exit for the runner

Important:
- "Native V1" is a quantification of public rules, not a claim that every discretionary
  J Law judgement has been reproduced perfectly.
- Point-in-time Sales Growth / acceleration is not fabricated when unavailable.
"""
from pathlib import Path
import numpy as np
import pandas as pd

FIXED_TARGETS=[0.15,0.20,0.25,0.30,0.40,0.50]
FIXED_STOP=0.07
FIXED_MAX_HOLD=60
MID_STOP=0.045
MID_TARGET=0.13
MID_MAX_HOLD=60
NATIVE_MAX_RISK=0.08
NATIVE_PARTIAL_TARGET=0.20

def load_ohlcv(path):
    d=pd.read_csv(path)
    d.columns=[str(c).strip().lower() for c in d.columns]
    dc="date" if "date" in d.columns else "datetime"
    need=[dc,"open","high","low","close","volume"]
    miss=[c for c in need if c not in d.columns]
    if miss: raise ValueError(f"{path}: missing {miss}")
    d=d[need].rename(columns={dc:"date"}).copy()
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    for c in ["open","high","low","close","volume"]:
        d[c]=pd.to_numeric(d[c],errors="coerce")
    return d.dropna().sort_values("date").drop_duplicates("date").reset_index(drop=True)

def indicators(d):
    x=d.copy()
    for n in [20,50,150,200]:
        x[f"ma{n}"]=x.close.rolling(n).mean()
    x["ma150_10ago"]=x.ma150.shift(10)
    x["ma200_20ago"]=x.ma200.shift(20)
    x["vol50"]=x.volume.rolling(50).mean()
    x["high52"]=x.high.rolling(252).max()
    x["low52"]=x.low.rolling(252).min()
    x["above52low"]=x.close/x.low52-1
    x["below52high"]=x.close/x.high52-1
    return x

def add_rs(d,spy):
    z=d.merge(spy[["date","close"]].rename(columns={"close":"spy"}),on="date",how="left")
    z["rsline"]=z.close/z.spy
    z["rsline_up"]=z.rsline>z.rsline.shift(20)
    valid=pd.Series(0.0,index=z.index)
    wsum=pd.Series(0.0,index=z.index)
    for n,w in [(63,.25),(126,.35),(252,.40)]:
        e=(z.close/z.close.shift(n)-1)-(z.spy/z.spy.shift(n)-1)
        valid=valid+e.fillna(0)*w
        wsum=wsum+e.notna().astype(float)*w
    z["rs_proxy"]=(50+(valid/wsum.replace(0,np.nan))*180).clip(0,100)
    return z

def detect(raw,spy,ticker):
    d=add_rs(indicators(raw),spy)
    sig=[]
    for i in range(260,len(d)):
        r=d.iloc[i]
        vals=[r.ma50,r.ma150,r.ma200,r.ma200_20ago,r.above52low,r.below52high,r.rs_proxy,r.vol50]
        if any(pd.isna(v) for v in vals): continue
        # J Law strong Stage 2 screen from the teaching material we confirmed.
        if not (
            r.close>=12 and
            r.close>r.ma50>r.ma150>r.ma200 and
            r.ma200>r.ma200_20ago and
            r.above52low>=.30 and
            r.below52high>=-.15 and
            r.rs_proxy>=89 and
            r.vol50>=25000
        ):
            continue

        base=d.iloc[i-60:i]
        thirds=np.array_split(base,3)
        c=[]
        for q in thirds:
            hi=float(q.high.max()); lo=float(q.low.min())
            c.append((hi-lo)/lo if lo>0 else np.nan)
        if any(pd.isna(v) for v in c): continue
        c1,c2,c3=c
        if not (c2<c1 and c3<c2 and c3<.10): continue

        prior=float(base.volume.iloc[:40].mean())
        recent=float(base.volume.iloc[-10:].mean())
        vdu=recent/prior if prior>0 else np.nan
        if not np.isfinite(vdu) or vdu>=.85: continue

        pivot=float(d.high.iloc[i-20:i].max())
        if not (d.close.iloc[i-1]<=pivot and r.close>pivot): continue

        extension=float(r.close/pivot-1)
        if extension>.05: continue

        vr=float(r.volume/r.vol50) if r.vol50>0 else np.nan
        recent_low=float(d.low.iloc[max(0,i-10):i+1].min())
        sig.append(dict(
            ticker=ticker,date=r.date,idx=i,entry=float(r.close),pivot=pivot,
            recent_low=recent_low,extension=extension,
            c1=c1,c2=c2,c3=c3,vdu=vdu,vol_ratio=vr,
            hvb_120=bool(vr>=1.20),rs_proxy=float(r.rs_proxy),
            rsline_up=bool(r.rsline_up)
        ))
    return d,pd.DataFrame(sig)

def _fixed_trade(d,s,target,stop_pct,max_hold):
    i=int(s.idx); entry=float(s.entry)
    stop=entry*(1-stop_pct); take=entry*(1+target)
    exit_px=exit_i=reason=None; mfe=0.; mae=0.
    for j in range(i+1,min(i+1+max_hold,len(d))):
        o,h,l=map(float,[d.loc[j,"open"],d.loc[j,"high"],d.loc[j,"low"]])
        mfe=max(mfe,h/entry-1); mae=min(mae,l/entry-1)
        if o<=stop: exit_px,exit_i,reason=o,j,"STOP_GAP"; break
        if o>=take: exit_px,exit_i,reason=o,j,"TARGET_GAP"; break
        # Conservative if daily OHLC cannot tell which came first.
        if l<=stop: exit_px,exit_i,reason=stop,j,"STOP"; break
        if h>=take: exit_px,exit_i,reason=take,j,"TARGET"; break
    if exit_px is None:
        exit_i=min(i+max_hold,len(d)-1)
        exit_px=float(d.loc[exit_i,"close"]); reason="TIME"
    return {
        "exit_date":d.loc[exit_i,"date"],"exit":exit_px,
        "return_pct":exit_px/entry-1,"reason":reason,
        "hold_days":exit_i-i,"mfe":mfe,"mae":mae
    }, exit_i

def backtest_fixed(d,sigs,target,stop_pct=FIXED_STOP,max_hold=FIXED_MAX_HOLD,model="FIXED"):
    rows=[]; last_exit=-1
    for _,s in sigs.iterrows():
        i=int(s.idx)
        if i<=last_exit: continue
        result,exit_i=_fixed_trade(d,s,target,stop_pct,max_hold)
        row={k:s[k] for k in s.index if k!="idx"}
        row.update(model=model,target_pct=target,stop_pct=stop_pct,max_hold=max_hold,**result)
        rows.append(row); last_exit=exit_i
    return pd.DataFrame(rows)

def _native_stop(entry,pivot,recent_low):
    # Structural stop below pivot / recent support, but never allow >8% initial risk.
    candidates=[entry*(1-NATIVE_MAX_RISK)]
    if np.isfinite(pivot) and pivot<entry:
        candidates.append(pivot*0.985)
    if np.isfinite(recent_low) and recent_low<entry:
        candidates.append(recent_low*0.99)
    stop=max(candidates)  # tighter structural stop, capped at 8% risk
    if stop>=entry:
        stop=entry*0.98
    return stop

def backtest_native(d,sigs):
    """
    Quantified J Law Native V1:
    - structural stop capped at 8%
    - +20%: sell half
    - runner: exit when close < 150d MA (≈30-week) OR 150d MA stops rising vs 10d ago
    - no arbitrary 60-day runner time stop; if history ends, mark END_DATA
    """
    rows=[]; last_exit=-1
    for _,s in sigs.iterrows():
        i=int(s.idx)
        if i<=last_exit: continue
        entry=float(s.entry)
        stop=_native_stop(entry,float(s.pivot),float(s.recent_low))
        partial_level=entry*(1+NATIVE_PARTIAL_TARGET)
        partial_done=False; partial_px=None; partial_i=None
        runner_exit=None; runner_i=None; reason=None
        mfe=0.; mae=0.

        for j in range(i+1,len(d)):
            o,h,l,c=map(float,[d.loc[j,"open"],d.loc[j,"high"],d.loc[j,"low"],d.loc[j,"close"]])
            mfe=max(mfe,h/entry-1); mae=min(mae,l/entry-1)

            # Before partial profit: original structural stop applies to entire position.
            if not partial_done:
                if o<=stop:
                    runner_exit=o; runner_i=j; reason="STOP_GAP"; break
                if l<=stop:
                    runner_exit=stop; runner_i=j; reason="STOP"; break

                if o>=partial_level:
                    partial_done=True; partial_px=o; partial_i=j
                    stop=max(stop,entry)  # move remaining risk to breakeven
                elif h>=partial_level:
                    partial_done=True; partial_px=partial_level; partial_i=j
                    stop=max(stop,entry)

            else:
                # Runner has breakeven floor after partial profit.
                if o<=stop:
                    runner_exit=o; runner_i=j; reason="RUNNER_BE_STOP_GAP"; break
                if l<=stop:
                    runner_exit=stop; runner_i=j; reason="RUNNER_BE_STOP"; break

                ma150=float(d.loc[j,"ma150"]) if pd.notna(d.loc[j,"ma150"]) else np.nan
                ma150old=float(d.loc[j,"ma150_10ago"]) if pd.notna(d.loc[j,"ma150_10ago"]) else np.nan
                stage2_weak=(
                    (np.isfinite(ma150) and c<ma150) or
                    (np.isfinite(ma150) and np.isfinite(ma150old) and ma150<=ma150old)
                )
                if stage2_weak:
                    runner_exit=c; runner_i=j; reason="STAGE2_WEAK"; break

        if runner_exit is None:
            runner_i=len(d)-1
            runner_exit=float(d.loc[runner_i,"close"])
            reason="END_DATA"

        # 50/50 realized return. If no +20% partial occurred, whole position exits together.
        if partial_done:
            ret=.5*(partial_px/entry-1)+.5*(runner_exit/entry-1)
            exit_px=.5*partial_px+.5*runner_exit
        else:
            ret=runner_exit/entry-1
            exit_px=runner_exit

        row={k:s[k] for k in s.index if k!="idx"}
        row.update(
            model="JLAW_NATIVE_V1",target_pct=np.nan,
            stop_pct=1-stop/entry,max_hold=np.nan,
            partial20=partial_done,
            partial_date=(d.loc[partial_i,"date"] if partial_i is not None else pd.NaT),
            exit_date=d.loc[runner_i,"date"],exit=exit_px,
            return_pct=ret,reason=reason,
            hold_days=runner_i-i,mfe=mfe,mae=mae
        )
        rows.append(row); last_exit=runner_i
    return pd.DataFrame(rows)

def stats(g):
    if g.empty:
        return dict(trades=0,win_rate=np.nan,target_hit_rate=np.nan,stop_rate=np.nan,
                    avg_return=np.nan,avg_win=np.nan,avg_loss=np.nan,payoff_ratio=np.nan,
                    expectancy=np.nan,profit_factor=np.nan,avg_hold=np.nan,
                    avg_mfe=np.nan,avg_mae=np.nan,max_consecutive_losses=0)
    wins=g.loc[g.return_pct>0,"return_pct"]
    losses=g.loc[g.return_pct<0,"return_pct"]
    gains=wins.sum(); loss_abs=-losses.sum()
    avg_win=wins.mean() if len(wins) else np.nan
    avg_loss=losses.mean() if len(losses) else np.nan
    payoff=(avg_win/abs(avg_loss)) if len(wins) and len(losses) and avg_loss!=0 else np.nan

    max_losing=0; cur=0
    for v in g.sort_values("exit_date").return_pct:
        if v<0: cur+=1; max_losing=max(max_losing,cur)
        else: cur=0

    return dict(
        trades=len(g),
        win_rate=(g.return_pct>0).mean(),
        target_hit_rate=g.reason.astype(str).str.startswith("TARGET").mean(),
        stop_rate=g.reason.astype(str).str.contains("STOP").mean(),
        avg_return=g.return_pct.mean(),
        avg_win=avg_win,avg_loss=avg_loss,payoff_ratio=payoff,
        expectancy=g.return_pct.mean(),
        profit_factor=(gains/loss_abs if loss_abs>0 else np.inf),
        avg_hold=g.hold_days.mean(),
        avg_mfe=g.mfe.mean(),avg_mae=g.mae.mean(),
        max_consecutive_losses=max_losing
    )

def run(spy_path,stock_paths,outdir,progress_cb=None):
    out=Path(outdir); out.mkdir(parents=True,exist_ok=True)
    spy=load_ohlcv(spy_path)
    total=len(stock_paths)
    signal_frames=[]
    fixed_map={t:[] for t in FIXED_TARGETS}
    midpoint=[]
    native=[]
    processed=0

    for p in stock_paths:
        ticker=Path(p).stem.upper()
        d,s=detect(load_ohlcv(p),spy,ticker)
        if not s.empty:
            signal_frames.append(s)
            for target in FIXED_TARGETS:
                tr=backtest_fixed(d,s,target,FIXED_STOP,FIXED_MAX_HOLD,"FIXED_7")
                if not tr.empty: fixed_map[target].append(tr)
            mid=backtest_fixed(d,s,MID_TARGET,MID_STOP,MID_MAX_HOLD,"JLAW_MID_4.5_13")
            if not mid.empty: midpoint.append(mid)
            nat=backtest_native(d,s)
            if not nat.empty: native.append(nat)

        processed+=1
        if progress_cb:
            progress_cb(processed,total,ticker)

    signals=pd.concat(signal_frames,ignore_index=True) if signal_frames else pd.DataFrame()
    trade_frames=[]
    for target,frames in fixed_map.items():
        if frames: trade_frames.append(pd.concat(frames,ignore_index=True))
    if midpoint: trade_frames.append(pd.concat(midpoint,ignore_index=True))
    if native: trade_frames.append(pd.concat(native,ignore_index=True))
    trades=pd.concat(trade_frames,ignore_index=True) if trade_frames else pd.DataFrame()

    signals.to_csv(out/"jlaw_signals_v2.csv",index=False)
    trades.to_csv(out/"jlaw_trades_v2.csv",index=False)

    rows=[]
    if not trades.empty:
        # Fixed 7% family
        for target in FIXED_TARGETS:
            g=trades[(trades.model=="FIXED_7")&(trades.target_pct==target)]
            if not g.empty:
                r={"model":"固定 -7%","target_label":f"+{int(target*100)}%",
                   "target_pct":target,"stop_pct":FIXED_STOP}
                r.update(stats(g)); rows.append(r)
        # Midpoint
        g=trades[trades.model=="JLAW_MID_4.5_13"]
        if not g.empty:
            r={"model":"J Law 中位数","target_label":"+13%",
               "target_pct":MID_TARGET,"stop_pct":MID_STOP}
            r.update(stats(g)); rows.append(r)
        # Native
        g=trades[trades.model=="JLAW_NATIVE_V1"]
        if not g.empty:
            r={"model":"J Law Native V1","target_label":"+20%卖半/Runner",
               "target_pct":np.nan,"stop_pct":g.stop_pct.mean()}
            r.update(stats(g))
            r["partial20_rate"]=g.partial20.mean()
            rows.append(r)

    summary=pd.DataFrame(rows)
    summary.to_csv(out/"jlaw_summary_v2.csv",index=False)

    print("J Law Backtest V2.0")
    print("Signals:",len(signals),"Trade rows:",len(trades))
    print(summary.to_string(index=False) if len(summary) else "No qualifying trades")
    return summary,trades,signals
