import re
import base64
import hmac
import os, json, time, threading, traceback, io, math
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse, parse_qs
import pandas as pd
import numpy as np
from flask import Flask, jsonify, render_template_string, redirect, url_for, send_file, request
try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound, VideoUnavailable
except Exception:
    YouTubeTranscriptApi = None
    TranscriptsDisabled = NoTranscriptFound = VideoUnavailable = Exception

APP = Flask(__name__)

# --- Cloud Auto Publisher (same Railway service) ---
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN","")
GITHUB_OWNER = os.environ.get("GITHUB_OWNER","cheunghonyuen")
GITHUB_REPO = os.environ.get("GITHUB_REPO","-VCP-Radar-Cloud")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH","main")
PUBLISH_SECRET = os.environ.get("PUBLISH_SECRET","")
PUBLISH_MAX_BYTES = int(os.environ.get("PUBLISH_MAX_BYTES","1500000"))

def _publisher_auth_ok(req):
    got = req.headers.get("X-Publish-Secret","")
    return bool(PUBLISH_SECRET) and hmac.compare_digest(got, PUBLISH_SECRET)

def _gh_headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "vcp-radar-cloud-publisher"
    }

def _gh_request(method, url, payload=None):
    import urllib.request, urllib.error
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=_gh_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8")
            return r.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try: body = json.loads(raw)
        except Exception: body = {"raw": raw}
        return e.code, body

def _gh_content_url(path, include_ref=True):
    from urllib.parse import quote
    qp = quote(path, safe="/")
    base = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{qp}"
    return base + (f"?ref={GITHUB_BRANCH}" if include_ref else "")

def _gh_get_sha(path):
    code, body = _gh_request("GET", _gh_content_url(path, True))
    if code == 200:
        return body.get("sha")
    if code == 404:
        return None
    raise RuntimeError(f"GitHub GET failed {code}: {body}")

def _gh_publish(path, content_bytes, message=None):
    if not GITHUB_TOKEN:
        raise RuntimeError("GITHUB_TOKEN missing")
    if not PUBLISH_SECRET:
        raise RuntimeError("PUBLISH_SECRET missing")
    if len(content_bytes) > PUBLISH_MAX_BYTES:
        raise RuntimeError(f"file too large: {len(content_bytes)} > {PUBLISH_MAX_BYTES}")

    sha = _gh_get_sha(path)
    payload = {
        "message": message or f"Auto publish {path}",
        "content": base64.b64encode(content_bytes).decode("ascii"),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    code, body = _gh_request("PUT", _gh_content_url(path, False), payload)
    if code not in (200, 201):
        raise RuntimeError(f"GitHub PUT failed {code}: {body}")

    return {
        "path": path,
        "commit_sha": ((body.get("commit") or {}).get("sha")),
        "content_sha": ((body.get("content") or {}).get("sha")),
        "html_url": ((body.get("content") or {}).get("html_url")),
    }

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
V5_VALIDATION_DIR = DATA_DIR / "jlaw_v5_validation"
V5_VALIDATION_STATUS_FILE = V5_VALIDATION_DIR / "status.json"
V5_VALIDATION_STOP_FILE = V5_VALIDATION_DIR / "STOP"
V6_SHADOW_DIR = DATA_DIR / "jlaw_v6_shadow"
V6_SHADOW_STATUS_FILE = V6_SHADOW_DIR / "status.json"
V6_SHADOW_STOP_FILE = V6_SHADOW_DIR / "STOP"
V61_FAST_DIR = DATA_DIR / "jlaw_v61_fast"
V61_FAST_STATUS_FILE = V61_FAST_DIR / "status.json"
V61_FAST_STOP_FILE = V61_FAST_DIR / "STOP"
V63_DIR = DATA_DIR / "jlaw_v63_rs2"
V63_STATUS_FILE = V63_DIR / "status.json"
V63_STOP_FILE = V63_DIR / "STOP"
YOUTUBE_DIR = DATA_DIR / "youtube_ingest"; YOUTUBE_DIR.mkdir(parents=True, exist_ok=True)
YOUTUBE_INDEX_FILE = YOUTUBE_DIR / "index.json"
YOUTUBE_UPLOAD_TOKEN = os.environ.get("YOUTUBE_UPLOAD_TOKEN", "").strip()
TIINGO_TOKEN = os.environ.get("TIINGO_TOKEN", "").strip()
AUTO_SCAN = os.environ.get("AUTO_SCAN", "1") == "1"
MIN_GAP = float(os.environ.get("TIINGO_MIN_GAP_SECONDS", "6"))
MAX_GAP = float(os.environ.get("TIINGO_MAX_GAP_SECONDS", "18"))
COOLDOWN_429 = int(os.environ.get("TIINGO_429_COOLDOWN_SECONDS", "60"))
AUTO_REFRESH_SECONDS = int(os.environ.get("AUTO_REFRESH_SECONDS", "5"))
# V5.1: Tiingo Starter-friendly overnight mode.
# 75s ~= 48 requests/hour, leaving a little headroom under the 50/hour free-tier cap.
FREE_TIER_GAP = float(os.environ.get("TIINGO_FREE_TIER_GAP_SECONDS", "75"))
AUTO_429_WAIT = int(os.environ.get("TIINGO_AUTO_429_WAIT_SECONDS", "3900"))  # 65 minutes
current_gap = max(MIN_GAP, FREE_TIER_GAP)
last_request_ts = 0.0
rate_lock = threading.Lock()

class TiingoRateLimitPause(Exception):
    """Stop the current scan immediately on Tiingo HTTP 429; checkpoint is kept for resume."""
    pass

UNIVERSE = sorted(set("""
A AAL AAPL ABBV ABNB ABT ACGL ACN ADBE ADI ADM AEE AEP AES AFL AJG ALB ALGN ALL AMAT AMCR AMD AMGN AMP AMT
AMZN ANET AON APA APD APH APO APP APTV ARE ARES ARM AVB AVGO AVY AWK AXON AXP AZO BA BAC BALL BAX BBY BDX BG
BIIB BK BKNG BKR BLK BMY BSX BWA BX BXP C CAG CAH CARR CASY CAT CB CBRE CCI CCK CCL CDNS CE CEG CELH CF CFG
CHD CHRD CHRW CHTR CHWY CI CIEN CINF CL CLX CME CMG CMI CMS CNC CNP CNX COF COHR COIN COO COP COR COST CPB CPT
CRM CRWD CSCO CSGP CSX CTRA CVNA CVS CVX D DAL DASH DD DDOG DE DECK DELL DFS DG DHI DHR DIS DLR DLTR DOC DOV
DOW DPZ DRI DTE DUK DVN DXCM EA EBAY ECL ED EIX EL ELV EMN EMR EOG EQIX EQR EQT ES ESS ETN ETR ETSY EVRG EW
EXC EXPD EXPE EXR F FANG FAST FCX FDX FE FI FIS FITB FIX FMC FOX FOXA FRT FSLR FTNT GD GE GEHC GEN GEV GILD
GIS GLW GM GOOG GOOGL GPC GPN GS GWW HAL HAS HBAN HCA HD HEI HES HIG HOLX HON HOOD HPE HPQ HRL HST HSY HUBB
HUBS HUM HWM IBM ICE IDXX IEX IFF ILMN INCY INTU INVH IP IPG IQV IR IRM ISRG ITW JBHT JCI JNJ JNPR JPM KDP KEY
KEYS KHC KIM KKR KLAC KMB KMI KO KR KVUE LCID LDOS LEN LIN LITE LLY LMT LNG LOW LRCX LUV LVS LW LYV MA MAA MAR
MAT MCD MCHP MCK MDB MDLZ MDT MET META MGM MKC MKTX MLM MMC MMM MNST MO MOH MOS MPC MPWR MRK MRNA MRO MS MSFT
MSI MTB MTCH MU NCLH NDAQ NDSN NEE NEM NET NFLX NI NKE NOC NOV NOW NUE NVDA NWS NWSA NXPI O ODFL OKE OMC ON
ORCL ORLY OTIS OXY PANW PARA PCAR PCG PEG PEP PFE PG PGR PH PHM PINS PKG PLD PLTR PM PNC PODD POOL PPG PPL PR
PRU PSA PSX PWR PYPL QCOM RBLX RCL RDDT REG REGN RF RH RIVN RMD ROK ROST RS RSG RTX SAIC SBAC SBUX SCHW SHW
SJM SLB SMCI SNAP SNPS SO SOLV SPG SPGI SPOT SRE STE STLD STT STX STZ SWK SWKS SYF SYK SYY T TAP TDG TEAM TECH
TEL TER TGT TJX TMO TMUS TOL TRGP TROW TRV TSLA TSN TT TTWO TXN TXT TYLER UAL UBER UHS ULTA UNH UNP UPS URI
USB V VEEV VICI VLO VMC VRT VRTX VST VZ W WAB WAT WBD WDC WEC WELL WFC WM WMB WMT WRB WSM WYNN XEL XOM XYL YUM
ZS ZTS
""".split()))

SECTOR = {}
for sec, names in {
"XLK":"AAPL MSFT NVDA AVGO ORCL AMD CRM ADBE QCOM TXN INTU AMAT LRCX KLAC MU PANW CRWD NOW PLTR ACN IBM CSCO ANET CDNS SNPS MSI APH GLW TEL MPWR MCHP ON NXPI ADI FSLR STX WDC DELL HPQ HPE FTNT ZS DDOG NET MDB HUBS TEAM APP ARM SMCI VRT CIEN LITE COHR KEYS TER SWKS JNPR GEN TYLER",
"XLY":"AMZN TSLA HD LOW BKNG TJX SBUX MCD NKE CMG ORLY AZO ROST MAR ABNB DASH CVNA RCL CCL NCLH DRI YUM DPZ TGT EBAY ETSY WSM RH DECK ULTA BBY GPC LEN DHI PHM TOL GM F RIVN LCID BWA APTV MGM LVS WYNN HAS MAT POOL EXPE CHWY W",
"XLC":"GOOGL GOOG META NFLX DIS T VZ TMUS CHTR TTWO EA WBD FOX FOXA PARA SPOT PINS SNAP RDDT RBLX MTCH OMC IPG LYV NWS NWSA",
"XLF":"JPM BAC WFC GS MS C AXP SCHW BLK SPGI CME ICE COF USB PNC BK STT TROW AMP AJG MMC AON CB TRV ALL PGR ACGL AFL MET PRU HIG CINF WRB RF HBAN CFG KEY FITB MTB SYF DFS V MA PYPL FI FIS GPN COIN HOOD ARES BX KKR APO NDAQ MKTX",
"XLV":"LLY UNH JNJ ABBV MRK TMO ABT DHR AMGN GILD ISRG VRTX REGN BSX MDT SYK BMY PFE ZTS CI CVS HUM ELV HCA MCK COR CAH CNC EW BDX IDXX IQV A ALGN DXCM PODD HOLX RMD BIIB MRNA BAX GEHC STE WAT COO TECH ILMN INCY UHS MOH SOLV VEEV",
"XLP":"WMT COST PG KO PEP PM MO CL MDLZ KMB GIS KR SYY KHC HSY MKC CHD CLX CAG CPB SJM HRL TSN TAP STZ ADM BG LW CELH MNST KDP EL KVUE DG DLTR CASY",
"XLI":"GE CAT DE ETN PH HON RTX LMT NOC GD UPS FDX UNP CSX WM RSG UBER BA MMM EMR ITW ROK IR CMI PWR JCI TT OTIS CARR GEV URI FAST GWW WAB HUBB XYL IEX DOV SWK NDSN PCAR ODFL JBHT EXPD CHRW DAL UAL LUV AAL TXT TDG HWM HEI AXON LDOS SAIC FIX",
"XLE":"XOM CVX COP EOG SLB MPC PSX VLO OXY WMB KMI TRGP HAL BKR FANG DVN APA MRO EQT CTRA HES OKE LNG PR CNX CHRD NOV",
"XLU":"NEE SO DUK AEP SRE EXC D DTE XEL ED PEG WEC ES AWK ETR PPL FE AEE CNP CMS NI EVRG AES PCG EIX VST CEG",
"XLB":"LIN APD SHW ECL FCX NEM NUE DD DOW PPG MLM VMC CF MOS IFF BALL AVY PKG IP AMCR CE ALB STLD RS CCK EMN FMC",
"XLRE":"AMT PLD EQIX WELL SPG O PSA DLR CCI VICI EXR AVB EQR ESS MAA INVH ARE BXP KIM REG FRT CPT HST IRM CBRE CSGP DOC SBAC"
}.items():
    for t in names.split(): SECTOR[t] = sec
SECTOR_NAME = {
"XLK":"科技","XLY":"可选消费","XLC":"通信","XLF":"金融","XLV":"医疗",
"XLP":"必需消费","XLI":"工业","XLE":"能源","XLU":"公用事业","XLB":"材料","XLRE":"房地产"
}
TOP_N = int(os.environ.get("TOP_N", "40"))
ALL_SYMBOLS = sorted(set(UNIVERSE + ["SPY"] + list(set(SECTOR.values()))))
V5_ORIGINAL_122=set("""AAPL ABBV ABT ADBE AMAT AMD AMGN AMT AMZN APD AVGO AXP AZO BAC BKNG BLK BSX C CAT CHTR CL CME CMG COF COP COST CRM CRWD CSX CVX DE DHR DIS ECL EOG EQIX ETN FCX FDX GD GE GILD GIS GOOGL GS HD HON ICE INTU ISRG JNJ JPM KLAC KMB KO LIN LLY LMT LOW LRCX MAR MCD MDLZ MDT META MO MPC MRK MS MSFT MU NEM NFLX NKE NOC NOW NUE NVDA O ORCL ORLY OXY PANW PEP PG PH PLD PLTR PM PNC PSX QCOM REGN ROST RSG RTX SBUX SCHW SHW SLB SPG SPGI SYK T TJX TMO TMUS TSLA TXN UBER UNH UNP UPS USB VLO VRTX VZ WELL WFC WM WMT XOM""".split())
V5_VALIDATION=set(UNIVERSE)-V5_ORIGINAL_122

lock = threading.Lock()
scan_thread = None
backtest_thread = None
v3_thread = None
v3_stop_event = threading.Event()
v5_validation_thread = None
v5_validation_stop_event = threading.Event()
v6_shadow_thread = None
v6_shadow_stop_event = threading.Event()
v61_fast_thread = None
v61_fast_stop_event = threading.Event()


def youtube_video_id(value):
    """Extract a YouTube video id from common youtube.com/youtu.be URLs or accept a raw id."""
    s = (value or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", s):
        return s
    try:
        u = urlparse(s)
        host = (u.netloc or "").lower().split(":")[0]
        path = (u.path or "").strip("/")
        if host in ("youtu.be", "www.youtu.be"):
            return path.split("/")[0] if path else ""
        if host.endswith("youtube.com"):
            if path == "watch":
                return (parse_qs(u.query).get("v") or [""])[0]
            parts = path.split("/")
            if len(parts) >= 2 and parts[0] in ("shorts", "embed", "live"):
                return parts[1]
    except Exception:
        pass
    m = re.search(r"(?:v=|youtu\.be/|shorts/|embed/|live/)([A-Za-z0-9_-]{11})", s)
    return m.group(1) if m else ""

def _yt_paths(video_id):
    return (
        YOUTUBE_DIR / f"{video_id}.json",
        YOUTUBE_DIR / f"{video_id}.txt",
    )

def _yt_read_index():
    if not YOUTUBE_INDEX_FILE.exists():
        return []
    try:
        x = json.loads(YOUTUBE_INDEX_FILE.read_text(encoding="utf-8"))
        return x if isinstance(x, list) else []
    except Exception:
        return []

def _yt_write_index(rows):
    YOUTUBE_INDEX_FILE.write_text(
        json.dumps(rows[:200], ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def youtube_cached(video_id):
    jp, tp = _yt_paths(video_id)
    if not jp.exists():
        return None
    try:
        return json.loads(jp.read_text(encoding="utf-8"))
    except Exception:
        return None

def _yt_choose_transcript(ytt, video_id, preferred=None):
    preferred = preferred or ["zh-Hant", "zh-TW", "zh-Hans", "zh-CN", "zh", "en"]
    tl = ytt.list(video_id)

    # Prefer a manual transcript in the requested languages, then generated, then any transcript.
    try:
        return tl.find_manually_created_transcript(preferred)
    except Exception:
        pass
    try:
        return tl.find_generated_transcript(preferred)
    except Exception:
        pass
    try:
        return tl.find_transcript(preferred)
    except Exception:
        pass

    # Fall back to the first available track so that a video with subtitles is still ingestible.
    for tr in tl:
        return tr
    raise NoTranscriptFound(video_id, preferred, {})

def youtube_ingest(url_or_id, force=False, preferred=None):
    """Fetch and persist the complete YouTube transcript. No LLM summarization happens here."""
    video_id = youtube_video_id(url_or_id)
    if not video_id:
        raise ValueError("无法识别 YouTube 视频 ID")

    if not force:
        cached = youtube_cached(video_id)
        if cached:
            cached["cache_hit"] = True
            return cached

    if YouTubeTranscriptApi is None:
        raise RuntimeError("服务器尚未安装 youtube-transcript-api；请部署新的 requirements.txt")

    ytt = YouTubeTranscriptApi()
    tr = _yt_choose_transcript(ytt, video_id, preferred=preferred)
    fetched = tr.fetch()

    snippets = []
    full_parts = []
    timestamp_lines = []
    for item in fetched:
        txt = str(getattr(item, "text", "") or "").replace("\n", " ").strip()
        if not txt:
            continue
        start = float(getattr(item, "start", 0.0) or 0.0)
        dur = float(getattr(item, "duration", 0.0) or 0.0)
        snippets.append({"text": txt, "start": start, "duration": dur})
        full_parts.append(txt)
        mm, ss = divmod(int(start), 60)
        hh, mm = divmod(mm, 60)
        stamp = f"{hh:02d}:{mm:02d}:{ss:02d}" if hh else f"{mm:02d}:{ss:02d}"
        timestamp_lines.append(f"[{stamp}] {txt}")

    full_text = " ".join(full_parts).strip()
    result = {
        "video_id": video_id,
        "source_url": f"https://www.youtube.com/watch?v={video_id}",
        "language": getattr(tr, "language", ""),
        "language_code": getattr(tr, "language_code", ""),
        "is_generated": bool(getattr(tr, "is_generated", False)),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "snippet_count": len(snippets),
        "char_count": len(full_text),
        "transcript": full_text,
        "snippets": snippets,
        "cache_hit": False,
    }

    jp, tp = _yt_paths(video_id)
    jp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    tp.write_text(
        f"YouTube: {result['source_url']}\n"
        f"Language: {result['language']} ({result['language_code']})\n"
        f"Generated captions: {result['is_generated']}\n"
        f"Fetched UTC: {result['fetched_at_utc']}\n\n"
        + "\n".join(timestamp_lines),
        encoding="utf-8"
    )

    idx = [r for r in _yt_read_index() if r.get("video_id") != video_id]
    idx.insert(0, {
        "video_id": video_id,
        "source_url": result["source_url"],
        "language": result["language"],
        "language_code": result["language_code"],
        "is_generated": result["is_generated"],
        "fetched_at_utc": result["fetched_at_utc"],
        "char_count": result["char_count"],
    })
    _yt_write_index(idx)
    return result

def youtube_ingest_list():
    return _yt_read_index()



def save_uploaded_youtube_transcript(payload):
    """Persist a transcript uploaded from the user's own Mac/residential network."""
    video_id = youtube_video_id(payload.get("video_id") or payload.get("source_url") or "")
    if not video_id:
        raise ValueError("invalid video id")

    full_text = str(payload.get("transcript") or "").strip()
    if not full_text:
        raise ValueError("empty transcript")

    snippets = payload.get("snippets") or []
    if not isinstance(snippets, list):
        snippets = []

    result = {
        "video_id": video_id,
        "source_url": payload.get("source_url") or f"https://www.youtube.com/watch?v={video_id}",
        "language": str(payload.get("language") or ""),
        "language_code": str(payload.get("language_code") or ""),
        "is_generated": bool(payload.get("is_generated", False)),
        "fetched_at_utc": str(payload.get("fetched_at_utc") or datetime.now(timezone.utc).isoformat()),
        "uploaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "snippet_count": len(snippets),
        "char_count": len(full_text),
        "transcript": full_text,
        "snippets": snippets,
        "cache_hit": False,
        "ingest_source": "mac_local_upload",
    }

    jp, tp = _yt_paths(video_id)
    jp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    timestamp_lines = []
    if snippets:
        for item in snippets:
            txt = str(item.get("text") or "").replace("\n", " ").strip()
            if not txt:
                continue
            start = float(item.get("start") or 0.0)
            mm, ss = divmod(int(start), 60)
            hh, mm = divmod(mm, 60)
            stamp = f"{hh:02d}:{mm:02d}:{ss:02d}" if hh else f"{mm:02d}:{ss:02d}"
            timestamp_lines.append(f"[{stamp}] {txt}")
    else:
        timestamp_lines = [full_text]

    tp.write_text(
        f"YouTube: {result['source_url']}\n"
        f"Language: {result['language']} ({result['language_code']})\n"
        f"Generated captions: {result['is_generated']}\n"
        f"Fetched UTC: {result['fetched_at_utc']}\n"
        f"Uploaded UTC: {result['uploaded_at_utc']}\n\n"
        + "\n".join(timestamp_lines),
        encoding="utf-8"
    )

    idx = [r for r in _yt_read_index() if r.get("video_id") != video_id]
    idx.insert(0, {
        "video_id": video_id,
        "source_url": result["source_url"],
        "language": result["language"],
        "language_code": result["language_code"],
        "is_generated": result["is_generated"],
        "fetched_at_utc": result["fetched_at_utc"],
        "char_count": result["char_count"],
        "ingest_source": result["ingest_source"],
    })
    _yt_write_index(idx)
    return result


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


def _cache_exists_good(ticker):
    p = MARKET_DIR / f"{ticker}.csv"
    return p.exists() and p.stat().st_size > 100

def _sleep_with_countdown(seconds, done_count, total_count, symbol):
    """Sleep in the background while keeping the dashboard informative."""
    end = time.time() + max(0, seconds)
    while True:
        left = int(end - time.time())
        if left <= 0:
            break
        mins = max(1, (left + 59) // 60)
        write_state(
            status="waiting_rate_limit",
            message=f"Tiingo 免费版限流；自动等待约 {mins} 分钟后继续，无需手动点击",
            last_symbol=symbol,
            updated=done_count,
            total=total_count,
            gap_seconds=current_gap,
            error=f"{symbol}: HTTP 429；系统会自动续跑"
        )
        time.sleep(min(60, left))

def _update_with_auto_429(ticker, done_count, total_count):
    """Retry the same symbol automatically after a 429 cooldown."""
    while True:
        try:
            return update_one(ticker)
        except TiingoRateLimitPause:
            _sleep_with_countdown(AUTO_429_WAIT, done_count, total_count, ticker)
            # After cooldown, retry the SAME ticker. No user action required.
            continue

def run_scan():
    global scan_thread
    cp = read_checkpoint()
    done = set(cp.get("done", []))
    failed = set(cp.get("failed", []))

    # V5.1 bootstrap mode:
    # If the expanded universe is not fully cached yet, existing cache files are
    # accepted as completed WITHOUT making another Tiingo request.
    cached_now = {t for t in ALL_SYMBOLS if _cache_exists_good(t)}
    bootstrap_mode = len(cached_now) < len(ALL_SYMBOLS)
    if bootstrap_mode:
        done |= cached_now
        failed -= cached_now
    elif len(done) >= len(ALL_SYMBOLS):
        # Once the universe is fully cached, a new manual scan means a normal
        # incremental refresh of every symbol.
        done.clear()
        failed.clear()
        clear_checkpoint()

    success_count = len(done)
    fail_count = len(failed)
    remaining = [t for t in ALL_SYMBOLS if t not in done]

    try:
        mode_text = "补齐缺失缓存" if bootstrap_mode else "正常增量更新"
        write_state(
            status="updating",
            message=f"{mode_text}：已缓存/完成 {len(done)}，剩余 {len(remaining)}；免费版自动限速，无需守着页面",
            updated=len(done), total=len(ALL_SYMBOLS), error=None,
            success_count=success_count, fail_count=fail_count, gap_seconds=current_gap
        )
        save_checkpoint(done, failed)

        for t in remaining:
            write_state(
                status="updating", last_symbol=t, updated=len(done),
                message=f"{mode_text} {t}（总进度 {len(done)}/{len(ALL_SYMBOLS)}）",
                success_count=len(done), fail_count=len(failed), gap_seconds=current_gap,
                error=None
            )
            try:
                _update_with_auto_429(t, len(done), len(ALL_SYMBOLS))
                done.add(t)
                failed.discard(t)
            except Exception as ex:
                failed.add(t)
                write_state(error=f"{t}: {type(ex).__name__}: {str(ex)[:160]}")
            save_checkpoint(done, failed)
            write_state(
                status="updating", updated=len(done),
                success_count=len(done), fail_count=len(failed),
                gap_seconds=current_gap
            )

        # Retry non-rate-limit failures once.
        retry_list = [t for t in sorted(failed) if t not in done]
        for t in retry_list:
            try:
                _update_with_auto_429(t, len(done), len(ALL_SYMBOLS))
                done.add(t)
                failed.discard(t)
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
        msg = (f"缓存/扫描完成：{len(done)}/{len(ALL_SYMBOLS)}；候选 {len(rows)}只；失败 {len(failed)}只"
               if failed else
               f"缓存/扫描完成：{len(done)}/{len(ALL_SYMBOLS)}；候选 {len(rows)}只")
        write_state(
            status="ready", message=msg, updated=len(done), total=len(ALL_SYMBOLS),
            last_scan=datetime.now(timezone.utc).isoformat(),
            error=None if not failed else f"仍有失败：{', '.join(sorted(failed))}",
            success_count=len(done), fail_count=len(failed), gap_seconds=current_gap,
            market_label=regime["label"], market_text=regime["text"], market_emoji=regime["emoji"]
        )
        if not failed:
            clear_checkpoint()

    except Exception as ex:
        save_checkpoint(done, failed)
        write_state(
            status="error", message="扫描异常，进度已保存",
            error=f"{type(ex).__name__}: {ex}", updated=len(done),
            success_count=len(done), fail_count=len(failed), gap_seconds=current_gap
        )
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
    # V4.6 engine may already persist regime/spy_ret columns into the trade CSV.
    # Drop them before merge_asof so pandas does not create regime_x/regime_y,
    # which previously caused KeyError: 'regime' in v42_regime_summary().
    left=d.sort_values("date").copy()
    left=left.drop(columns=[c for c in ["regime","spy_ret20","spy_ret60"] if c in left.columns], errors="ignore")
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


def v462_deploy_probe():
    import os, sys, hashlib, importlib.util
    rows=[]
    cwd=Path.cwd()
    candidates=[
        cwd/"jlaw_v3_phase1.py",
        Path(__file__).resolve().parent/"jlaw_v3_phase1.py",
        Path("/app/jlaw_v3_phase1.py"),
        Path("/workspace/jlaw_v3_phase1.py"),
    ]
    seen=set()
    for p in candidates:
        try:
            rp=str(p.resolve())
        except Exception:
            rp=str(p)
        if rp in seen: 
            continue
        seen.add(rp)
        info=dict(path=rp,exists=p.exists(),size=0,sha12="",has_v461=False,has_v46=False,mtime="")
        if p.exists():
            try:
                b=p.read_bytes()
                info["size"]=len(b)
                info["sha12"]=hashlib.sha256(b).hexdigest()[:12]
                s=b.decode("utf-8",errors="ignore")
                info["has_v461"]="V4.6.1-DIAG" in s
                info["has_v46"]="V4.6 SHADOW GATE" in s or "v46_diag" in s
                info["mtime"]=str(p.stat().st_mtime)
            except Exception as e:
                info["error"]=repr(e)
        rows.append(info)

    imported={}
    try:
        import jlaw_v3_phase1 as jm
        imported["module_file"]=str(getattr(jm,"__file__",""))
        imported["has_diag_attr"]=hasattr(jm,"V46_SCHEMES") or hasattr(jm,"_v46_adaptive_score")
        try:
            ip=Path(imported["module_file"])
            if ip.exists():
                b=ip.read_bytes()
                imported["sha12"]=hashlib.sha256(b).hexdigest()[:12]
                s=b.decode("utf-8",errors="ignore")
                imported["contains_v461"]="V4.6.1-DIAG" in s
        except Exception:
            pass
    except Exception as e:
        imported["import_error"]=f"{type(e).__name__}: {e}"

    return dict(
        cwd=str(cwd),
        app_file=str(Path(__file__).resolve()),
        python=sys.version.split()[0],
        candidates=rows,
        imported=imported
    )


def v47_true_engine_threshold_sweep():
    """Threshold robustness using REAL V4.6 engine trades, not app-side re-ranking."""
    base=v3_result_dir()
    p=base/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:
        d=pd.read_csv(p)
    except Exception:
        return []
    if d.empty or "v46_score" not in d.columns:return []
    d["v46_score"]=pd.to_numeric(d["v46_score"],errors="coerce")
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    rows=[]
    for th in [70,72.5,75,77.5,80,82.5,85,87.5,90,92.5,95]:
        g=d[d["v46_score"]>=th].copy().sort_values("date")
        if g.empty:continue
        m=_v41_metrics(g)
        old=g[g["date"].dt.year<=2021]
        rec=g[g["date"].dt.year>=2022]
        om=_v41_metrics(old) if not old.empty else {}
        rm=_v41_metrics(rec) if not rec.empty else {}
        rows.append(dict(
            threshold=th,**m,
            old_trades=om.get("trades",0),old_win=om.get("win_rate",np.nan),
            old_avg=om.get("avg_return",np.nan),old_pf=om.get("pf",np.nan),
            recent_trades=rm.get("trades",0),recent_win=rm.get("win_rate",np.nan),
            recent_avg=rm.get("avg_return",np.nan),recent_pf=rm.get("pf",np.nan)
        ))
    return rows

def v47_true_engine_periods():
    base=v3_result_dir()
    p=base/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:d=pd.read_csv(p)
    except Exception:return []
    if d.empty:return []
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    d["v46_score"]=pd.to_numeric(d.get("v46_score"),errors="coerce")
    rows=[]
    for th in [70,85,90]:
        x=d[d["v46_score"]>=th].copy()
        for lo,hi,label in [(2007,2011,"2007-2011"),(2012,2016,"2012-2016"),
                            (2017,2021,"2017-2021"),(2022,2026,"2022-2026")]:
            g=x[(x["date"].dt.year>=lo)&(x["date"].dt.year<=hi)].sort_values("date")
            if g.empty:continue
            m=_v41_metrics(g)
            rows.append(dict(threshold=th,period=label,**m))
    return rows

def v47_threshold_plateau():
    """A conservative plateau detector: prefer thresholds with >=30 trades and PF>=1.5."""
    rows=v47_true_engine_threshold_sweep()
    if not rows:return dict(status="NO_DATA",note="No V4.6 trades")
    z=pd.DataFrame(rows)
    eligible=z[(z.trades>=30)&(z.pf>=1.5)&(z.avg_return>0)].copy()
    if eligible.empty:
        return dict(status="NO_STABLE_PLATEAU",note="No threshold has >=30 trades, PF>=1.5 and positive average return.")
    # Reward PF/return but penalize tiny samples.
    eligible["robust_score"]=(eligible["avg_return"]*100)+np.minimum(eligible["pf"],4)*0.7+np.minimum(eligible["trades"],100)/100*0.4
    best=eligible.sort_values(["robust_score","trades"],ascending=[False,False]).iloc[0]
    return dict(
        status="CANDIDATE",
        threshold=float(best.threshold),
        trades=int(best.trades),
        win_rate=float(best.win_rate),
        avg_return=float(best.avg_return),
        pf=float(best.pf),
        note="Candidate only; use neighboring thresholds and period table to judge plateau, not a single best point."
    )


def v48_dual_tier_summary():
    """Two-tier interpretation of REAL V4.6 engine trades.
    CORE keeps breadth; ELITE is a priority layer, not a replacement for CORE.
    """
    base=v3_result_dir()
    p=base/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:d=pd.read_csv(p)
    except Exception:return []
    if d.empty or "v46_score" not in d.columns:return []
    d["v46_score"]=pd.to_numeric(d["v46_score"],errors="coerce")
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    tiers=[
        ("CORE >=70", d[d["v46_score"]>=70]),
        ("CORE+ >=77.5", d[d["v46_score"]>=77.5]),
        ("ELITE >=87.5", d[d["v46_score"]>=87.5]),
        ("SUPER >=90", d[d["v46_score"]>=90]),
    ]
    rows=[]
    for name,g in tiers:
        g=g.copy().sort_values("date")
        if g.empty:continue
        m=_v41_metrics(g)
        old=g[g["date"].dt.year<=2021]
        rec=g[g["date"].dt.year>=2022]
        om=_v41_metrics(old) if not old.empty else {}
        rm=_v41_metrics(rec) if not rec.empty else {}
        rows.append(dict(
            tier=name,**m,
            old_trades=om.get("trades",0),old_avg=om.get("avg_return",np.nan),old_pf=om.get("pf",np.nan),
            recent_trades=rm.get("trades",0),recent_avg=rm.get("avg_return",np.nan),recent_pf=rm.get("pf",np.nan)
        ))
    return rows

def v48_score_bins():
    """Non-overlapping score bins to see what each score band itself contributes."""
    base=v3_result_dir()
    p=base/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:d=pd.read_csv(p)
    except Exception:return []
    if d.empty or "v46_score" not in d.columns:return []
    d["v46_score"]=pd.to_numeric(d["v46_score"],errors="coerce")
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    bands=[(70,75),(75,80),(80,82.5),(82.5,85),(85,87.5),(87.5,90),(90,101)]
    rows=[]
    for lo,hi in bands:
        g=d[(d["v46_score"]>=lo)&(d["v46_score"]<hi)].copy().sort_values("date")
        if g.empty:continue
        m=_v41_metrics(g)
        rows.append(dict(score_band=f"{lo:g}-{hi:g}",**m))
    return rows


V5_DEFERRED_SYMBOLS={"EQR","MMC","TYLER"}

def v50_expanded_universe_status():
    cached=[t for t in UNIVERSE if (MARKET_DIR/f"{t}.csv").exists()]
    missing=[t for t in UNIVERSE if not (MARKET_DIR/f"{t}.csv").exists()]
    blocking=[t for t in missing if t not in V5_DEFERRED_SYMBOLS]
    validation_cached=[t for t in V5_VALIDATION if (MARKET_DIR/f"{t}.csv").exists()]
    validation_missing=[t for t in V5_VALIDATION if not (MARKET_DIR/f"{t}.csv").exists()]
    validation_usable=[t for t in validation_cached if t not in V5_DEFERRED_SYMBOLS]
    return dict(
        universe=len(UNIVERSE),original=len(V5_ORIGINAL_122),validation=len(V5_VALIDATION),
        cached=len(cached),missing=len(missing),
        original_cached=sum((MARKET_DIR/f"{t}.csv").exists() for t in V5_ORIGINAL_122),
        validation_cached=len(validation_cached),
        validation_usable=len(validation_usable),
        validation_missing=len(validation_missing),
        ready=(len(blocking)==0),
        deferred=" ".join(sorted(V5_DEFERRED_SYMBOLS & set(missing))),
        next_missing=" ".join(missing[:30])
    )

def v50_validation_results():
    p=V5_VALIDATION_DIR/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:d=pd.read_csv(p)
    except Exception:return []
    if d.empty or "v5_cohort" not in d.columns:return []
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    d["v46_score"]=pd.to_numeric(d.get("v46_score"),errors="coerce")
    rows=[]
    for cohort in ["ORIGINAL_122","NEW_VALIDATION"]:
        x=d[d["v5_cohort"]==cohort].copy()
        for th in [70,87.5,90]:
            g=x[x["v46_score"]>=th].sort_values("date")
            if g.empty:continue
            rows.append(dict(cohort=cohort,threshold=th,**_v41_metrics(g)))
    return rows


def read_v5_validation_status():
    s={
        "status":"idle","message":"尚未运行新增324独立验证",
        "stocks":0,"processed":0,"current":"","error":None,
        "started_at":None,"finished_at":None,
        "requested":len(V5_VALIDATION),"usable":0,"skipped":[]
    }
    if V5_VALIDATION_STATUS_FILE.exists():
        try:s.update(json.loads(V5_VALIDATION_STATUS_FILE.read_text(encoding="utf-8")))
        except Exception:pass
    return s

def write_v5_validation_status(**kw):
    V5_VALIDATION_DIR.mkdir(parents=True,exist_ok=True)
    s=read_v5_validation_status(); s.update(kw)
    V5_VALIDATION_STATUS_FILE.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")

def v5_validation_summary():
    p=V5_VALIDATION_DIR/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return []
    try:
        d=pd.read_csv(p)
    except Exception:
        return []
    if d.empty or "v46_score" not in d.columns:
        return []
    d["date"]=pd.to_datetime(d["date"],errors="coerce")
    d["v46_score"]=pd.to_numeric(d["v46_score"],errors="coerce")
    rows=[]
    for th,label in [(70,"CORE ≥70"),(87.5,"ELITE ≥87.5"),(90,"SUPER ≥90")]:
        g=d[d["v46_score"]>=th].copy().sort_values("date")
        if g.empty:
            rows.append(dict(tier=label,threshold=th,trades=0,win_rate=0,avg_return=0,pf=0,max_losing_streak=0))
            continue
        met=_v41_metrics(g)
        rows.append(dict(tier=label,threshold=th,**met))
    return rows

def run_v5_validation():
    global v5_validation_thread
    try:
        from jlaw_v3_phase1 import run as vrun
        V5_VALIDATION_DIR.mkdir(parents=True,exist_ok=True)
        v5_validation_stop_event.clear()
        try:
            if V5_VALIDATION_STOP_FILE.exists(): V5_VALIDATION_STOP_FILE.unlink()
        except Exception:
            pass

        available=sorted(t for t in V5_VALIDATION if (MARKET_DIR/f"{t}.csv").exists())
        skipped=sorted(t for t in V5_VALIDATION if t not in set(available))
        # Never fail the OOS run merely because EQR/MMC/TYLER are deferred.
        usable=[t for t in available if t not in V5_DEFERRED_SYMBOLS]

        if len(usable) < 250:
            raise RuntimeError(f"独立验证可用股票只有 {len(usable)} 只，低于安全下限250，只停止不运行")

        write_v5_validation_status(
            status="running",
            message=f"正在运行冻结规则独立验证：{len(usable)}/{len(V5_VALIDATION)}只；缺失自动跳过，不改参数",
            requested=len(V5_VALIDATION),usable=len(usable),skipped=skipped,
            stocks=len(usable),processed=0,current="",
            started_at=datetime.now(timezone.utc).isoformat(),finished_at=None,error=None
        )

        def prog(done,total,t):
            if v5_validation_stop_event.is_set():
                raise RuntimeError("__V5_VALIDATION_STOPPED__")
            write_v5_validation_status(
                status="running",
                message=f"Validation 324：{done}/{total} · {t}",
                stocks=total,processed=done,current=t
            )

        vrun(
            str(MARKET_DIR),str(V5_VALIDATION_DIR),
            progress_cb=prog,quick=False,
            stop_file=str(V5_VALIDATION_STOP_FILE),
            ticker_filter=usable
        )

        if V5_VALIDATION_STOP_FILE.exists() or v5_validation_stop_event.is_set():
            raise RuntimeError("__V5_VALIDATION_STOPPED__")

        write_v5_validation_status(
            status="done",
            message=f"独立验证完成：实际 {len(usable)} 只 / 目标 {len(V5_VALIDATION)}只；缺失 {len(skipped)} 只已跳过",
            stocks=len(usable),processed=len(usable),current="",
            requested=len(V5_VALIDATION),usable=len(usable),skipped=skipped,
            finished_at=datetime.now(timezone.utc).isoformat(),error=None
        )
    except Exception as ex:
        if str(ex)=="__V5_VALIDATION_STOPPED__":
            s=read_v5_validation_status()
            write_v5_validation_status(
                status="stopped",message="已停止新增股票独立验证",
                processed=s.get("processed",0),stocks=s.get("stocks",0),
                current="",finished_at=datetime.now(timezone.utc).isoformat(),error=None
            )
        else:
            traceback.print_exc()
            write_v5_validation_status(
                status="error",message="新增股票独立验证失败",
                error=f"{type(ex).__name__}: {ex}",
                finished_at=datetime.now(timezone.utc).isoformat()
            )
    finally:
        v5_validation_thread=None
        v5_validation_stop_event.clear()

def start_v5_validation():
    global v5_validation_thread
    if v5_validation_thread is not None and v5_validation_thread.is_alive():
        return False
    v5_validation_thread=threading.Thread(target=run_v5_validation,daemon=True)
    v5_validation_thread.start()
    return True

def stop_v5_validation():
    V5_VALIDATION_DIR.mkdir(parents=True,exist_ok=True)
    try: V5_VALIDATION_STOP_FILE.write_text("stop",encoding="utf-8")
    except Exception: pass
    v5_validation_stop_event.set()
    return True



def _oos_metrics(g):
    if g is None or len(g)==0 or "return_pct" not in g.columns:
        return dict(trades=0,win_rate=0.0,avg_return=0.0,pf=0.0,max_losing_streak=0,
                    avg_win=0.0,avg_loss=0.0,target_rate=0.0,stop_rate=0.0)
    x=pd.to_numeric(g["return_pct"],errors="coerce").dropna()
    if x.empty:
        return dict(trades=0,win_rate=0.0,avg_return=0.0,pf=0.0,max_losing_streak=0,
                    avg_win=0.0,avg_loss=0.0,target_rate=0.0,stop_rate=0.0)
    wins=x[x>0]; losses=x[x<0]
    pf=(wins.sum()/(-losses.sum())) if len(losses) and -losses.sum()>0 else (999.0 if len(wins) else 0.0)
    streak=0; mx=0
    for v in x.tolist():
        if v<0: streak+=1; mx=max(mx,streak)
        else: streak=0
    reason=g["reason"].astype(str) if "reason" in g.columns else pd.Series([],dtype=str)
    return dict(
        trades=int(len(x)),
        win_rate=float((x>0).mean()),
        avg_return=float(x.mean()),
        pf=float(pf),
        max_losing_streak=int(mx),
        avg_win=float(wins.mean()) if len(wins) else 0.0,
        avg_loss=float(losses.mean()) if len(losses) else 0.0,
        target_rate=float(reason.str.startswith("TARGET").mean()) if len(reason) else 0.0,
        stop_rate=float(reason.str.startswith("STOP").mean()) if len(reason) else 0.0,
    )

def _oos_band_rows(d,col,bands):
    if col not in d.columns: return []
    x=d.copy()
    x[col]=pd.to_numeric(x[col],errors="coerce")
    rows=[]
    for lo,hi,label in bands:
        g=x[(x[col]>=lo)&(x[col]<hi)]
        if len(g)==0: continue
        m=_oos_metrics(g)
        rows.append(dict(factor=col,band=label,lo=lo,hi=hi,**m))
    return rows

def v5_oos_autopsy():
    """Descriptive OOS failure autopsy only. No parameter fitting and no rerun."""
    p=V5_VALIDATION_DIR/"jlaw_v46_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return dict(available=False,message="Validation 交易文件不存在")
    try:
        d=pd.read_csv(p)
    except Exception as e:
        return dict(available=False,message=f"读取失败: {type(e).__name__}: {e}")
    if d.empty:
        return dict(available=False,message="Validation 交易文件为空")

    for c in ["return_pct","v46_score","volratio","quality","rs","supply_score",
              "sector_score","leader_score","pivot_tight","group_breadth","mfe","mae","hold_days"]:
        if c in d.columns: d[c]=pd.to_numeric(d[c],errors="coerce")
    if "date" in d.columns:
        d["date"]=pd.to_datetime(d["date"],errors="coerce")
        d["year"]=d["date"].dt.year

    overall=_oos_metrics(d)
    core=_oos_metrics(d[d["v46_score"]>=70]) if "v46_score" in d.columns else overall
    elite=_oos_metrics(d[d["v46_score"]>=87.5]) if "v46_score" in d.columns else {}
    superm=_oos_metrics(d[d["v46_score"]>=90]) if "v46_score" in d.columns else {}

    score_bins=[]
    if "v46_score" in d.columns:
        for lo,hi,label in [
            (70,75,"70–75"),(75,80,"75–80"),(80,82.5,"80–82.5"),
            (82.5,85,"82.5–85"),(85,87.5,"85–87.5"),
            (87.5,90,"87.5–90"),(90,101,"90+")
        ]:
            g=d[(d.v46_score>=lo)&(d.v46_score<hi)]
            if len(g): score_bins.append(dict(band=label,**_oos_metrics(g)))

    factor_rows=[]
    factor_rows += _oos_band_rows(d,"volratio",[
        (0,1.2,"<1.2"),(1.2,1.5,"1.2–1.5"),(1.5,1.8,"1.5–1.8"),
        (1.8,2.5,"1.8–2.5"),(2.5,3.0,"2.5–3.0"),(3.0,99,">=3.0")])
    factor_rows += _oos_band_rows(d,"quality",[
        (0,73,"<73"),(73,76,"73–76"),(76,82,"76–82"),(82,85,"82–85"),(85,101,"85+")])
    factor_rows += _oos_band_rows(d,"supply_score",[
        (0,40,"<40"),(40,50,"40–50"),(50,75,"50–75"),(75,90,"75–90"),(90,101,"90+")])
    factor_rows += _oos_band_rows(d,"sector_score",[
        (0,60,"<60"),(60,65,"60–65"),(65,80,"65–80"),(80,101,"80+")])
    factor_rows += _oos_band_rows(d,"leader_score",[
        (0,60,"<60"),(60,70,"60–70"),(70,90,"70–90"),(90,101,"90+")])
    factor_rows += _oos_band_rows(d,"rs",[
        (0,72,"<72"),(72,80,"72–80"),(80,90,"80–90"),(90,101,"90+")])

    regimes=[]
    if "regime" in d.columns:
        for name,g in d.groupby("regime",dropna=False):
            regimes.append(dict(regime=str(name),**_oos_metrics(g)))

    eras=[]
    if "year" in d.columns:
        for lo,hi,label in [(2007,2011,"2007–11"),(2012,2016,"2012–16"),(2017,2021,"2017–21"),(2022,2026,"2022–26")]:
            g=d[(d.year>=lo)&(d.year<=hi)]
            if len(g): eras.append(dict(era=label,**_oos_metrics(g)))

    exits=[]
    if "reason" in d.columns:
        for name,g in d.groupby("reason",dropna=False):
            exits.append(dict(reason=str(name),count=int(len(g)),avg_return=float(pd.to_numeric(g.return_pct,errors="coerce").mean())))

    top_losers=[]
    cols=[c for c in ["ticker","date","v46_score","return_pct","volratio","quality","rs","sector_score","leader_score","supply_score","regime","reason"] if c in d.columns]
    if cols:
        q=d.sort_values("return_pct",ascending=True).head(25)[cols].copy()
        for _,r in q.iterrows():
            z={}
            for c in cols:
                v=r[c]
                if pd.isna(v): z[c]=""
                elif isinstance(v,pd.Timestamp): z[c]=v.strftime("%Y-%m-%d")
                elif c=="date":
                    try:z[c]=pd.to_datetime(v).strftime("%Y-%m-%d")
                    except:z[c]=str(v)
                elif isinstance(v,(float,int,np.floating,np.integer)): z[c]=float(v)
                else:z[c]=str(v)
            top_losers.append(z)

    # Compare the failed high-score tier to CORE winners/losses descriptively.
    profile=[]
    numeric=[c for c in ["v46_score","volratio","quality","rs","sector_score","leader_score","supply_score","pivot_tight","group_breadth","mfe","mae","hold_days"] if c in d.columns]
    if "v46_score" in d.columns and "return_pct" in d.columns:
        groups=[
            ("CORE winners",d[(d.v46_score>=70)&(d.return_pct>0)]),
            ("CORE losers",d[(d.v46_score>=70)&(d.return_pct<0)]),
            ("ELITE+ winners",d[(d.v46_score>=87.5)&(d.return_pct>0)]),
            ("ELITE+ losers",d[(d.v46_score>=87.5)&(d.return_pct<0)]),
            ("SUPER winners",d[(d.v46_score>=90)&(d.return_pct>0)]),
            ("SUPER losers",d[(d.v46_score>=90)&(d.return_pct<0)]),
        ]
        for name,g in groups:
            if len(g)==0: continue
            row={"group":name,"n":int(len(g))}
            for c in numeric:
                row[c]=float(pd.to_numeric(g[c],errors="coerce").median()) if pd.to_numeric(g[c],errors="coerce").notna().any() else None
            profile.append(row)

    # Explicit diagnostic flags from OOS facts, not fitted thresholds.
    flags=[]
    if core.get("pf",0)<1.2:
        flags.append("CORE 的 PF 接近 1，底层信号只有很薄的 OOS 优势。")
    if elite.get("pf",999)<1:
        flags.append("ELITE 在 OOS 中 PF<1，高分层没有泛化。")
    if superm.get("pf",999)<elite.get("pf",999):
        flags.append("SUPER 比 ELITE 更差，评分与结果呈反向关系。")
    if superm.get("trades",0)>=30:
        flags.append("SUPER 样本已超过30笔，不能只用“小样本噪音”解释。")

    return dict(
        available=True,rows=int(len(d)),overall=overall,core=core,elite=elite,super=superm,
        score_bins=score_bins,factor_rows=factor_rows,regimes=regimes,eras=eras,exits=exits,
        top_losers=top_losers,profile=profile,flags=flags,
        generated_at=datetime.now(timezone.utc).isoformat()
    )

def export_v5_oos_autopsy_csv():
    a=v5_oos_autopsy()
    if not a.get("available"): return None
    rows=[]
    for r in a.get("score_bins",[]):
        rows.append(dict(section="score_bin",name=r["band"],**{k:v for k,v in r.items() if k!="band"}))
    for r in a.get("factor_rows",[]):
        rows.append(dict(section=f"factor:{r['factor']}",name=r["band"],**{k:v for k,v in r.items() if k not in ("factor","band")}))
    for r in a.get("regimes",[]):
        rows.append(dict(section="regime",name=r["regime"],**{k:v for k,v in r.items() if k!="regime"}))
    for r in a.get("eras",[]):
        rows.append(dict(section="era",name=r["era"],**{k:v for k,v in r.items() if k!="era"}))
    out=V5_VALIDATION_DIR/"v5_oos_autopsy.csv"
    pd.DataFrame(rows).to_csv(out,index=False)
    return out



def read_v6_shadow_status():
    s={
        "status":"idle","message":"尚未运行 V6 Shadow",
        "stocks":0,"processed":0,"current":"","error":None,
        "started_at":None,"finished_at":None
    }
    if V6_SHADOW_STATUS_FILE.exists():
        try:s.update(json.loads(V6_SHADOW_STATUS_FILE.read_text(encoding="utf-8")))
        except Exception:pass
    return s

def write_v6_shadow_status(**kw):
    V6_SHADOW_DIR.mkdir(parents=True,exist_ok=True)
    s=read_v6_shadow_status(); s.update(kw)
    V6_SHADOW_STATUS_FILE.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")

def _v6_shadow_metrics(g):
    if g is None or len(g)==0 or "return_pct" not in g.columns:
        return dict(trades=0,win_rate=0.0,avg_return=0.0,pf=0.0,max_losing_streak=0)
    x=pd.to_numeric(g["return_pct"],errors="coerce").dropna()
    if x.empty:
        return dict(trades=0,win_rate=0.0,avg_return=0.0,pf=0.0,max_losing_streak=0)
    w=x[x>0]; l=x[x<0]
    pf=(w.sum()/(-l.sum())) if len(l) and -l.sum()>0 else (999.0 if len(w) else 0.0)
    streak=0; mx=0
    for v in x.tolist():
        if v<0: streak+=1; mx=max(mx,streak)
        else: streak=0
    return dict(trades=int(len(x)),win_rate=float((x>0).mean()),avg_return=float(x.mean()),
                pf=float(pf),max_losing_streak=int(mx))

def v6_shadow_summary():
    p=V6_SHADOW_DIR/"jlaw_v6_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return []
    try:d=pd.read_csv(p)
    except Exception:return []
    if d.empty:return []
    rows=[]
    segs=[("ALL V6",d)]
    if "v6_tier" in d.columns:
        segs += [
            ("A1",d[d.v6_tier=="A1"]),
            ("A1+A2",d[d.v6_tier.isin(["A1","A2"])]),
        ]
    if "v6_top3" in d.columns:
        top3=d[d.v6_top3.astype(str).str.lower().isin(["true","1"])]
        segs.append(("TOP3",top3))
    for label,g in segs:
        if len(g):
            rows.append(dict(segment=label,**_v6_shadow_metrics(g)))
    return rows

def v6_shadow_character_rows():
    p=V6_SHADOW_DIR/"jlaw_v6_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:d=pd.read_csv(p)
    except Exception:return []
    if d.empty or "v6_market_character" not in d.columns:return []
    rows=[]
    for name,g in d.groupby("v6_market_character",dropna=False):
        rows.append(dict(character=str(name),**_v6_shadow_metrics(g)))
    return rows

def run_v6_shadow():
    global v6_shadow_thread
    try:
        import importlib.util
        engine_path=Path(__file__).with_name("jlaw_v6_0_DECISION_SHADOW.py")
        if not engine_path.exists():
            raise RuntimeError("GitHub 根目录缺少 jlaw_v6_0_DECISION_SHADOW.py")

        spec=importlib.util.spec_from_file_location("jlaw_v6_shadow_engine",str(engine_path))
        mod=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        V6_SHADOW_DIR.mkdir(parents=True,exist_ok=True)
        v6_shadow_stop_event.clear()
        try:
            if V6_SHADOW_STOP_FILE.exists():V6_SHADOW_STOP_FILE.unlink()
        except Exception:pass

        available=sorted(t for t in V5_VALIDATION if (MARKET_DIR/f"{t}.csv").exists())
        if len(available)<250:
            raise RuntimeError(f"V6 Shadow 可用验证股票只有 {len(available)} 只，低于安全下限250")

        write_v6_shadow_status(
            status="running",
            message=f"V6 Shadow 正在运行：{len(available)}只；与 V5 结果分开保存",
            stocks=len(available),processed=0,current="",
            started_at=datetime.now(timezone.utc).isoformat(),
            finished_at=None,error=None
        )

        def prog(done,total,t):
            if v6_shadow_stop_event.is_set():
                raise RuntimeError("__V6_SHADOW_STOPPED__")
            write_v6_shadow_status(
                status="running",
                message=f"V6 Shadow：{done}/{total} · {t}",
                stocks=total,processed=done,current=t
            )

        mod.run(
            str(MARKET_DIR),str(V6_SHADOW_DIR),
            progress_cb=prog,quick=False,
            stop_file=str(V6_SHADOW_STOP_FILE),
            ticker_filter=available
        )

        if V6_SHADOW_STOP_FILE.exists() or v6_shadow_stop_event.is_set():
            raise RuntimeError("__V6_SHADOW_STOPPED__")

        write_v6_shadow_status(
            status="done",
            message=f"V6 Shadow 完成：{len(available)}只。请看 V6 Shadow 结果，不覆盖 V5。",
            stocks=len(available),processed=len(available),current="",
            finished_at=datetime.now(timezone.utc).isoformat(),error=None
        )
    except Exception as ex:
        if str(ex)=="__V6_SHADOW_STOPPED__":
            s=read_v6_shadow_status()
            write_v6_shadow_status(
                status="stopped",message="已停止 V6 Shadow",
                stocks=s.get("stocks",0),processed=s.get("processed",0),
                current="",finished_at=datetime.now(timezone.utc).isoformat(),error=None
            )
        else:
            traceback.print_exc()
            write_v6_shadow_status(
                status="error",message="V6 Shadow 运行失败",
                error=f"{type(ex).__name__}: {ex}",
                finished_at=datetime.now(timezone.utc).isoformat()
            )
    finally:
        v6_shadow_thread=None
        v6_shadow_stop_event.clear()

def start_v6_shadow():
    global v6_shadow_thread
    if v6_shadow_thread is not None and v6_shadow_thread.is_alive():
        return False
    v6_shadow_thread=threading.Thread(target=run_v6_shadow,daemon=True)
    v6_shadow_thread.start()
    return True

def stop_v6_shadow():
    V6_SHADOW_DIR.mkdir(parents=True,exist_ok=True)
    try:V6_SHADOW_STOP_FILE.write_text("stop",encoding="utf-8")
    except Exception:pass
    v6_shadow_stop_event.set()
    return True



def v61_decision_lab():
    """Analyze completed V6 Shadow outputs only. No rerun."""
    p=V6_SHADOW_DIR/"jlaw_v6_trades.csv"
    if not p.exists() or p.stat().st_size==0:
        return dict(available=False,message="V6 Shadow 交易文件不存在")
    try:d=pd.read_csv(p)
    except Exception as e:return dict(available=False,message=f"读取失败: {type(e).__name__}: {e}")
    if d.empty:return dict(available=False,message="V6 Shadow 交易文件为空")

    for c in ["return_pct","v6_edge_count","v6_rank","rs","volratio","supply_score","extension"]:
        if c in d.columns:d[c]=pd.to_numeric(d[c],errors="coerce")

    matrix=[]
    if "v6_market_character" in d.columns:
        for ch in ["POWER_TREND","BULL_TREND"]:
            base=d[d["v6_market_character"].astype(str)==ch]
            for tier in ["ALL","A1","A1+A2","TOP3"]:
                g=base
                if tier=="A1" and "v6_tier" in g.columns:g=g[g.v6_tier.astype(str)=="A1"]
                elif tier=="A1+A2" and "v6_tier" in g.columns:g=g[g.v6_tier.astype(str).isin(["A1","A2"])]
                elif tier=="TOP3" and "v6_top3" in g.columns:g=g[g.v6_top3.astype(str).str.lower().isin(["true","1"])]
                if len(g):matrix.append(dict(character=ch,tier=tier,**_v6_shadow_metrics(g)))

    edges=[]
    if "v6_edge_count" in d.columns:
        for n in sorted(d.v6_edge_count.dropna().unique()):
            g=d[d.v6_edge_count==n]
            if len(g):edges.append(dict(edge_count=int(n),**_v6_shadow_metrics(g)))

    ranks=[]
    if "v6_rank" in d.columns:
        for label,mask in [("Rank 1",d.v6_rank==1),("Rank 2",d.v6_rank==2),("Rank 3",d.v6_rank==3),("Rank 4+",d.v6_rank>=4)]:
            g=d[mask]
            if len(g):ranks.append(dict(rank=label,**_v6_shadow_metrics(g)))

    char=d.v6_market_character.astype(str) if "v6_market_character" in d.columns else pd.Series("",index=d.index)
    tier=d.v6_tier.astype(str) if "v6_tier" in d.columns else pd.Series("",index=d.index)
    combos=[("POWER + A1",(char=="POWER_TREND")&(tier=="A1")),
            ("POWER + A1/A2",(char=="POWER_TREND")&(tier.isin(["A1","A2"])))]
    if "extension" in d.columns:combos.append(("POWER + entry≤2%",(char=="POWER_TREND")&(d.extension<=.02)))
    if "volratio" in d.columns:combos.append(("POWER + volume≥1.8x",(char=="POWER_TREND")&(d.volratio>=1.8)))
    if "supply_score" in d.columns:combos.append(("POWER + supply≥75",(char=="POWER_TREND")&(d.supply_score>=75)))
    if "rs" in d.columns:combos.append(("POWER + RS≥90",(char=="POWER_TREND")&(d.rs>=90)))
    opportunity=[]
    for name,mask in combos:
        g=d[mask.fillna(False)]
        if len(g):opportunity.append(dict(name=name,**_v6_shadow_metrics(g)))

    return dict(available=True,matrix=matrix,edges=edges,ranks=ranks,opportunity=opportunity)


def read_v63_status():
    base={"state":"idle","message":"尚未运行 V6.3","done":0,"total":321,"ticker":"","error":"","started_at":""}
    if V63_STATUS_FILE.exists():
        try:base.update(json.loads(V63_STATUS_FILE.read_text(encoding="utf-8")))
        except Exception:pass
    return base

def write_v63_status(**kw):
    V63_DIR.mkdir(parents=True,exist_ok=True)
    cur=read_v63_status();cur.update(kw)
    V63_STATUS_FILE.write_text(json.dumps(cur,ensure_ascii=False,indent=2),encoding="utf-8")

def v63_summary():
    p=V63_DIR/"full"/"jlaw_v6_trades.csv"
    if not p.exists() or p.stat().st_size==0:return {"available":False}
    try:T=pd.read_csv(p)
    except Exception:return {"available":False}
    if T.empty:return {"available":False}
    rows=[]
    def add(name,g):
        if g is None or g.empty:return
        m=_v6_shadow_metrics(g)
        rows.append({"name":name,**m})
    add("ALL V6.3",T)
    if "v6_market_character" in T.columns:
        add("POWER_TREND",T[T["v6_market_character"].astype(str)=="POWER_TREND"])
    if "rs" in T.columns:
        add("RS2 ≥90",T[pd.to_numeric(T["rs"],errors="coerce")>=90])
        add("RS2 90–95",T[(pd.to_numeric(T["rs"],errors="coerce")>=90)&(pd.to_numeric(T["rs"],errors="coerce")<95)])
        add("RS2 ≥95",T[pd.to_numeric(T["rs"],errors="coerce")>=95])
    if "volratio" in T.columns and "rs" in T.columns and "v6_market_character" in T.columns:
        m=(T["v6_market_character"].astype(str)=="POWER_TREND")&(pd.to_numeric(T["volratio"],errors="coerce")>=1.8)&(pd.to_numeric(T["rs"],errors="coerce")>=90)
        add("POWER + Vol≥1.8x + RS2≥90",T[m])

    eras=[]; recent=[]; rsbands=[]
    if "date" in T.columns:
        T["date"]=pd.to_datetime(T["date"],errors="coerce")
        for nm,st,en in [("2007–2011","2007-01-01","2011-12-31"),("2012–2016","2012-01-01","2016-12-31"),
                         ("2017–2021","2017-01-01","2021-12-31"),("2022–2026","2022-01-01","2026-12-31")]:
            g=T[(T["date"]>=pd.Timestamp(st))&(T["date"]<=pd.Timestamp(en))]
            if len(g): eras.append({"name":nm,**_v6_shadow_metrics(g)})

        R=T[(T["date"]>=pd.Timestamp("2022-01-01"))&(T["date"]<=pd.Timestamp("2026-12-31"))].copy()
        if len(R):
            def radd(nm,g):
                if len(g): recent.append({"name":nm,**_v6_shadow_metrics(g)})
            radd("2022–26 ALL",R)
            if "v6_market_character" in R.columns:
                radd("2022–26 POWER",R[R["v6_market_character"].astype(str)=="POWER_TREND"])
            if "rs" in R.columns:
                rr=pd.to_numeric(R["rs"],errors="coerce")
                radd("2022–26 RS2≥90",R[rr>=90])
            if all(c in R.columns for c in ["v6_market_character","rs","volratio"]):
                rr=pd.to_numeric(R["rs"],errors="coerce"); vv=pd.to_numeric(R["volratio"],errors="coerce")
                mm=(R["v6_market_character"].astype(str)=="POWER_TREND")&(rr>=90)&(vv>=1.8)
                radd("2022–26 POWER+Vol≥1.8+RS2≥90",R[mm])

    if "rs" in T.columns:
        rr=pd.to_numeric(T["rs"],errors="coerce")
        for nm,lo,hi in [("<80",None,80),("80–90",80,90),("90–95",90,95),("95–98",95,98),("98–100",98,100.0001)]:
            m=pd.Series(True,index=T.index)
            if lo is not None:m &= rr>=lo
            if hi is not None:m &= rr<hi
            g=T[m]
            if len(g): rsbands.append({"name":nm,**_v6_shadow_metrics(g)})

    cohort_rows=[]
    recent_rs2_bands=[]

    # Split by original research cohort vs validation cohort using ticker lists already defined in app.
    if "ticker" in T.columns:
        tick=T["ticker"].astype(str)
        original_set=set(V5_ORIGINAL_122)
        validation_set=set(V5_VALIDATION)
        cohort_defs=[
            ("ORIGINAL_122", T[tick.isin(original_set)]),
            ("VALIDATION_324", T[tick.isin(validation_set)]),
        ]
        for cname,C in cohort_defs:
            if C.empty: continue
            def cadd(label,g):
                if len(g): cohort_rows.append({"cohort":cname,"name":label,**_v6_shadow_metrics(g)})
            cadd("ALL",C)
            if "v6_market_character" in C.columns:
                cadd("POWER",C[C["v6_market_character"].astype(str)=="POWER_TREND"])
            if "rs" in C.columns:
                rr=pd.to_numeric(C["rs"],errors="coerce")
                cadd("RS2≥90",C[rr>=90])
            if all(c in C.columns for c in ["v6_market_character","rs","volratio"]):
                rr=pd.to_numeric(C["rs"],errors="coerce")
                vv=pd.to_numeric(C["volratio"],errors="coerce")
                mm=(C["v6_market_character"].astype(str)=="POWER_TREND")&(rr>=90)&(vv>=1.8)
                cadd("POWER+Vol≥1.8+RS2≥90",C[mm])

    # Recent-era non-overlapping RS2 bands.
    if "date" in T.columns and "rs" in T.columns:
        R=T[(T["date"]>=pd.Timestamp("2022-01-01"))&(T["date"]<=pd.Timestamp("2026-12-31"))].copy()
        if len(R):
            rr=pd.to_numeric(R["rs"],errors="coerce")
            for nm,lo,hi in [("<80",None,80),("80–90",80,90),("90–95",90,95),("95–98",95,98),("98–100",98,100.0001)]:
                m=pd.Series(True,index=R.index)
                if lo is not None:m &= rr>=lo
                if hi is not None:m &= rr<hi
                g=R[m]
                if len(g): recent_rs2_bands.append({"name":nm,**_v6_shadow_metrics(g)})

    validation_core=[]
    validation_core_eras=[]

    # V6.3.7: drill into the validation cohort CORE candidate only.
    # Candidate = VALIDATION_324 + POWER_TREND + volratio>=1.8 + RS2>=90.
    if "ticker" in T.columns:
        tick=T["ticker"].astype(str)
        V=T[tick.isin(set(V5_VALIDATION))].copy()

        if all(c in V.columns for c in ["v6_market_character","volratio","rs"]):
            vv=pd.to_numeric(V["volratio"],errors="coerce")
            rr=pd.to_numeric(V["rs"],errors="coerce")
            base=(V["v6_market_character"].astype(str)=="POWER_TREND")&(vv>=1.8)&(rr>=90)
            C=V[base].copy()

            def vcadd(group,label,g):
                if len(g):
                    validation_core.append({"group":group,"name":label,**_v6_shadow_metrics(g)})

            vcadd("BASE","ALL CORE",C)

            # Non-overlapping Volume buckets
            if "volratio" in C.columns:
                cv=pd.to_numeric(C["volratio"],errors="coerce")
                for nm,lo,hi in [("1.8–2.0x",1.8,2.0),("2.0–2.5x",2.0,2.5),("2.5x+",2.5,None)]:
                    m=cv>=lo
                    if hi is not None:m &= cv<hi
                    vcadd("VOLUME",nm,C[m])

            # Non-overlapping RS2 buckets
            if "rs" in C.columns:
                cr=pd.to_numeric(C["rs"],errors="coerce")
                for nm,lo,hi in [("90–92",90,92),("92–95",92,95),("95–98",95,98),("98–100",98,100.0001)]:
                    vcadd("RS2",nm,C[(cr>=lo)&(cr<hi)])

            # Supply buckets
            if "supply_score" in C.columns:
                cs=pd.to_numeric(C["supply_score"],errors="coerce")
                for nm,lo,hi in [("<55",None,55),("55–65",55,65),("65–75",65,75),("75+",75,None)]:
                    m=pd.Series(True,index=C.index)
                    if lo is not None:m &= cs>=lo
                    if hi is not None:m &= cs<hi
                    vcadd("SUPPLY",nm,C[m])

            # Entry extension buckets
            if "extension" in C.columns:
                ce=pd.to_numeric(C["extension"],errors="coerce")
                for nm,lo,hi in [("≤1%",None,0.01),("1–2%",0.01,0.02),("2–3%",0.02,0.03),(">3%",0.03,None)]:
                    m=pd.Series(True,index=C.index)
                    if lo is not None:m &= ce>lo
                    if hi is not None:m &= ce<=hi
                    vcadd("ENTRY",nm,C[m])

            # Sector buckets
            if "sector_score" in C.columns:
                cs=pd.to_numeric(C["sector_score"],errors="coerce")
                for nm,lo,hi in [("<60",None,60),("60–65",60,65),("65–70",65,70),("70+",70,None)]:
                    m=pd.Series(True,index=C.index)
                    if lo is not None:m &= cs>=lo
                    if hi is not None:m &= cs<hi
                    vcadd("SECTOR",nm,C[m])

            # Group breadth buckets
            if "group_breadth" in C.columns:
                cg=pd.to_numeric(C["group_breadth"],errors="coerce")
                for nm,lo,hi in [("<40",None,40),("40–50",40,50),("50–60",50,60),("60+",60,None)]:
                    m=pd.Series(True,index=C.index)
                    if lo is not None:m &= cg>=lo
                    if hi is not None:m &= cg<hi
                    vcadd("GROUP",nm,C[m])

            # Era stability for this exact validation-core candidate
            if "date" in C.columns:
                C["date"]=pd.to_datetime(C["date"],errors="coerce")
                for nm,st,en in [("2007–2011","2007-01-01","2011-12-31"),
                                 ("2012–2016","2012-01-01","2016-12-31"),
                                 ("2017–2021","2017-01-01","2021-12-31"),
                                 ("2022–2026","2022-01-01","2026-12-31")]:
                    g=C[(C["date"]>=pd.Timestamp(st))&(C["date"]<=pd.Timestamp(en))]
                    if len(g):
                        validation_core_eras.append({"name":nm,**_v6_shadow_metrics(g)})

    validation_core_recent_trades=[]
    validation_core_feature_compare=[]

    if "ticker" in T.columns:
        tick=T["ticker"].astype(str)
        V=T[tick.isin(set(V5_VALIDATION))].copy()
        if all(c in V.columns for c in ["v6_market_character","volratio","rs"]):
            vv=pd.to_numeric(V["volratio"],errors="coerce")
            rr=pd.to_numeric(V["rs"],errors="coerce")
            C=V[(V["v6_market_character"].astype(str)=="POWER_TREND")&(vv>=1.8)&(rr>=90)].copy()

            if "date" in C.columns:
                C["date"]=pd.to_datetime(C["date"],errors="coerce")
                R=C[(C["date"]>=pd.Timestamp("2022-01-01"))&(C["date"]<=pd.Timestamp("2026-12-31"))].copy()
                for _,r in R.sort_values("date").iterrows():
                    item={}
                    for c in ["ticker","date","return_pct","volratio","rs","rs_raw","supply_score",
                              "sector_score","leader_score","group_breadth","extension",
                              "v6_edge_count","v6_red_count","v6_tier","v6_rank"]:
                        if c in R.columns:
                            v=r.get(c)
                            if c=="date":
                                item[c]=v.strftime("%Y-%m-%d") if pd.notna(v) else ""
                            elif isinstance(v,(int,float)) and pd.isna(v):
                                item[c]=None
                            else:
                                item[c]=v
                    validation_core_recent_trades.append(item)

                early=C[C["date"]<pd.Timestamp("2022-01-01")]
                recentc=C[C["date"]>=pd.Timestamp("2022-01-01")]
                for c in ["volratio","rs","rs_raw","supply_score","sector_score","leader_score",
                          "group_breadth","extension","v6_edge_count","v6_red_count"]:
                    if c not in C.columns: continue
                    def _med(df):
                        s=pd.to_numeric(df[c],errors="coerce").dropna()
                        return float(s.median()) if len(s) else None
                    validation_core_feature_compare.append({
                        "feature":c,"early":_med(early),"recent":_med(recentc)
                    })

    context_veto=[]
    context_veto_recent=[]

    # V6.3.9 Context Veto Diagnostic
    if "ticker" in T.columns:
        tick=T["ticker"].astype(str)
        V=T[tick.isin(set(V5_VALIDATION))].copy()
        if all(c in V.columns for c in ["v6_market_character","volratio","rs"]):
            vv=pd.to_numeric(V["volratio"],errors="coerce")
            rr=pd.to_numeric(V["rs"],errors="coerce")
            C=V[(V["v6_market_character"].astype(str)=="POWER_TREND")&(vv>=1.8)&(rr>=90)].copy()

            def cvadd(group,label,g,target):
                if len(g):
                    target.append({"group":group,"name":label,**_v6_shadow_metrics(g)})

            # Tier
            if "v6_tier" in C.columns:
                for nm in ["A1","A2","B","WATCH","REJECT"]:
                    cvadd("TIER",nm,C[C["v6_tier"].astype(str)==nm],context_veto)

            # Edge count
            if "v6_edge_count" in C.columns:
                ec=pd.to_numeric(C["v6_edge_count"],errors="coerce")
                for nm,lo,hi in [("≤5",None,5.0001),("6",5.999,6.001),("7",6.999,7.001),("8+",8,None)]:
                    m=pd.Series(True,index=C.index)
                    if lo is not None:m &= ec>=lo
                    if hi is not None:m &= ec<hi
                    cvadd("EDGE",nm,C[m],context_veto)

            # Supply + Extension veto candidates
            if "supply_score" in C.columns:
                ss=pd.to_numeric(C["supply_score"],errors="coerce")
                cvadd("SUPPLY","<55",C[ss<55],context_veto)
                cvadd("SUPPLY","55–65",C[(ss>=55)&(ss<65)],context_veto)
                cvadd("SUPPLY","≥65",C[ss>=65],context_veto)
            if "extension" in C.columns:
                ex=pd.to_numeric(C["extension"],errors="coerce")
                cvadd("ENTRY","≤1%",C[ex<=0.01],context_veto)
                cvadd("ENTRY","1–2%",C[(ex>0.01)&(ex<=0.02)],context_veto)
                cvadd("ENTRY",">2%",C[ex>0.02],context_veto)

            # Combined veto-style conditions
            if "v6_tier" in C.columns and "v6_edge_count" in C.columns:
                ec=pd.to_numeric(C["v6_edge_count"],errors="coerce")
                tb=C["v6_tier"].astype(str)=="B"
                cvadd("COMBO","Tier B",C[tb],context_veto)
                cvadd("COMBO","Tier B + Edge=6",C[tb&(ec==6)],context_veto)

                if "supply_score" in C.columns:
                    ss=pd.to_numeric(C["supply_score"],errors="coerce")
                    cvadd("COMBO","B + Edge=6 + Supply<55",C[tb&(ec==6)&(ss<55)],context_veto)
                    cvadd("COMBO","B + Edge=6 + Supply≥55",C[tb&(ec==6)&(ss>=55)],context_veto)
                if "extension" in C.columns:
                    ex=pd.to_numeric(C["extension"],errors="coerce")
                    cvadd("COMBO","B + Edge=6 + Ext>1%",C[tb&(ec==6)&(ex>0.01)],context_veto)
                    cvadd("COMBO","B + Edge=6 + Ext≤1%",C[tb&(ec==6)&(ex<=0.01)],context_veto)
                if "supply_score" in C.columns and "extension" in C.columns:
                    ss=pd.to_numeric(C["supply_score"],errors="coerce")
                    ex=pd.to_numeric(C["extension"],errors="coerce")
                    cvadd("COMBO","B+E6 + weak supply OR ext>1%",
                          C[tb&(ec==6)&((ss<55)|(ex>0.01))],context_veto)
                    cvadd("COMBO","B+E6 + Supply≥55 + Ext≤1%",
                          C[tb&(ec==6)&(ss>=55)&(ex<=0.01)],context_veto)

            # Same diagnostics on recent era only.
            if "date" in C.columns:
                C["date"]=pd.to_datetime(C["date"],errors="coerce")
                R=C[C["date"]>=pd.Timestamp("2022-01-01")].copy()
                if len(R):
                    if "v6_tier" in R.columns:
                        for nm in ["A1","A2","B","WATCH","REJECT"]:
                            cvadd("RECENT_TIER",nm,R[R["v6_tier"].astype(str)==nm],context_veto_recent)
                    if "v6_edge_count" in R.columns:
                        ec=pd.to_numeric(R["v6_edge_count"],errors="coerce")
                        for nm,val in [("6",6),("7",7),("8+",8)]:
                            if nm=="8+":
                                g=R[ec>=8]
                            else:
                                g=R[ec==val]
                            cvadd("RECENT_EDGE",nm,g,context_veto_recent)
                    if "supply_score" in R.columns:
                        ss=pd.to_numeric(R["supply_score"],errors="coerce")
                        cvadd("RECENT_SUPPLY","<55",R[ss<55],context_veto_recent)
                        cvadd("RECENT_SUPPLY","≥55",R[ss>=55],context_veto_recent)
                    if "extension" in R.columns:
                        ex=pd.to_numeric(R["extension"],errors="coerce")
                        cvadd("RECENT_ENTRY","≤1%",R[ex<=0.01],context_veto_recent)
                        cvadd("RECENT_ENTRY",">1%",R[ex>0.01],context_veto_recent)

    veto_validation=[]

    def _vv_add(scope,label,g):
        if len(g):
            veto_validation.append({"scope":scope,"name":label,**_v6_shadow_metrics(g)})

    # V6.4 candidate veto validation: compare BEFORE vs AFTER, no rerun.
    # Candidates:
    # A: Extension >2% veto
    # B: Supply >=65 veto
    # C: Combined A OR B veto
    if len(T):
        scopes=[("ALL_466",T)]
        if "ticker" in T.columns:
            tick=T["ticker"].astype(str)
            scopes.append(("ORIGINAL_122",T[tick.isin(set(V5_ORIGINAL_122))]))
            scopes.append(("VALIDATION_324",T[tick.isin(set(V5_VALIDATION))]))

        if "date" in T.columns:
            Td=T.copy()
            Td["date"]=pd.to_datetime(Td["date"],errors="coerce")
            scopes.append(("2007–2021",Td[Td["date"]<pd.Timestamp("2022-01-01")]))
            scopes.append(("2022–2026",Td[Td["date"]>=pd.Timestamp("2022-01-01")]))

        for scope_name,S in scopes:
            if S.empty: continue
            _vv_add(scope_name,"BEFORE",S)

            ex=pd.to_numeric(S["extension"],errors="coerce") if "extension" in S.columns else pd.Series(index=S.index,dtype=float)
            ss=pd.to_numeric(S["supply_score"],errors="coerce") if "supply_score" in S.columns else pd.Series(index=S.index,dtype=float)

            if "extension" in S.columns:
                _vv_add(scope_name,"AFTER veto Ext>2%",S[~(ex>0.02)])
            if "supply_score" in S.columns:
                _vv_add(scope_name,"AFTER veto Supply≥65",S[~(ss>=65)])
            if "extension" in S.columns and "supply_score" in S.columns:
                keep=~((ex>0.02)|(ss>=65))
                _vv_add(scope_name,"AFTER combined veto",S[keep])

    ranking_diag=[]
    opportunity_diag=[]

    def _rk_add(group,label,g):
        if len(g):
            ranking_diag.append({"group":group,"name":label,**_v6_shadow_metrics(g)})

    def _op_add(group,label,g):
        if len(g):
            opportunity_diag.append({"group":group,"name":label,**_v6_shadow_metrics(g)})

    # V6.5 Ranking Diagnostic — no rerun, existing V6.3 trades only.
    if len(T):
        # Rank buckets overall
        if "v6_rank" in T.columns:
            rk=pd.to_numeric(T["v6_rank"],errors="coerce")
            _rk_add("ALL","Rank 1",T[rk==1])
            _rk_add("ALL","Rank 2",T[rk==2])
            _rk_add("ALL","Rank 3",T[rk==3])
            _rk_add("ALL","Rank 4+",T[rk>=4])

        # Same by market character
        if "v6_market_character" in T.columns and "v6_rank" in T.columns:
            rk=pd.to_numeric(T["v6_rank"],errors="coerce")
            for mc in ["POWER_TREND","BULL_TREND"]:
                M=T[T["v6_market_character"].astype(str)==mc]
                if len(M):
                    mr=pd.to_numeric(M["v6_rank"],errors="coerce")
                    _rk_add(mc,"Rank 1",M[mr==1])
                    _rk_add(mc,"Rank 2",M[mr==2])
                    _rk_add(mc,"Rank 3",M[mr==3])
                    _rk_add(mc,"Rank 4+",M[mr>=4])

        # Validation cohort ranking
        if "ticker" in T.columns and "v6_rank" in T.columns:
            tick=T["ticker"].astype(str)
            V=T[tick.isin(set(V5_VALIDATION))]
            vr=pd.to_numeric(V["v6_rank"],errors="coerce")
            _rk_add("VALIDATION_324","Rank 1",V[vr==1])
            _rk_add("VALIDATION_324","Rank 2",V[vr==2])
            _rk_add("VALIDATION_324","Rank 3",V[vr==3])
            _rk_add("VALIDATION_324","Rank 4+",V[vr>=4])

        # Recent ranking
        if "date" in T.columns and "v6_rank" in T.columns:
            Td=T.copy()
            Td["date"]=pd.to_datetime(Td["date"],errors="coerce")
            R=Td[Td["date"]>=pd.Timestamp("2022-01-01")]
            rr=pd.to_numeric(R["v6_rank"],errors="coerce")
            _rk_add("2022–2026","Rank 1",R[rr==1])
            _rk_add("2022–2026","Rank 2",R[rr==2])
            _rk_add("2022–2026","Rank 3",R[rr==3])
            _rk_add("2022–2026","Rank 4+",R[rr>=4])

        # Opportunity-quality comparison: not hard gates, descriptive stacks.
        cols=set(T.columns)
        if "v6_market_character" in cols:
            P=T[T["v6_market_character"].astype(str)=="POWER_TREND"].copy()
            _op_add("POWER","ALL",P)
            if "v6_edge_count" in cols:
                ec=pd.to_numeric(P["v6_edge_count"],errors="coerce")
                _op_add("POWER","Edge≥7",P[ec>=7])
                _op_add("POWER","Edge≥8",P[ec>=8])
            if "v6_red_count" in cols:
                rc=pd.to_numeric(P["v6_red_count"],errors="coerce")
                _op_add("POWER","Red=0",P[rc==0])
                _op_add("POWER","Red≤1",P[rc<=1])
            if "extension" in cols:
                ex=pd.to_numeric(P["extension"],errors="coerce")
                _op_add("POWER","Extension≤1%",P[ex<=0.01])
            if "volratio" in cols:
                vv=pd.to_numeric(P["volratio"],errors="coerce")
                _op_add("POWER","Volume≥1.8x",P[vv>=1.8])
            if "rs" in cols:
                rr=pd.to_numeric(P["rs"],errors="coerce")
                _op_add("POWER","RS2≥90",P[rr>=90])

            if all(c in cols for c in ["v6_edge_count","v6_red_count","extension"]):
                ec=pd.to_numeric(P["v6_edge_count"],errors="coerce")
                rc=pd.to_numeric(P["v6_red_count"],errors="coerce")
                ex=pd.to_numeric(P["extension"],errors="coerce")
                _op_add("POWER","Edge≥7 + Red=0 + Ext≤1%",P[(ec>=7)&(rc==0)&(ex<=0.01)])

    return {"available":True,"rows":rows,"trades":int(len(T)),"eras":eras,"recent":recent,
            "rsbands":rsbands,"cohort_rows":cohort_rows,"recent_rs2_bands":recent_rs2_bands,
            "validation_core":validation_core,"validation_core_eras":validation_core_eras,
            "validation_core_recent_trades":validation_core_recent_trades,
            "validation_core_feature_compare":validation_core_feature_compare,
            "context_veto":context_veto,"context_veto_recent":context_veto_recent,
            "veto_validation":veto_validation,
            "ranking_diag":ranking_diag,"opportunity_diag":opportunity_diag}

def run_v63():
    import importlib.util
    try:
        if V63_STOP_FILE.exists():V63_STOP_FILE.unlink()
        write_v63_status(state="running",message="阶段1/2：正在建立 RS2 Feature Store",done=0,total=321,ticker="",error="",started_at=datetime.now(timezone.utc).isoformat())
        ep=Path(__file__).with_name("jlaw_v6_3_RS2_FEATURE_STORE.py")
        if not ep.exists():raise RuntimeError("GitHub 根目录缺少 jlaw_v6_3_RS2_FEATURE_STORE.py")
        spec=importlib.util.spec_from_file_location("jlaw_v63_rs2",ep)
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        outdir=V63_DIR/"full"
        outdir.mkdir(parents=True,exist_ok=True)
        def cb(done,total,ticker):
            write_v63_status(state="running",message=f"阶段2/2：全量股票扫描 {done}/{total}",done=int(done),total=int(total),ticker=str(ticker),error="")
        mod.run(MARKET_DIR,outdir,progress_cb=cb,quick=False,stop_file=V63_STOP_FILE,parallel_workers=2,resume=True)
        st_done=read_v63_status()
        actual_total=int(st_done.get("total") or st_done.get("done") or 0)
        actual_done=int(st_done.get("done") or actual_total)
        write_v63_status(state="done",message=f"V6.3 RS2 全量完成：{actual_done}/{actual_total}",done=actual_done,total=actual_total,ticker="",error="")
    except Exception as e:
        write_v63_status(state="error",message="V6.3 失败",error=str(e))

def start_v63():
    st=read_v63_status()
    if st.get("state")=="running":return
    threading.Thread(target=run_v63,daemon=True).start()

def stop_v63():
    V63_DIR.mkdir(parents=True,exist_ok=True);V63_STOP_FILE.write_text("stop",encoding="utf-8")
    write_v63_status(state="stopping",message="正在停止 V6.3；已完成股票会保留 checkpoint")

def read_v61_fast_status():
    s={"status":"idle","mode":"","message":"尚未运行 V6.1 Fast","stocks":0,"processed":0,"current":"",
       "started_at":None,"finished_at":None,"error":None}
    if V61_FAST_STATUS_FILE.exists():
        try:s.update(json.loads(V61_FAST_STATUS_FILE.read_text(encoding="utf-8")))
        except Exception:pass
    return s

def write_v61_fast_status(**kw):
    V61_FAST_DIR.mkdir(parents=True,exist_ok=True)
    s=read_v61_fast_status();s.update(kw)
    V61_FAST_STATUS_FILE.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")

def v61_fast_summary(mode="full"):
    base=V61_FAST_DIR/("quick50" if mode=="quick50" else "full")
    p=base/"jlaw_v6_trades.csv"
    if not p.exists() or p.stat().st_size==0:return []
    try:d=pd.read_csv(p)
    except Exception:return []
    if d.empty:return []
    rows=[dict(segment="ALL",**_v6_shadow_metrics(d))]
    if "v6_tier" in d.columns:
        for label,g in [("A1",d[d.v6_tier=="A1"]),("A1+A2",d[d.v6_tier.isin(["A1","A2"])])]:
            if len(g):rows.append(dict(segment=label,**_v6_shadow_metrics(g)))
    if "v6_top3" in d.columns:
        g=d[d.v6_top3.astype(str).str.lower().isin(["true","1"])]
        if len(g):rows.append(dict(segment="TOP3",**_v6_shadow_metrics(g)))
    return rows

def run_v61_fast(mode):
    global v61_fast_thread
    try:
        import importlib.util
        ep=Path(__file__).with_name("jlaw_v6_1_DECISION_FAST.py")
        if not ep.exists():raise RuntimeError("GitHub 根目录缺少 jlaw_v6_1_DECISION_FAST.py")
        spec=importlib.util.spec_from_file_location("jlaw_v61_fast_engine",str(ep))
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)

        V61_FAST_DIR.mkdir(parents=True,exist_ok=True)
        v61_fast_stop_event.clear()
        try:
            if V61_FAST_STOP_FILE.exists():V61_FAST_STOP_FILE.unlink()
        except Exception:pass

        available=sorted(t for t in V5_VALIDATION if (MARKET_DIR/f"{t}.csv").exists())
        quick=(mode=="quick50")
        if quick:
            # Evenly sample across the 321-stock validation pool instead of taking
            # the first 50 alphabetically, which can be sector/ticker biased.
            if len(available)<=50:
                target=available
            else:
                step=(len(available)-1)/49.0
                idxs=sorted(set(int(round(i*step)) for i in range(50)))
                target=[available[i] for i in idxs]
        else:
            target=available
        outdir=V61_FAST_DIR/("quick50" if quick else "full")
        if quick:
            # Revised Quick50 semantics; remove old 420-bar run artifacts once.
            marker=outdir/"QUICK50_FULL_HISTORY_V2"
            if not marker.exists():
                try:
                    import shutil
                    if outdir.exists():
                        shutil.rmtree(outdir)
                    outdir.mkdir(parents=True,exist_ok=True)
                    marker.write_text("full-history representative quick50",encoding="utf-8")
                except Exception:
                    pass

        write_v61_fast_status(status="running",mode=mode,
            message=("Quick 50代表样本·全历史" if quick else "Full 321")+" · 2线程并行 · checkpoint续跑",
            stocks=len(target),processed=0,current="",error=None,
            started_at=datetime.now(timezone.utc).isoformat(),finished_at=None)

        def prog(done,total,t):
            if v61_fast_stop_event.is_set():raise RuntimeError("__V61_STOPPED__")
            write_v61_fast_status(status="running",mode=mode,
                message=("Quick 50代表样本·全历史" if quick else "Full 321")+" · 2线程并行 · checkpoint续跑",
                stocks=total,processed=done,current=t)

        # Quick50 means "50 representative stocks over full history".
        # This is still much faster than 321 stocks, but avoids the old 420-bar
        # truncation that could legitimately produce zero V6 trades.
        mod.run(str(MARKET_DIR),str(outdir),progress_cb=prog,
                quick=False,quick_n=50,quick_lookback=420,
                stop_file=str(V61_FAST_STOP_FILE),ticker_filter=target,
                parallel_workers=2,resume=True)

        if V61_FAST_STOP_FILE.exists() or v61_fast_stop_event.is_set():
            raise RuntimeError("__V61_STOPPED__")

        write_v61_fast_status(status="done",mode=mode,
            message=("Quick 50" if quick else "Full 321")+" 完成",
            stocks=len(target),processed=len(target),current="",
            finished_at=datetime.now(timezone.utc).isoformat(),error=None)
    except Exception as ex:
        if str(ex)=="__V61_STOPPED__":
            s=read_v61_fast_status()
            write_v61_fast_status(status="stopped",mode=mode,message="已停止；下次会从 checkpoint 继续",
                                  stocks=s.get("stocks",0),processed=s.get("processed",0),current="",
                                  finished_at=datetime.now(timezone.utc).isoformat(),error=None)
        else:
            traceback.print_exc()
            write_v61_fast_status(status="error",mode=mode,message="V6.1 Fast 失败",
                                  error=f"{type(ex).__name__}: {ex}",
                                  finished_at=datetime.now(timezone.utc).isoformat())
    finally:
        v61_fast_thread=None;v61_fast_stop_event.clear()

def start_v61_fast(mode):
    global v61_fast_thread
    if v61_fast_thread is not None and v61_fast_thread.is_alive():return False
    v61_fast_thread=threading.Thread(target=run_v61_fast,args=(mode,),daemon=True)
    v61_fast_thread.start();return True

def stop_v61_fast():
    V61_FAST_DIR.mkdir(parents=True,exist_ok=True)
    try:V61_FAST_STOP_FILE.write_text("stop",encoding="utf-8")
    except Exception:pass
    v61_fast_stop_event.set();return True


# ================= V6.2 INSTANT RESEARCH CACHE =================
V62_CACHE_DIR = DATA_DIR / "jlaw_v62_research_cache"
V62_SIGNAL_CACHE = V62_CACHE_DIR / "signals.pkl"
V62_TRADE_CACHE = V62_CACHE_DIR / "trades.pkl"
V62_META = V62_CACHE_DIR / "meta.json"

def v62_cache_status():
    meta={"ready":False,"signals":0,"trades":0,"source":"","built_at":None,
          "message":"尚未建立研究缓存"}
    if V62_META.exists():
        try:meta.update(json.loads(V62_META.read_text(encoding="utf-8")))
        except Exception:pass
    return meta

def build_v62_research_cache():
    """Build once from the most complete existing V6 output. No historical rescan."""
    V62_CACHE_DIR.mkdir(parents=True,exist_ok=True)
    candidates=[
        (V61_FAST_DIR/"full"/"jlaw_v6_signals.csv", V61_FAST_DIR/"full"/"jlaw_v6_trades.csv", "V6.1 Full"),
        (V6_SHADOW_DIR/"jlaw_v6_signals.csv", V6_SHADOW_DIR/"jlaw_v6_trades.csv", "V6.0 Shadow"),
    ]
    sigp=trp=source=None
    for a,b,name in candidates:
        if a.exists() and a.stat().st_size>0 and b.exists() and b.stat().st_size>0:
            sigp,trp,source=a,b,name
            break
    if sigp is None:
        return {"ok":False,"error":"找不到完整 V6 signals/trades；先让当前 Full 321 跑完。"}

    S=pd.read_csv(sigp); T=pd.read_csv(trp)
    if S.empty or T.empty:
        return {"ok":False,"error":"V6 signals/trades 为空"}

    for df in (S,T):
        if "date" in df.columns:df["date"]=pd.to_datetime(df["date"],errors="coerce")
        for c in ["return_pct","v6_edge_count","v6_red_count","v6_rank","rs","volratio",
                  "supply_score","sector_score","leader_score","group_breadth","extension",
                  "pivot_tight","c1","c2","c3","dry","v46_score"]:
            if c in df.columns:df[c]=pd.to_numeric(df[c],errors="coerce")

    S.to_pickle(V62_SIGNAL_CACHE)
    T.to_pickle(V62_TRADE_CACHE)
    meta={
        "ready":True,"signals":int(len(S)),"trades":int(len(T)),"source":source,
        "built_at":datetime.now(timezone.utc).isoformat(),
        "message":"研究缓存已建立：以后改 A1/A2、POWER、Edge、Ranking 不再重扫321只"
    }
    V62_META.write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
    return {"ok":True,**meta}

def _v62_load():
    if V62_SIGNAL_CACHE.exists() and V62_TRADE_CACHE.exists():
        return pd.read_pickle(V62_SIGNAL_CACHE),pd.read_pickle(V62_TRADE_CACHE)
    raise RuntimeError("V6.2 Research Cache 尚未建立")

def v62_instant_lab():
    st=v62_cache_status()
    if not st.get("ready"):return {"available":False,"status":st}
    try:S,T=_v62_load()
    except Exception as e:return {"available":False,"status":st,"error":str(e)}

    d=T.copy()
    if d.empty:return {"available":False,"status":st}
    char=d["v6_market_character"].astype(str) if "v6_market_character" in d.columns else pd.Series("",index=d.index)
    tier=d["v6_tier"].astype(str) if "v6_tier" in d.columns else pd.Series("",index=d.index)

    rows=[]
    masks=[
        ("ALL",pd.Series(True,index=d.index)),
        ("POWER only",char=="POWER_TREND"),
        ("POWER + A1",(char=="POWER_TREND")&(tier=="A1")),
        ("POWER + A1/A2",(char=="POWER_TREND")&(tier.isin(["A1","A2"]))),
    ]
    if "v6_edge_count" in d.columns:
        for n in [6,7,8,9]:
            masks.append((f"POWER + edges≥{n}",(char=="POWER_TREND")&(d["v6_edge_count"]>=n)))
    if "extension" in d.columns:
        masks.append(("POWER + entry≤2%",(char=="POWER_TREND")&(d["extension"]<=.02)))
    if "volratio" in d.columns:
        masks.append(("POWER + volume≥1.8x",(char=="POWER_TREND")&(d["volratio"]>=1.8)))
    if "supply_score" in d.columns:
        masks.append(("POWER + supply≥75",(char=="POWER_TREND")&(d["supply_score"]>=75)))
    if "rs" in d.columns:
        masks.append(("POWER + RS≥90",(char=="POWER_TREND")&(d["rs"]>=90)))

    # V6.2.2 cross-combination diagnostics.
    # These are descriptive tests on the same development set, not new OOS validation.
    power=(char=="POWER_TREND")
    if "volratio" in d.columns and "extension" in d.columns:
        masks.append(("POWER + vol≥1.8x + entry≤2%", power&(d["volratio"]>=1.8)&(d["extension"]<=.02)))
    if "volratio" in d.columns and "rs" in d.columns:
        masks.append(("POWER + vol≥1.8x + RS≥90", power&(d["volratio"]>=1.8)&(d["rs"]>=90)))
    if "volratio" in d.columns and "supply_score" in d.columns:
        masks.append(("POWER + vol≥1.8x + supply≥75", power&(d["volratio"]>=1.8)&(d["supply_score"]>=75)))
    if "volratio" in d.columns and "extension" in d.columns and "rs" in d.columns:
        masks.append(("POWER + vol≥1.8x + entry≤2% + RS≥90",
                      power&(d["volratio"]>=1.8)&(d["extension"]<=.02)&(d["rs"]>=90)))
    if "volratio" in d.columns and "v6_edge_count" in d.columns:
        masks.append(("POWER + vol≥1.8x + edges≥7", power&(d["volratio"]>=1.8)&(d["v6_edge_count"]>=7)))
        masks.append(("POWER + vol≥1.8x + edges≥8", power&(d["volratio"]>=1.8)&(d["v6_edge_count"]>=8)))

    # V6.2.3 Stability Grid: test whether the edge is a broad plateau rather than one lucky threshold.
    stability=[]
    if "volratio" in d.columns and "rs" in d.columns:
        vol_thresholds=[1.5,1.6,1.7,1.8,2.0,2.2,2.5]
        rs_thresholds=[88,90,92,95]
        for vv in vol_thresholds:
            for rr in rs_thresholds:
                g=d[power&(d["volratio"]>=vv)&(d["rs"]>=rr)]
                if len(g):
                    m=_v6_shadow_metrics(g)
                    stability.append({
                        "vol":vv,"rs":rr,
                        "trades":m["trades"],"win_rate":m["win_rate"],
                        "avg_return":m["avg_return"],"pf":m["pf"],
                        "max_losing_streak":m["max_losing_streak"]
                    })

    # V6.2.4 robustness: non-overlapping volume buckets under RS>=90.
    volume_buckets=[]
    if "volratio" in d.columns and "rs" in d.columns:
        base=power&(d["rs"]>=90)
        bucket_defs=[
            ("1.5–1.7x",1.5,1.7),
            ("1.7–1.8x",1.7,1.8),
            ("1.8–2.0x",1.8,2.0),
            ("2.0–2.2x",2.0,2.2),
            ("2.2–2.5x",2.2,2.5),
            ("2.5x+",2.5,None),
        ]
        for name,lo,hi in bucket_defs:
            mask=base&(d["volratio"]>=lo)
            if hi is not None:
                mask=mask&(d["volratio"]<hi)
            g=d[mask]
            if len(g):
                m=_v6_shadow_metrics(g)
                volume_buckets.append({
                    "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                    "avg_return":m["avg_return"],"pf":m["pf"],
                    "max_losing_streak":m["max_losing_streak"]
                })

    # Split the selected CORE rule by broad eras to check persistence.
    era_rows=[]
    if "volratio" in d.columns and "rs" in d.columns and "date" in d.columns:
        dd=d.copy()
        dd["date"]=pd.to_datetime(dd["date"],errors="coerce")
        core=power&(dd["volratio"]>=1.8)&(dd["rs"]>=90)
        era_defs=[
            ("2007–2011","2007-01-01","2011-12-31"),
            ("2012–2016","2012-01-01","2016-12-31"),
            ("2017–2021","2017-01-01","2021-12-31"),
            ("2022–2026","2022-01-01","2026-12-31"),
        ]
        for name,start,end in era_defs:
            g=dd[core&(dd["date"]>=pd.Timestamp(start))&(dd["date"]<=pd.Timestamp(end))]
            if len(g):
                m=_v6_shadow_metrics(g)
                era_rows.append({
                    "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                    "avg_return":m["avg_return"],"pf":m["pf"],
                    "max_losing_streak":m["max_losing_streak"]
                })

    # Year-by-year split for the same CORE rule.
    yearly_rows=[]
    if "volratio" in d.columns and "rs" in d.columns and "date" in d.columns:
        dd=d.copy()
        dd["date"]=pd.to_datetime(dd["date"],errors="coerce")
        core=power&(dd["volratio"]>=1.8)&(dd["rs"]>=90)
        dg=dd[core].copy()
        if not dg.empty:
            dg["year"]=dg["date"].dt.year
            for yy,g in dg.groupby("year"):
                if len(g):
                    m=_v6_shadow_metrics(g)
                    yearly_rows.append({
                        "year":int(yy),"trades":m["trades"],"win_rate":m["win_rate"],
                        "avg_return":m["avg_return"],"pf":m["pf"],
                        "max_losing_streak":m["max_losing_streak"]
                    })

    # V6.2.5 Fault Isolation: inspect the actual CORE trades, especially 2022-2026.
    fault_recent=[]
    fault_compare=[]
    fault_features=[]

    if "volratio" in d.columns and "rs" in d.columns and "date" in d.columns:
        dd=d.copy()
        dd["date"]=pd.to_datetime(dd["date"],errors="coerce")
        core_mask=(dd["v6_market_character"].astype(str)=="POWER_TREND")&(dd["volratio"]>=1.8)&(dd["rs"]>=90)
        core_df=dd[core_mask].copy().sort_values("date")

        # Exact recent CORE trades.
        recent=core_df[(core_df["date"]>=pd.Timestamp("2022-01-01"))&(core_df["date"]<=pd.Timestamp("2026-12-31"))].copy()
        show_cols=[
            "ticker","date","return_pct","volratio","rs","supply_score","sector_score",
            "leader_score","group_breadth","extension","v6_edge_count","v6_red_count",
            "v6_tier","v6_rank"
        ]
        for _,r in recent.iterrows():
            item={}
            for c in show_cols:
                if c in recent.columns:
                    v=r.get(c)
                    if c=="date":
                        item[c]=v.strftime("%Y-%m-%d") if pd.notna(v) else ""
                    elif isinstance(v,(int,float)) and pd.isna(v):
                        item[c]=None
                    else:
                        item[c]=v
            fault_recent.append(item)

        # Broad period comparison.
        period_defs=[
            ("2007–2016","2007-01-01","2016-12-31"),
            ("2017–2021","2017-01-01","2021-12-31"),
            ("2022–2026","2022-01-01","2026-12-31"),
        ]
        for name,start,end in period_defs:
            g=core_df[(core_df["date"]>=pd.Timestamp(start))&(core_df["date"]<=pd.Timestamp(end))]
            if len(g):
                m=_v6_shadow_metrics(g)
                fault_compare.append({
                    "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                    "avg_return":m["avg_return"],"pf":m["pf"],
                    "max_losing_streak":m["max_losing_streak"]
                })

        # Feature comparison: early profitable sample vs recent sample, plus winners vs losers.
        numeric_features=[
            "volratio","rs","supply_score","sector_score","leader_score",
            "group_breadth","extension","v6_edge_count","v6_red_count"
        ]
        early=core_df[core_df["date"]<pd.Timestamp("2022-01-01")]
        recent=core_df[core_df["date"]>=pd.Timestamp("2022-01-01")]
        winners=core_df[core_df["return_pct"]>0] if "return_pct" in core_df.columns else core_df.iloc[0:0]
        losers=core_df[core_df["return_pct"]<=0] if "return_pct" in core_df.columns else core_df.iloc[0:0]
        for c in numeric_features:
            if c not in core_df.columns: continue
            def med(df):
                s=pd.to_numeric(df[c],errors="coerce").dropna()
                return float(s.median()) if len(s) else None
            fault_features.append({
                "feature":c,
                "early":med(early),
                "recent":med(recent),
                "winners":med(winners),
                "losers":med(losers),
            })

    # V6.2.6 Recent Regime Diagnostics:
    # determine whether recent weakness comes from the POWER_TREND classifier,
    # the breakout-volume filter, or simply tiny sample size.
    recent_regime=[]
    recent_power_buckets=[]
    recent_rs_buckets=[]

    if "date" in d.columns:
        rd=d.copy()
        rd["date"]=pd.to_datetime(rd["date"],errors="coerce")
        rd=rd[(rd["date"]>=pd.Timestamp("2022-01-01"))&(rd["date"]<=pd.Timestamp("2026-12-31"))].copy()

        if "v6_market_character" in rd.columns:
            for name,g in rd.groupby(rd["v6_market_character"].astype(str)):
                if len(g):
                    m=_v6_shadow_metrics(g)
                    recent_regime.append({
                        "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                        "avg_return":m["avg_return"],"pf":m["pf"],
                        "max_losing_streak":m["max_losing_streak"]
                    })
            recent_regime=sorted(recent_regime,key=lambda x:x["trades"],reverse=True)

        if "v6_market_character" in rd.columns and "volratio" in rd.columns:
            rp=rd[rd["v6_market_character"].astype(str)=="POWER_TREND"].copy()
            defs=[
                ("<1.5x",None,1.5),
                ("1.5–1.8x",1.5,1.8),
                ("1.8–2.0x",1.8,2.0),
                ("2.0–2.5x",2.0,2.5),
                ("2.5x+",2.5,None),
            ]
            for name,lo,hi in defs:
                mask=pd.Series(True,index=rp.index)
                if lo is not None: mask &= rp["volratio"]>=lo
                if hi is not None: mask &= rp["volratio"]<hi
                g=rp[mask]
                if len(g):
                    m=_v6_shadow_metrics(g)
                    recent_power_buckets.append({
                        "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                        "avg_return":m["avg_return"],"pf":m["pf"],
                        "max_losing_streak":m["max_losing_streak"]
                    })

        if "v6_market_character" in rd.columns and "rs" in rd.columns:
            rp=rd[rd["v6_market_character"].astype(str)=="POWER_TREND"].copy()
            defs=[("<90",None,90),("90–95",90,95),("95–99",95,99.999),("100",99.999,None)]
            for name,lo,hi in defs:
                mask=pd.Series(True,index=rp.index)
                if lo is not None: mask &= rp["rs"]>=lo
                if hi is not None: mask &= rp["rs"]<hi
                g=rp[mask]
                if len(g):
                    m=_v6_shadow_metrics(g)
                    recent_rs_buckets.append({
                        "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                        "avg_return":m["avg_return"],"pf":m["pf"],
                        "max_losing_streak":m["max_losing_streak"]
                    })

    # V6.2.9 RS Saturation diagnostic
    rs100_profile=[]
    rs_band_profiles=[]
    recent_power_years=[]

    if "date" in d.columns and "v6_market_character" in d.columns:
        rd=d.copy()
        rd["date"]=pd.to_datetime(rd["date"],errors="coerce")
        rp=rd[(rd["date"]>=pd.Timestamp("2022-01-01"))&
              (rd["date"]<=pd.Timestamp("2026-12-31"))&
              (rd["v6_market_character"].astype(str)=="POWER_TREND")].copy()

        # POWER by year
        if not rp.empty:
            rp["year"]=rp["date"].dt.year
            for yy,g in rp.groupby("year"):
                if len(g):
                    m=_v6_shadow_metrics(g)
                    recent_power_years.append({
                        "year":int(yy),"trades":m["trades"],"win_rate":m["win_rate"],
                        "avg_return":m["avg_return"],"pf":m["pf"],
                        "max_losing_streak":m["max_losing_streak"]
                    })

        # Profiles for RS bands
        if "rs" in rp.columns:
            bands=[
                ("RS 90–95", (rp["rs"]>=90)&(rp["rs"]<95)),
                ("RS 95–99", (rp["rs"]>=95)&(rp["rs"]<99.999)),
                ("RS 100", rp["rs"]>=99.999),
            ]
            feature_cols=["volratio","supply_score","sector_score","leader_score",
                          "group_breadth","extension","v6_edge_count","v6_red_count"]
            for name,mask in bands:
                g=rp[mask].copy()
                if not len(g): continue
                m=_v6_shadow_metrics(g)
                row={
                    "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                    "avg_return":m["avg_return"],"pf":m["pf"],
                    "max_losing_streak":m["max_losing_streak"]
                }
                for c in feature_cols:
                    if c in g.columns:
                        s=pd.to_numeric(g[c],errors="coerce").dropna()
                        row[c]=float(s.median()) if len(s) else None
                rs_band_profiles.append(row)

            # Split RS100 by Volume to see if high RS needs stronger confirmation.
            rs100=rp[rp["rs"]>=99.999].copy()
            if len(rs100) and "volratio" in rs100.columns:
                defs=[
                    ("<1.5x",None,1.5),
                    ("1.5–1.8x",1.5,1.8),
                    ("1.8–2.2x",1.8,2.2),
                    ("2.2–2.5x",2.2,2.5),
                    ("2.5x+",2.5,None),
                ]
                for name,lo,hi in defs:
                    mask=pd.Series(True,index=rs100.index)
                    if lo is not None: mask &= rs100["volratio"]>=lo
                    if hi is not None: mask &= rs100["volratio"]<hi
                    g=rs100[mask]
                    if len(g):
                        m=_v6_shadow_metrics(g)
                        rs100_profile.append({
                            "name":name,"trades":m["trades"],"win_rate":m["win_rate"],
                            "avg_return":m["avg_return"],"pf":m["pf"],
                            "max_losing_streak":m["max_losing_streak"]
                        })

    seen=set()
    for name,mask in masks:
        g=d[mask.fillna(False)]
        if len(g)==0:continue
        key=(name,len(g))
        if key in seen:continue
        seen.add(key)
        rows.append(dict(name=name,**_v6_shadow_metrics(g)))

    ranks=[]
    if "v6_rank" in d.columns:
        for name,mask in [
            ("Rank 1",d.v6_rank==1),("Rank 2",d.v6_rank==2),
            ("Rank 3",d.v6_rank==3),("Rank 4+",d.v6_rank>=4)
        ]:
            g=d[mask]
            if len(g):ranks.append(dict(name=name,**_v6_shadow_metrics(g)))
    return {"available":True,"status":st,"rows":rows,"ranks":ranks,"stability":stability,"volume_buckets":volume_buckets,"era_rows":era_rows,"yearly_rows":yearly_rows,"fault_recent":fault_recent,"fault_compare":fault_compare,"fault_features":fault_features,"recent_regime":recent_regime,"recent_power_buckets":recent_power_buckets,"recent_rs_buckets":recent_rs_buckets,"recent_power_years":recent_power_years,"rs_band_profiles":rs_band_profiles,"rs100_profile":rs100_profile}

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










    lines += ["=== V4.6.2 DEPLOY PROBE ==="]
    try:
        pr=v462_deploy_probe()
        lines.append(f"cwd: {_ascii_cell(pr.get('cwd',''))}")
        lines.append(f"app_file: {_ascii_cell(pr.get('app_file',''))}")
        lines.append(f"imported: {_ascii_cell(pr.get('imported',{}))}")
        for r in pr.get("candidates",[]):
            lines.append(_ascii_cell(r))
    except Exception as e:
        lines.append(f"probe error: {_ascii_cell(e)}")
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



    lines += ["=== V4.8 DUAL TIER SUMMARY ==="]
    try:
        dt=pd.DataFrame(v48_dual_tier_summary())
        if not dt.empty:
            cols=["tier","trades","win_rate","avg_return","pf","max_losing_streak",
                  "old_trades","old_avg","old_pf","recent_trades","recent_avg","recent_pf"]
            widths=[15,7,9,10,7,8,8,9,7,8,9,7]
            pctcols={"win_rate","avg_return","old_avg","recent_avg"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(dt,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.8 SCORE BINS ==="]
    try:
        sb=pd.DataFrame(v48_score_bins())
        if not sb.empty:
            cols=["score_band","trades","win_rate","avg_return","pf","max_losing_streak"]
            widths=[12,7,9,10,7,8]
            pctcols={"win_rate","avg_return"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(sb,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.7 TRUE ENGINE THRESHOLD SWEEP ==="]
    try:
        sw=pd.DataFrame(v47_true_engine_threshold_sweep())
        if not sw.empty:
            cols=["threshold","trades","win_rate","avg_return","pf","max_losing_streak",
                  "old_trades","old_win","old_avg","old_pf","recent_trades","recent_win","recent_avg","recent_pf"]
            widths=[8,7,9,10,7,8,8,9,9,7,8,9,9,7]
            pctcols={"win_rate","avg_return","old_win","old_avg","recent_win","recent_avg"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(sw,cols,widths,transforms=tr)
    except Exception as e:
        lines.append(f"read error: {_ascii_cell(e)}")
    lines.append("")

    lines += ["=== V4.7 TRUE ENGINE PERIOD CHECK ==="]
    try:
        pp=pd.DataFrame(v47_true_engine_periods())
        if not pp.empty:
            cols=["threshold","period","trades","win_rate","avg_return","pf","max_losing_streak"]
            widths=[8,12,7,9,10,7,8]
            pctcols={"win_rate","avg_return"}
            tr={c:(lambda v,_c=c: _fmt_num(v,pct=_c in pctcols)) for c in cols}
            lines += _table_lines(pp,cols,widths,transforms=tr)
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



YOUTUBE_HTML = r"""<!doctype html><html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>YouTube Ingest</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f4f7fb;color:#142033;margin:0}
.wrap{max-width:980px;margin:auto;padding:20px}.card{background:#fff;border-radius:18px;padding:18px;margin:14px 0;box-shadow:0 5px 24px #14203312}
input{box-sizing:border-box;width:100%;padding:14px;border:1px solid #ccd5e2;border-radius:12px;font-size:16px}
button,.btn{display:inline-block;padding:12px 16px;border:0;border-radius:10px;background:#1d64f0;color:#fff;font-weight:700;text-decoration:none;font-size:15px}
pre{white-space:pre-wrap;word-break:break-word;background:#f7f9fc;border-radius:12px;padding:14px;max-height:520px;overflow:auto}
.muted{color:#68758a;font-size:14px}.ok{color:#138a45}.err{color:#c73737}
table{width:100%;border-collapse:collapse}td,th{padding:8px;border-bottom:1px solid #edf0f5;text-align:left;font-size:14px}

#publisher-fab{
  position:fixed; right:16px; bottom:88px; z-index:9999;
  text-decoration:none;
}
#publisher-fab button{
  border:0;border-radius:999px;padding:12px 16px;
  font-weight:900;font-size:15px;
  box-shadow:0 6px 24px rgba(0,0,0,.18);
}
@media print{#publisher-fab{display:none!important}}

</style></head><body><div class="wrap">
<div><a href="/" class="btn">← VCP Radar</a></div>
<div class="card">
<h2 style="margin-top:0">YouTube Ingest</h2>
<div class="muted">粘贴 YouTube 链接 → 自动抓完整字幕 → 保存到云端。以后你只要把 YouTube 链接发给 ChatGPT，我就可以通过这个云端入口读取字幕并分析。</div>
<form method="post" action="/youtube/ingest" style="margin-top:14px">
<input name="url" placeholder="https://youtu.be/..." value="{{url or ''}}" required>
<div style="margin-top:10px"><button type="submit">抓取完整字幕</button></div>
</form>
{% if error %}<p class="err"><b>{{error}}</b></p>{% endif %}
{% if result %}
<p class="ok"><b>已保存：</b>{{result.video_id}} · {{result.language}} · {{result.char_count}} 字符 · {{result.snippet_count}} 段</p>
<p>
<a class="btn" href="/youtube/text/{{result.video_id}}">查看纯文字</a>
<a class="btn" href="/youtube/download/{{result.video_id}}">下载 TXT</a>
<a class="btn" href="/api/youtube/transcript?video_id={{result.video_id}}">JSON API</a>
</p>
<pre>{{result.transcript[:16000]}}{{"…" if result.transcript|length > 16000 else ""}}</pre>
{% endif %}
</div>
<details class="card" style="margin-top:18px">
<summary style="font-size:24px;font-weight:800;cursor:pointer;list-style:none">
YouTube 字幕库 <span class="muted" style="font-size:15px;font-weight:600">（点开查看）</span>
</summary>
<div style="margin-top:16px">


<p><a class="btn" href="/api/youtube/index">公开只读字幕索引</a></p>
<table><tr><th>Video ID</th><th>语言</th><th>字幕</th><th>字符</th><th>时间</th></tr>
{% for r in history %}
<tr><td><a href="/youtube/text/{{r.video_id}}">{{r.video_id}}</a></td><td>{{r.language}}</td>
<td>{{"自动" if r.is_generated else "人工"}}</td><td>{{r.char_count}}</td><td>{{r.fetched_at_utc}}</td></tr>
{% endfor %}
</table>
</div>
</div>
</details>


<script>
(function(){
  const startedAt = {{ (v63_status.started_at or "")|tojson }};
  function elapsedText(iso){
    if(!iso) return "";
    const ms = Date.now() - new Date(iso).getTime();
    if(!isFinite(ms) || ms < 0) return "";
    const m = Math.floor(ms/60000), s = Math.floor((ms%60000)/1000);
    return " · 已运行 " + (m>0 ? m+"分" : "") + s + "秒";
  }
  async function pollV63(){
    try{
      const r = await fetch("/api/v63/status", {cache:"no-store"});
      if(!r.ok) return;
      const s = await r.json();
      const total = Number(s.total || 321);
      const done = Number(s.done || 0);
      const pct = total ? Math.max(0, Math.min(100, done/total*100)) : 0;

      const state=document.getElementById("v63-state");
      const msg=document.getElementById("v63-message");
      const count=document.getElementById("v63-count");
      const bar=document.getElementById("v63-progress-bar");
      const percent=document.getElementById("v63-percent");
      const ticker=document.getElementById("v63-ticker");
      const phase=document.getElementById("v63-phase");
      const elapsed=document.getElementById("v63-elapsed");
      const err=document.getElementById("v63-error");

      if(state) state.textContent=s.state||"";
      if(msg) msg.textContent=s.message||"";
      if(count) count.textContent=done+" / "+total;
      if(bar) bar.style.width=pct.toFixed(1)+"%";
      if(percent) percent.textContent=pct.toFixed(1)+"%";
      if(ticker) ticker.textContent=s.ticker ? " · 当前 "+s.ticker : "";
      if(phase) phase.textContent=(s.state==="running" && done===0) ? "阶段1/2：RS2 Feature Store" : "阶段2/2：全量股票扫描";
      if(elapsed) elapsed.textContent=elapsedText(s.started_at || startedAt);
      if(err){
        if(s.error){ err.style.display="block"; err.textContent="⚠️ "+s.error; }
        else { err.style.display="none"; }
      }
    }catch(e){}
  }
  pollV63();
  setInterval(pollV63, 3000);
})();
</script>


<a id="publisher-fab" href="/publisher/control">
  <button type="button">☁️ 发布</button>
</a>

</body></html>"""

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

/* ===== V6.2.1 User Friendly UI ===== */
.quick-nav{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0 18px}
.quick-nav a{display:inline-block;text-decoration:none;padding:9px 12px;border-radius:999px;background:#eef2f7;color:#17202a;font-weight:800;font-size:14px}
.quick-nav a.primary{background:#111827;color:#fff}
.version-label{display:inline-block;margin-left:8px;padding:4px 8px;border-radius:999px;background:#eef4ff;color:#244a9b;font-size:12px;font-weight:800;vertical-align:middle}
.current-card{border:2px solid #d7e5ff;box-shadow:0 8px 28px #1d64f012}
.archive-title{margin:22px 0 8px;font-size:18px;color:#667085;font-weight:800}
.mobile-metrics{display:none}
.scroll-hint{display:none;color:#667085;font-size:13px;margin:7px 0}
details.card>summary,details.section-fold>summary{-webkit-tap-highlight-color:transparent}
@media(max-width:700px){
  .wrap{padding:10px}
  .card{border-radius:14px;padding:14px;margin:10px 0}
  h1{font-size:25px}
  .kpi{font-size:22px}
  button{min-height:44px;padding:11px 13px}
  .quick-nav{position:sticky;top:0;z-index:20;background:#f3f6fbee;padding:8px 0;backdrop-filter:blur(8px)}
  .desktop-v62-table{display:none}
  .mobile-metrics{display:grid;grid-template-columns:1fr;gap:10px;margin-top:12px}
  .metric-card{background:#f8fafc;border:1px solid #e7ebf1;border-radius:14px;padding:12px}
  .metric-card .name{font-size:17px;font-weight:900;margin-bottom:8px}
  .metric-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}
  .metric-box{background:#fff;border-radius:10px;padding:9px;border:1px solid #edf0f4}
  .metric-box small{display:block;color:#667085;font-size:11px;margin-bottom:2px}
  .metric-box b{font-size:17px}
  .scroll-hint{display:block}
  details.card>summary{font-size:20px!important;line-height:1.25}
  details.section-fold>summary{font-size:18px}
}

</style></head><body><div class="wrap">
<div class="top"><div><h1>VCP Radar Cloud · V6.5.0</h1><div class="muted">当前研究版：V6.5.0 Opportunity Ranking Diagnostic · 最新版本优先 · 历史版本收纳在下方</div></div>
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


<div class="quick-nav">
<a class="primary" href="#v62-current">🚀 V6.5.0 当前版</a>
<a href="#v61-fast">⚡ V6.1 Fast</a>
<a href="#research-archive">历史版本</a>
<a href="#top-candidates">Top 候选</a>
</div>
<div id="latest-version-mount"></div>
{% if sector_rows %}
<details class="card" style="margin-top:18px">
<summary style="font-size:24px;font-weight:800;cursor:pointer;list-style:none">
板块强弱 <span class="muted" style="font-size:15px;font-weight:600">（点开查看）</span>
</summary>
<div style="margin-top:16px">
<div class="scroll"><table style="min-width:520px">
<tr><th>板块</th><th>候选数</th><th>平均评分</th><th>90+数量</th></tr>
{% for s in sector_rows %}<tr><td><b>{{s.sector}}</b></td><td>{{s.count}}</td><td>{{"%.1f"|format(s.avg_score)}}</td><td>{{s.count90}}</td></tr>{% endfor %}
</table></div></div>
{% endif %}

<details id="top-candidates" class="card" style="margin-top:18px">
<summary style="font-size:28px;font-weight:800;cursor:pointer;list-style:none">
Top 候选 <span class="muted" style="font-size:16px;font-weight:600">（点开查看）</span>
</summary>
<div style="margin-top:18px">
<div><div class="top"><h2>Top 候选</h2><div>
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



</div>
</details>

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
</details>

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










{% if v462_probe %}
<details class="section-fold" open>
<summary>V4.6.2 Deploy Probe · 实际加载的是哪个发动机文件</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.6.2 Deploy Probe</h3>
<div class="muted" style="margin-bottom:10px">
不需要跑 Full122。这里直接检查 Railway 容器内实际存在和 Python 实际 import 的 jlaw_v3_phase1.py。
</div>
<div style="margin-bottom:10px">
<b>CWD:</b> {{v462_probe.cwd}}<br>
<b>App:</b> {{v462_probe.app_file}}<br>
<b>Imported module:</b> {{v462_probe.imported.module_file}}<br>
<b>Imported has V4.6:</b> {{v462_probe.imported.has_diag_attr}} /
{{v462_probe.imported.contains_v461}}
</div>
<div class="scroll"><table style="min-width:1100px">
<tr><th>路径</th><th>存在</th><th>大小</th><th>SHA12</th><th>含V4.6.1-DIAG</th><th>含V4.6代码</th><th>mtime</th></tr>
{% for r in v462_probe.candidates %}
<tr>
<td>{{r.path}}</td><td>{{r.exists}}</td><td>{{r.size}}</td><td><b>{{r.sha12}}</b></td>
<td>{{r.has_v461}}</td><td>{{r.has_v46}}</td><td>{{r.mtime}}</td>
</tr>
{% endfor %}
</table></div>
</div>
</details>
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




{% if v50_status %}
<details class="section-fold" open>
<summary>V5.0 Expanded Universe · 扩大股票池验证</summary>
<div class="fold-body">
<h3 style="margin-top:0">V5.3 Independent Validation</h3>
<div class="muted" style="margin-bottom:10px">
冻结 V4.8 / V4.6 规则；只跑新增验证组。EQR、MMC、TYLER 本轮允许缺失并自动跳过；验证结果写入独立目录，不覆盖原122只历史证据。
</div>
<div style="margin:10px 0">
<form method="post" action="/v5/validation/start" style="display:inline">
<button {% if v5v_status.status in ["running","stopping"] %}disabled style="opacity:.45"{% endif %}>
{% if v5v_status.status=="running" %}Validation 运行中…{% else %}▶ Validation 324 独立测试{% endif %}
</button></form>
{% if v5v_status.status=="running" %}
<form method="post" action="/v5/validation/stop" style="display:inline">
<button style="background:#7f1d1d">■ 停止 Validation</button>
</form>
{% endif %}
</div>
{% if v5v_status.status=="running" %}
<div class="progress"><div style="width:{{((v5v_status.processed/v5v_status.stocks*100) if v5v_status.stocks else 0)}}%"></div></div>
<div><b>{{v5v_status.processed}} / {{v5v_status.stocks}}</b> · {{v5v_status.current}}</div>
{% endif %}
<p><b>Validation 状态：</b>{{v5v_status.status}}　{{v5v_status.message}}</p>
{% if v5v_status.error %}<p class="warn">{{v5v_status.error}}</p>{% endif %}
<div class="scroll"><table style="min-width:900px">
<tr><th>总股票池</th><th>原研究组</th><th>新增验证组</th><th>已缓存</th><th>未缓存</th><th>原组缓存</th><th>验证组缓存</th><th>Ready</th></tr>
<tr><td><b>{{v50_status.universe}}</b></td><td>{{v50_status.original}}</td><td>{{v50_status.validation}}</td><td>{{v50_status.cached}}</td><td>{{v50_status.missing}}</td><td>{{v50_status.original_cached}}</td><td>{{v50_status.validation_cached}}</td><td><b>{{v50_status.ready}}</b></td></tr>
</table></div>
{% if v50_status.missing %}
<div style="margin-top:10px"><b>缺失：</b> {{v50_status.next_missing}}</div>
{% endif %}
{% if v50_status.deferred %}
<div style="margin-top:6px"><b>本轮允许跳过：</b> {{v50_status.deferred}}</div>
{% endif %}
<div style="margin-top:6px"><b>验证组当前可用：</b> {{v50_status.validation_usable}} / {{v50_status.validation}}</div>


<div class="card" style="margin-top:18px">
<h3 style="margin-top:0">V6.0 J Law Decision Shadow</h3>
<div class="muted" style="margin-bottom:10px">
新架构：Context → Edge Stack → Red-Flag Veto → Entry Quality → Ranking。独立写入 /data/jlaw_v6_shadow，不覆盖 V5.3。
</div>

<div style="margin:10px 0">
<form method="post" action="/v6/shadow/start" style="display:inline">
<button {% if v6_status.status=="running" %}disabled style="opacity:.45"{% endif %}>
{% if v6_status.status=="running" %}V6 Shadow 运行中…{% else %}▶ 运行 V6 Shadow{% endif %}
</button></form>
{% if v6_status.status=="running" %}
<form method="post" action="/v6/shadow/stop" style="display:inline">
<button style="background:#7f1d1d">■ 停止 V6 Shadow</button>
</form>
{% endif %}
</div>

{% if v6_status.status=="running" %}
<div class="progress"><div style="width:{{((v6_status.processed/v6_status.stocks*100) if v6_status.stocks else 0)}}%"></div></div>
<div><b>{{v6_status.processed}} / {{v6_status.stocks}}</b> · {{v6_status.current}}</div>
{% endif %}

<p><b>状态：</b>{{v6_status.status}}　{{v6_status.message}}</p>
{% if v6_status.error %}<p class="warn">{{v6_status.error}}</p>{% endif %}

{% if v6_summary %}
<details style="margin-top:12px">
<summary style="font-weight:800;cursor:pointer">V6 Shadow 结果（点开）</summary>
<div style="margin-top:10px">
<h4>V6 Shadow 结果</h4>
<div class="scroll"><table style="min-width:760px">
<tr><th>分组</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v6_summary %}
<tr>
<td><b>{{r.segment}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td>
<td><b>{{"%.2f"|format(r.pf)}}</b></td>
<td>{{r.max_losing_streak}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}
</div></details>

{% if v6_characters %}
<details style="margin-top:12px">
<summary style="font-weight:800;cursor:pointer">Market Character（点开）</summary>
<div style="margin-top:10px">
<h4>Market Character</h4>
<div class="scroll"><table style="min-width:700px">
<tr><th>环境</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v6_characters %}
<tr>
<td>{{r.character}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td>
<td>{{"%.2f"|format(r.pf)}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}
</div></details>
</div>


{% if v61_lab.available %}
<details class="card" style="margin-top:18px">
<summary style="font-size:24px;font-weight:800;cursor:pointer">🧪 V6.1 Decision Lab（不重跑）</summary>
<div class="muted" style="margin:10px 0">读取刚完成的 V6 Shadow 结果；不重跑321只。</div>

<details open style="margin-top:10px">
<summary style="font-weight:800;cursor:pointer">Market Character × Tier</summary>
<div class="scroll"><table style="min-width:820px">
<tr><th>环境</th><th>层级</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v61_lab.matrix %}
<tr><td>{{r.character}}</td><td><b>{{r.tier}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format(r.win_rate*100)}}</td><td>{{"%.2f%%"|format(r.avg_return*100)}}</td>
<td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>{% endfor %}
</table></div></details>

<details style="margin-top:10px"><summary style="font-weight:800;cursor:pointer">Edge Count 单调性</summary>
<div class="scroll"><table style="min-width:720px">
<tr><th>Edges</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v61_lab.edges %}<tr><td>{{r.edge_count}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td></tr>{% endfor %}
</table></div></details>

<details style="margin-top:10px"><summary style="font-weight:800;cursor:pointer">Ranking 是否有用</summary>
<div class="scroll"><table style="min-width:700px">
<tr><th>Rank</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v61_lab.ranks %}<tr><td>{{r.rank}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td></tr>{% endfor %}
</table></div></details>

<details style="margin-top:10px"><summary style="font-weight:800;cursor:pointer">J Law 启发组合（描述性）</summary>
<div class="scroll"><table style="min-width:760px">
<tr><th>组合</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v61_lab.opportunity %}<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td></tr>{% endfor %}
</table></div></details>
</details>
{% endif %}


<details id="v62-current" class="card current-card" style="margin-top:18px" data-version="6.2">
<summary style="font-size:25px;font-weight:900;cursor:pointer">🚀 V6.2.8 Research Archive <span class="version-label">当前</span></summary>
<div class="muted" style="margin:10px 0">
完整历史只算一次。以后改 POWER / A1 / A2 / Edge / Ranking 直接读缓存，
不再重扫321只，目标从几小时降到几秒～几分钟。
</div>
<p><b>缓存状态：</b>
{% if v62_status.ready %}
✅ ready · {{v62_status.signals}} signals · {{v62_status.trades}} trades · 来源 {{v62_status.source}}
{% else %}
未建立
{% endif %}
</p>
<form method="post" action="/v62/build-cache">
<button>⚡ 建立/更新研究缓存（不重跑）</button>
</form>
{% if v62_lab.available %}
<details open style="margin-top:12px">
<summary style="font-weight:800;cursor:pointer">Instant Lab 结果</summary>
<div class="scroll-hint">手机会自动用卡片显示；电脑仍显示完整表格。</div>
<div class="scroll desktop-v62-table"><table style="min-width:820px">
<tr><th>规则组合</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.rows %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format(r.win_rate*100)}}</td><td>{{"%.2f%%"|format(r.avg_return*100)}}</td>
<td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
<div class="mobile-metrics">
{% for r in v62_lab.rows %}
<div class="metric-card">
<div class="name">{{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</details>
<details style="margin-top:10px">
<summary style="font-weight:800;cursor:pointer">Ranking 诊断</summary>
<div class="scroll desktop-v62-table"><table style="min-width:700px">
<tr><th>Rank</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.ranks %}
<tr><td>{{r.name}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
<div class="mobile-metrics">
{% for r in v62_lab.ranks %}
<div class="metric-card">
<div class="name">{{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</details>

<details open style="margin-top:12px">
<summary style="font-weight:900;cursor:pointer">🧪 Volume × RS 稳定性网格</summary>
<div class="muted" style="margin:8px 0">
目的：不是找单一最高 PF，而是找附近多个门槛都表现良好的“稳定平台”。样本太小的组合只作观察。
</div>

<div class="scroll desktop-v62-table">
<table style="min-width:920px">
<tr>
<th>Volume</th><th>RS</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th>
</tr>
{% for r in v62_lab.stability %}
<tr>
<td><b>≥{{"%.1f"|format(r.vol)}}x</b></td>
<td><b>≥{{r.rs}}</b></td>
<td>{{r.trades}}</td>
<td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td>
<td><b>{{"%.2f"|format(r.pf)}}</b></td>
<td>{{r.max_losing_streak}}</td>
</tr>
{% endfor %}
</table>
</div>

<div class="mobile-metrics">
{% for r in v62_lab.stability %}
<div class="metric-card">
<div class="name">Vol ≥{{"%.1f"|format(r.vol)}}x · RS ≥{{r.rs}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>

<details open style="margin-top:12px">
<summary style="font-weight:900;cursor:pointer">🧱 非重叠 Volume 区间 · RS≥90</summary>
<div class="muted" style="margin:8px 0">
这组最重要：每个区间互不重叠，用来确认 1.8x 以后是否真的逐段变强，而不是累计门槛造成的假象。
</div>
<div class="scroll desktop-v62-table">
<table style="min-width:760px">
<tr><th>Volume 区间</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.volume_buckets %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table>
</div>
<div class="mobile-metrics">
{% for r in v62_lab.volume_buckets %}
<div class="metric-card">
<div class="name">{{r.name}} · RS≥90</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</details>

<details open style="margin-top:12px">
<summary style="font-weight:900;cursor:pointer">🕒 CORE 跨时期稳定性 · Vol≥1.8x + RS≥90</summary>
<div class="muted" style="margin:8px 0">
检查这个候选核心规则是不是只靠某一段牛市撑起来。先看四个大时期，再看逐年明细。
</div>
<div class="scroll desktop-v62-table">
<table style="min-width:760px">
<tr><th>时期</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.era_rows %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table>
</div>
<div class="mobile-metrics">
{% for r in v62_lab.era_rows %}
<div class="metric-card">
<div class="name">{{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>

<details style="margin-top:10px">
<summary style="font-weight:800;cursor:pointer">逐年明细</summary>
<div class="scroll desktop-v62-table">
<table style="min-width:760px">
<tr><th>年份</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.yearly_rows %}
<tr><td><b>{{r.year}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table>
</div>
<div class="mobile-metrics">
{% for r in v62_lab.yearly_rows %}
<div class="metric-card">
<div class="name">{{r.year}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</details>
</details>

</details>


<details open style="margin-top:12px">
<summary style="font-weight:900;cursor:pointer">🩺 V6.2.5 Fault Isolation · 最近失效诊断</summary>
<div class="muted" style="margin:8px 0">
核心规则固定为 POWER + Vol≥1.8x + RS≥90。这里不调参数，只检查为什么 2022–2026 表现掉下来。
</div>

<h3 style="margin:14px 0 8px">时期对比</h3>
<div class="mobile-metrics">
{% for r in v62_lab.fault_compare %}
<div class="metric-card">
<div class="name">{{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>

<div class="scroll desktop-v62-table">
<table style="min-width:760px">
<tr><th>时期</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.fault_compare %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table>
</div>

<h3 style="margin:16px 0 8px">2022–2026 · 实际 CORE 交易</h3>
{% if v62_lab.fault_recent %}
<div class="mobile-metrics">
{% for r in v62_lab.fault_recent %}
<div class="metric-card">
<div class="name">{{r.ticker or "?"}} · {{r.date}}</div>
<div class="metric-grid">
<div class="metric-box"><small>收益</small><b>{% if r.return_pct is not none %}{{"%.2f%%"|format(r.return_pct*100)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Volume / RS</small><b>{% if r.volratio is not none %}{{"%.2f"|format(r.volratio)}}x{% else %}-{% endif %} / {% if r.rs is not none %}{{"%.1f"|format(r.rs)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Supply / Sector</small><b>{% if r.supply_score is not none %}{{"%.1f"|format(r.supply_score)}}{% else %}-{% endif %} / {% if r.sector_score is not none %}{{"%.1f"|format(r.sector_score)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Leader / Group</small><b>{% if r.leader_score is not none %}{{"%.1f"|format(r.leader_score)}}{% else %}-{% endif %} / {% if r.group_breadth is not none %}{{"%.1f"|format(r.group_breadth)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Extension</small><b>{% if r.extension is not none %}{{"%.2f%%"|format(r.extension*100)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Edge / Red / Tier / Rank</small><b>{{r.v6_edge_count or "-"}} / {{r.v6_red_count or "-"}} / {{r.v6_tier or "-"}} / {{r.v6_rank or "-"}}</b></div>
</div>
</div>
{% endfor %}
</div>
{% else %}
<div class="muted">没有找到 2022–2026 CORE 交易。</div>
{% endif %}

<h3 style="margin:16px 0 8px">特征中位数对比</h3>
<div class="muted" style="margin-bottom:8px">看“早期 vs 最近”以及“赢家 vs 输家”有没有结构性差异。</div>
<div class="scroll">
<table style="min-width:760px">
<tr><th>特征</th><th>2007–2021</th><th>2022–2026</th><th>赢家</th><th>输家</th></tr>
{% for r in v62_lab.fault_features %}
<tr>
<td><b>{{r.feature}}</b></td>
<td>{% if r.early is not none %}{{"%.3f"|format(r.early)}}{% else %}-{% endif %}</td>
<td>{% if r.recent is not none %}{{"%.3f"|format(r.recent)}}{% else %}-{% endif %}</td>
<td>{% if r.winners is not none %}{{"%.3f"|format(r.winners)}}{% else %}-{% endif %}</td>
<td>{% if r.losers is not none %}{{"%.3f"|format(r.losers)}}{% else %}-{% endif %}</td>
</tr>
{% endfor %}
</table>
</div>
</details>




{% endif %}
</details>



<div class="card" id="publisher-top" style="margin-top:10px;border:2px solid #bbf7d0">
  <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap">
    <div>
      <h2 style="margin:0 0 4px">☁️ 自动发布</h2>
      <div class="muted">常用入口已放最上面；密码可在本机记住。</div>
    </div>
    <a href="/publisher/control" style="text-decoration:none">
      <button type="button">🚀 打开上传发布</button>
    </a>
  </div>
</div>
<div class="card current-card" id="v628-diagnostic" style="margin-top:18px">
<h2 style="margin:0 0 6px">🧭 V6.2.8 Recent Regime Diagnostics · 2022–2026</h2>
<div class="muted" style="margin-bottom:12px">
这是独立显示区，不放在折叠菜单里，确保手机和PDF都能看到真实结果。底层仍读取 V6.1 Full 冻结缓存，不重跑321只。
</div>

<h3 style="margin:14px 0 8px">Market Character · 最近几年</h3>
{% if v62_lab.recent_regime %}
<div class="scroll"><table style="min-width:760px">
<tr><th>Market Character</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.recent_regime %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
{% else %}
<div style="padding:10px;border-radius:10px;background:#fff7ed">⚠️ Market Character 最近几年无可显示数据。</div>
{% endif %}

<h3 style="margin:16px 0 8px">POWER_TREND 内 · Volume 非重叠区间</h3>
{% if v62_lab.recent_power_buckets %}
<div class="scroll"><table style="min-width:760px">
<tr><th>Volume区间</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.recent_power_buckets %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
{% else %}
<div style="padding:10px;border-radius:10px;background:#fff7ed">⚠️ 最近 POWER_TREND 的 Volume 分桶无可显示数据。</div>
{% endif %}

<h3 style="margin:16px 0 8px">POWER_TREND 内 · RS 非重叠区间</h3>
{% if v62_lab.recent_rs_buckets %}
<div class="scroll"><table style="min-width:760px">
<tr><th>RS区间</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.recent_rs_buckets %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
{% else %}
<div style="padding:10px;border-radius:10px;background:#fff7ed">⚠️ 最近 POWER_TREND 的 RS 分桶无可显示数据。</div>
{% endif %}
</div>




<div class="card" style="margin-top:12px;border:2px solid #bbf7d0">
<h3 style="margin:0 0 6px">☁️ Cloud Publisher 已嵌入</h3>
<div class="muted">
设置 Railway Variables 后，这个同一个 Service 就能通过 GitHub API 更新 app.py / jlaw_*.py；
主 Railway 服务会因 GitHub 更新自动重新 Deploy。不会新增第二个 Service。
</div>
<div style="margin-top:8px;font-size:14px">
检查地址：<code>/publisher/health</code>
</div>
<div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
<a href="/publisher/control" style="text-decoration:none;flex:1;min-width:220px">
<button type="button" style="width:100%">☁️ 打开自动发布控制台</button>
</a>
<a href="/publisher/test-page" style="text-decoration:none;flex:1;min-width:220px">
<button type="button" style="width:100%">🧪 安全测试</button>
</a>
</div>
</div>
<div class="card current-card" id="v63-current" style="margin-top:18px;border:2px solid #b7d4ff">
<h2 style="margin:0 0 8px">🚀 V6.3.9.10 RS2 + Feature Store</h2>
<div class="muted" style="margin-bottom:10px">
底层升级：RS 改成每日横截面 Percentile Ranking，并保留 uncapped RS Raw。
Feature Store 只建立一次；Peer/Group RS 直接读取缓存，避免重复计算。
</div>
<p><b>状态：</b> <span id="v63-state">{{v63_status.state}}</span> · <span id="v63-message">{{v63_status.message}}</span></p>

<div id="v63-progress-wrap" style="margin:12px 0">
  <div style="display:flex;justify-content:space-between;gap:10px;font-weight:800;margin-bottom:6px">
    <span id="v63-phase">{% if v63_status.done == 0 and v63_status.state == "running" %}RS2 Feature Store 建立中{% else %}全量股票扫描{% endif %}</span>
    <span id="v63-count">{{v63_status.done}} / {{v63_status.total or 321}}</span>
  </div>
  <div style="height:14px;background:#e8edf5;border-radius:999px;overflow:hidden">
    <div id="v63-progress-bar" style="height:100%;width:{{ ((v63_status.done or 0) / (v63_status.total or 321) * 100)|round(1) }}%;background:#111827;transition:width .35s ease"></div>
  </div>
  <div class="muted" style="margin-top:6px">
    <span id="v63-percent">{{ ((v63_status.done or 0) / (v63_status.total or 321) * 100)|round(1) }}%</span>
    <span id="v63-ticker">{% if v63_status.ticker %} · 当前 {{v63_status.ticker}}{% endif %}</span>
    <span id="v63-elapsed"></span>
  </div>
</div>

{% if v63_status.error %}<div id="v63-error" style="padding:10px;background:#fff1f1;border-radius:10px">⚠️ {{v63_status.error}}</div>{% else %}<div id="v63-error" style="display:none;padding:10px;background:#fff1f1;border-radius:10px"></div>{% endif %}
<div style="display:flex;gap:8px;flex-wrap:wrap;margin:10px 0">
<a href="/v63/context-veto" style="text-decoration:none"><button type="button">🧱 打开 Context Veto 独立页</button></a>
<form method="post" action="/v63/run"><button>▶ V6.3 全量 + RS2 Build</button></form>
<form method="post" action="/v63/stop"><button>■ 停止 / 保留Checkpoint</button></form>
</div>

{% if v63_summary.available %}
<h3 style="margin:14px 0 8px">V6.3 新RS结果</h3>
<div class="mobile-metrics">
{% for r in v63_summary.rows %}
<div class="metric-card"><div class="name">{{r.name}}</div><div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div></div>
{% endfor %}
</div>
<div class="scroll desktop-v62-table"><table style="min-width:780px">
<tr><th>组合</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v63_summary.rows %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>

<details open style="margin-top:14px">
<summary style="font-weight:900;cursor:pointer">📊 V6.3.5 结果诊断（不重跑）</summary>
<div class="muted" style="margin:8px 0">重点看新 RS2 在 2022–2026 是否真的改善，以及 RS2 是否有合理分层。</div>

<h3>年代稳定性</h3>
<div class="mobile-metrics">
{% for r in v63_summary.eras %}
<div class="metric-card"><div class="name">{{r.name}}</div><div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数/最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div></div>
{% endfor %}
</div>

<h3 style="margin-top:14px">2022–2026 单独检查</h3>
<div class="mobile-metrics">
{% for r in v63_summary.recent %}
<div class="metric-card"><div class="name">{{r.name}}</div><div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数/最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div></div>
{% endfor %}
</div>

<h3 style="margin-top:14px">RS2 非重叠分层</h3>
<div class="mobile-metrics">
{% for r in v63_summary.rsbands %}
<div class="metric-card"><div class="name">RS2 {{r.name}}</div><div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数/最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div></div>
{% endfor %}
</div>

<h3 style="margin-top:16px">Cohort 拆分 · 原122 vs 新增验证组</h3>
<div class="muted" style="margin-bottom:8px">确认 V6.3 的改善是不是被原研究样本拉高。这里不重跑，只按 ticker 归组读取现有 V6.3 trades。</div>
<div class="mobile-metrics">
{% for r in v63_summary.cohort_rows %}
<div class="metric-card">
<div class="name">{{r.cohort}} · {{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>

<h3 style="margin-top:16px">2022–2026 · RS2 非重叠分层</h3>
<div class="mobile-metrics">
{% for r in v63_summary.recent_rs2_bands %}
<div class="metric-card">
<div class="name">RS2 {{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>


<h3 style="margin-top:18px">🧪 Validation CORE 38笔 · 稳健性拆解</h3>
<div class="muted" style="margin-bottom:8px">
只看新增验证组中的候选：POWER_TREND + Volume≥1.8x + RS2≥90。
不重跑，只从现有 V6.3 trades 拆分。
</div>

{% for group_name in ["BASE","VOLUME","RS2","SUPPLY","ENTRY","SECTOR","GROUP"] %}
<details open style="margin-top:10px">
<summary style="font-weight:900;cursor:pointer">{{group_name}}</summary>
<div class="mobile-metrics">
{% for r in v63_summary.validation_core if r.group == group_name %}
<div class="metric-card">
<div class="name">{{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</details>
{% endfor %}

<h3 style="margin-top:16px">Validation CORE · 年代稳定性</h3>
<div class="mobile-metrics">
{% for r in v63_summary.validation_core_eras %}
<div class="metric-card">
<div class="name">{{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>

<h3 style="margin-top:18px">🩺 Validation CORE · 2022–2026 逐笔</h3>
<div class="muted" style="margin-bottom:8px">因为最近时期只有4笔且 PF 0.83，这里直接看是哪4笔拖累，不再继续盲调阈值。</div>
<div class="mobile-metrics">
{% for r in v63_summary.validation_core_recent_trades %}
<div class="metric-card">
<div class="name">{{r.ticker or "?"}} · {{r.date}}</div>
<div class="metric-grid">
<div class="metric-box"><small>收益</small><b>{% if r.return_pct is not none %}{{"%.2f%%"|format(r.return_pct*100)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Vol / RS2</small><b>{% if r.volratio is not none %}{{"%.2f"|format(r.volratio)}}x{% else %}-{% endif %} / {% if r.rs is not none %}{{"%.1f"|format(r.rs)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Supply / Sector</small><b>{% if r.supply_score is not none %}{{"%.1f"|format(r.supply_score)}}{% else %}-{% endif %} / {% if r.sector_score is not none %}{{"%.1f"|format(r.sector_score)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Leader / Group</small><b>{% if r.leader_score is not none %}{{"%.1f"|format(r.leader_score)}}{% else %}-{% endif %} / {% if r.group_breadth is not none %}{{"%.1f"|format(r.group_breadth)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Extension</small><b>{% if r.extension is not none %}{{"%.2f%%"|format(r.extension*100)}}{% else %}-{% endif %}</b></div>
<div class="metric-box"><small>Edge / Red / Tier / Rank</small><b>{{r.v6_edge_count or "-"}} / {{r.v6_red_count or "-"}} / {{r.v6_tier or "-"}} / {{r.v6_rank or "-"}}</b></div>
</div>
</div>
{% endfor %}
</div>

<h3 style="margin-top:16px">早期 vs 最近 · 特征中位数</h3>
<div class="scroll">
<table style="min-width:620px">
<tr><th>特征</th><th>2007–2021</th><th>2022–2026</th></tr>
{% for r in v63_summary.validation_core_feature_compare %}
<tr><td><b>{{r.feature}}</b></td>
<td>{% if r.early is not none %}{{"%.3f"|format(r.early)}}{% else %}-{% endif %}</td>
<td>{% if r.recent is not none %}{{"%.3f"|format(r.recent)}}{% else %}-{% endif %}</td></tr>
{% endfor %}
</table>
</div>

<h3 style="margin-top:18px">🧱 V6.3.9 Context Veto Diagnostic</h3>
<div class="muted" style="margin-bottom:8px">
目标：不是继续调 RS2/Volume 门槛，而是找“哪些结构应该直接否决”。重点看 Tier、Edge Count、Supply、Extension 及其组合。
</div>

{% for group_name in ["TIER","EDGE","SUPPLY","ENTRY","COMBO"] %}
<details open style="margin-top:10px">
<summary style="font-weight:900;cursor:pointer">{{group_name}}</summary>
<div class="mobile-metrics">
{% for r in v63_summary.context_veto if r.group == group_name %}
<div class="metric-card">
<div class="name">{{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</details>
{% endfor %}

<h3 style="margin-top:16px">2022–2026 · Context Veto</h3>
<div class="mobile-metrics">
{% for r in v63_summary.context_veto_recent %}
<div class="metric-card">
<div class="name">{{r.group}} · {{r.name}}</div>
<div class="metric-grid">
<div class="metric-box"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric-box"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric-box"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric-box"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</details>
{% endif %}
</div>

<div class="card current-card" id="v629-rsdiag" style="margin-top:18px">
<h2 style="margin:0 0 6px">🎯 V6.2.9 RS Saturation Diagnostic</h2>
<div class="muted" style="margin-bottom:12px">
重点检查最近几年为什么 RS=100 反而表现很差：它是真正“过强/过热”，还是 RS 分数被封顶后失去区分度。
全部直接读现有缓存，不重跑321只。
</div>

<h3>2022–2026 POWER_TREND · 逐年</h3>
<div class="scroll"><table style="min-width:720px">
<tr><th>年份</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.recent_power_years %}
<tr><td><b>{{r.year}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>

<h3 style="margin-top:16px">RS Band · 特征剖面</h3>
<div class="scroll"><table style="min-width:1100px">
<tr>
<th>RS组</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th>
<th>Vol中位</th><th>Supply</th><th>Sector</th><th>Leader</th><th>Group</th><th>Extension</th>
</tr>
{% for r in v62_lab.rs_band_profiles %}
<tr>
<td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td>
<td>{% if r.volratio is not none %}{{"%.2f"|format(r.volratio)}}x{% else %}-{% endif %}</td>
<td>{% if r.supply_score is not none %}{{"%.1f"|format(r.supply_score)}}{% else %}-{% endif %}</td>
<td>{% if r.sector_score is not none %}{{"%.1f"|format(r.sector_score)}}{% else %}-{% endif %}</td>
<td>{% if r.leader_score is not none %}{{"%.1f"|format(r.leader_score)}}{% else %}-{% endif %}</td>
<td>{% if r.group_breadth is not none %}{{"%.1f"|format(r.group_breadth)}}{% else %}-{% endif %}</td>
<td>{% if r.extension is not none %}{{"%.2f%%"|format(r.extension*100)}}{% else %}-{% endif %}</td>
</tr>
{% endfor %}
</table></div>

<h3 style="margin-top:16px">RS=100 · 再按 Volume 拆</h3>
<div class="scroll"><table style="min-width:720px">
<tr><th>Volume区间</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v62_lab.rs100_profile %}
<tr><td><b>{{r.name}}</b></td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
</div>

<details id="v61-fast" class="card" style="margin-top:18px" data-version="6.1">
<summary style="font-size:24px;font-weight:800;cursor:pointer">⚡ V6.1 Fast Engine <span class="version-label">上一版</span></summary>
<div class="muted" style="margin:10px 0">
2线程并行 + 每只股票 checkpoint。中断后再次运行会跳过已经完成的股票。先 Quick 50（50只代表样本、全历史），再决定是否 Full 321。旧版 Quick50 只看420日，可能出现0笔交易；这版已修正。
</div>
<div style="display:flex;gap:8px;flex-wrap:wrap;margin:10px 0">
<form method="post" action="/v61/quick50"><button {% if v61_status.status=="running" %}disabled style="opacity:.45"{% endif %}>⚡ Quick 50</button></form>
<form method="post" action="/v61/full"><button {% if v61_status.status=="running" %}disabled style="opacity:.45"{% endif %}>▶ Full 321 Fast</button></form>
{% if v61_status.status=="running" %}<form method="post" action="/v61/stop"><button style="background:#7f1d1d">■ 停止（可续跑）</button></form>{% endif %}
</div>
{% if v61_status.status=="running" %}
<div class="progress"><div style="width:{{((v61_status.processed/v61_status.stocks*100) if v61_status.stocks else 0)}}%"></div></div>
<div><b>{{v61_status.processed}} / {{v61_status.stocks}}</b> · {{v61_status.current}}</div>
{% endif %}
<p><b>状态：</b>{{v61_status.status}}　{{v61_status.message}}</p>
{% if v61_status.error %}<p class="warn">{{v61_status.error}}</p>{% endif %}

{% if v61_quick %}
<details open style="margin-top:10px"><summary style="font-weight:800;cursor:pointer">Quick 50 结果</summary>
<div class="scroll"><table style="min-width:700px"><tr><th>分组</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v61_quick %}<tr><td>{{r.segment}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td><td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td></tr>{% endfor %}
</table></div></details>{% endif %}

{% if v61_status.status=="done" and v61_status.mode=="quick50" and not v61_quick %}
<div style="margin-top:10px;padding:10px;border-radius:10px;background:#fff7ed">
Quick 50 已完成，但没有产生 V6 交易记录。旧版420日Quick可能出现这种情况；请使用本修正版重新跑 Quick 50。
</div>
{% endif %}

{% if v61_full %}
<details style="margin-top:10px"><summary style="font-weight:800;cursor:pointer">Full Fast 结果</summary>
<div class="scroll"><table style="min-width:700px"><tr><th>分组</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v61_full %}<tr><td>{{r.segment}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td><td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td></tr>{% endfor %}
</table></div></details>{% endif %}
</details>

{% if v5_autopsy.available %}
<div id="research-archive" class="archive-title">历史研究版本 · 最新在上，最旧在下</div>
<details class="card" style="margin-top:18px">
<summary style="font-size:20px;font-weight:800;cursor:pointer">🔬 V5.4 OOS Failure Autopsy（不重跑）</summary>
<div class="muted" style="margin:10px 0">
只读取已经完成的 Validation 交易文件做失败解剖；不改参数、不重新回测、不污染这次 OOS。
</div>

{% if v5_autopsy.flags %}
<div style="padding:12px;border-radius:12px;background:#fff7ed;margin:10px 0">
{% for x in v5_autopsy.flags %}<div>• {{x}}</div>{% endfor %}
</div>
{% endif %}

<p><a class="btn" href="/v5/autopsy.csv">导出 Autopsy CSV</a></p>

<h4>分数区间是否真的单调</h4>
<div class="scroll"><table style="min-width:760px">
<tr><th>Score</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v5_autopsy.score_bins %}
<tr><td><b>{{r.band}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format(r.win_rate*100)}}</td><td>{{"%.2f%%"|format(r.avg_return*100)}}</td>
<td><b>{{"%.2f"|format(r.pf)}}</b></td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>

<h4>Regime</h4>
<div class="scroll"><table style="min-width:650px">
<tr><th>Regime</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v5_autopsy.regimes %}
<tr><td>{{r.regime}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td></tr>
{% endfor %}
</table></div>

<h4>时代分段</h4>
<div class="scroll"><table style="min-width:650px">
<tr><th>时期</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v5_autopsy.eras %}
<tr><td>{{r.era}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format(r.win_rate*100)}}</td>
<td>{{"%.2f%%"|format(r.avg_return*100)}}</td><td>{{"%.2f"|format(r.pf)}}</td></tr>
{% endfor %}
</table></div>

<h4>因子区间（描述性，不拿来调参）</h4>
<div class="scroll"><table style="min-width:900px">
<tr><th>因子</th><th>区间</th><th>交易</th><th>胜率</th><th>平均收益</th><th>PF</th></tr>
{% for r in v5_autopsy.factor_rows %}
<tr><td>{{r.factor}}</td><td>{{r.band}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format(r.win_rate*100)}}</td><td>{{"%.2f%%"|format(r.avg_return*100)}}</td>
<td>{{"%.2f"|format(r.pf)}}</td></tr>
{% endfor %}
</table></div>

<h4>高分赢家 vs 高分输家：中位数画像</h4>
<div class="scroll"><table style="min-width:1100px">
<tr><th>组</th><th>N</th><th>Score</th><th>Vol</th><th>Quality</th><th>RS</th><th>Sector</th><th>Leader</th><th>Supply</th><th>MFE</th><th>MAE</th><th>Hold</th></tr>
{% for r in v5_autopsy.profile %}
<tr><td><b>{{r.group}}</b></td><td>{{r.n}}</td>
<td>{{"%.1f"|format(r.v46_score or 0)}}</td><td>{{"%.2f"|format(r.volratio or 0)}}</td>
<td>{{"%.1f"|format(r.quality or 0)}}</td><td>{{"%.1f"|format(r.rs or 0)}}</td>
<td>{{"%.1f"|format(r.sector_score or 0)}}</td><td>{{"%.1f"|format(r.leader_score or 0)}}</td>
<td>{{"%.1f"|format(r.supply_score or 0)}}</td>
<td>{{"%.1f%%"|format((r.mfe or 0)*100)}}</td><td>{{"%.1f%%"|format((r.mae or 0)*100)}}</td>
<td>{{"%.0f"|format(r.hold_days or 0)}}</td></tr>
{% endfor %}
</table></div>

<h4>最差25笔（重点解剖对象）</h4>
<div class="scroll"><table style="min-width:1150px">
<tr><th>Ticker</th><th>Date</th><th>Score</th><th>Return</th><th>Vol</th><th>Quality</th><th>RS</th><th>Sector</th><th>Leader</th><th>Supply</th><th>Regime</th><th>Exit</th></tr>
{% for r in v5_autopsy.top_losers %}
<tr><td><b>{{r.ticker}}</b></td><td>{{r.date}}</td>
<td>{{"%.1f"|format(r.v46_score or 0)}}</td><td>{{"%.2f%%"|format((r.return_pct or 0)*100)}}</td>
<td>{{"%.2f"|format(r.volratio or 0)}}</td><td>{{"%.1f"|format(r.quality or 0)}}</td>
<td>{{"%.1f"|format(r.rs or 0)}}</td><td>{{"%.1f"|format(r.sector_score or 0)}}</td>
<td>{{"%.1f"|format(r.leader_score or 0)}}</td><td>{{"%.1f"|format(r.supply_score or 0)}}</td>
<td>{{r.regime}}</td><td>{{r.reason}}</td></tr>
{% endfor %}
</table></div>
</details>
{% endif %}

{% if v5v_summary %}
<h4 style="margin-top:16px">本次 OOS 独立验证结果</h4>
<div class="scroll"><table style="min-width:760px">
<tr><th>层级</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v5v_summary %}
<tr>
<td><b>{{r.tier}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</td>
<td><b>{{"%.2f"|format(r.pf or 0)}}</b></td><td>{{r.max_losing_streak}}</td>
</tr>
{% endfor %}
</table></div>
{% endif %}
{% if v50_results %}
<h4 style="margin-top:16px">冻结规则验证：原组 vs 新增验证组</h4>
<div class="scroll"><table style="min-width:900px">
<tr><th>组别</th><th>门槛</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v50_results %}
<tr><td><b>{{r.cohort}}</b></td><td>{{r.threshold}}</td><td>{{r.trades}}</td><td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td><td>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</td><td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
{% endif %}
</div>
</details>
{% endif %}

{% if v48_tiers %}
<details class="section-fold" open>
<summary>V4.8 Dual Tier · CORE + ELITE 双层筛选</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.8 Dual Tier</h3>
<div class="muted" style="margin-bottom:10px">
不再用单一门槛“全有或全无”。CORE 保留稳定样本量，ELITE 作为优先级层。
</div>
<div class="scroll"><table style="min-width:1180px">
<tr><th>层级</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th>
<th>≤2021笔数</th><th>≤2021平均</th><th>≤2021 PF</th>
<th>2022+笔数</th><th>2022+平均</th><th>2022+ PF</th></tr>
{% for r in v48_tiers %}
<tr>
<td><b>{{r.tier}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td><b>{{"%.2f"|format(r.pf or 0)}}</b></td><td>{{r.max_losing_streak}}</td>
<td>{{r.old_trades}}</td><td>{{"%.2f%%"|format((r.old_avg or 0)*100)}}</td><td>{{"%.2f"|format(r.old_pf or 0)}}</td>
<td>{{r.recent_trades}}</td><td>{{"%.2f%%"|format((r.recent_avg or 0)*100)}}</td><td>{{"%.2f"|format(r.recent_pf or 0)}}</td>
</tr>
{% endfor %}
</table></div>

{% if v48_bins %}
<h4 style="margin-top:16px">非重叠 Score Band</h4>
<div class="scroll"><table style="min-width:860px">
<tr><th>分数区间</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v48_bins %}
<tr><td><b>{{r.score_band}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</td>
<td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
{% endif %}
</div>
</details>
{% endif %}

{% if v47_sweep %}
<details class="section-fold" open>
<summary>V4.7 True Engine Robustness · 真发动机门槛稳定性</summary>
<div class="fold-body">
<h3 style="margin-top:0">V4.7 True Engine Threshold Sweep</h3>
<div class="muted" style="margin-bottom:10px">
这里只读取真正由 V4.6 发动机生成的 jlaw_v46_trades.csv。
不再使用旧385笔做 App 端模拟。
</div>
<div style="margin:8px 0 14px"><b>Plateau判断：</b> {{v47_plateau.status}}
{% if v47_plateau.threshold is defined %} — 候选门槛 {{v47_plateau.threshold}}, {{v47_plateau.trades}}笔,
胜率 {{"%.1f%%"|format((v47_plateau.win_rate or 0)*100)}},
平均 {{"%.2f%%"|format((v47_plateau.avg_return or 0)*100)}},
PF {{"%.2f"|format(v47_plateau.pf or 0)}}{% endif %}
</div>
<div class="scroll"><table style="min-width:1320px">
<tr><th>门槛</th><th>总笔数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th>
<th>≤2021笔数</th><th>≤2021胜率</th><th>≤2021平均</th><th>≤2021 PF</th>
<th>2022+笔数</th><th>2022+胜率</th><th>2022+平均</th><th>2022+ PF</th></tr>
{% for r in v47_sweep %}
<tr>
<td><b>{{r.threshold}}</b></td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td><b>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</b></td>
<td><b>{{"%.2f"|format(r.pf or 0)}}</b></td><td>{{r.max_losing_streak}}</td>
<td>{{r.old_trades}}</td><td>{{"%.1f%%"|format((r.old_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.old_avg or 0)*100)}}</td><td>{{"%.2f"|format(r.old_pf or 0)}}</td>
<td>{{r.recent_trades}}</td><td>{{"%.1f%%"|format((r.recent_win or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.recent_avg or 0)*100)}}</td><td>{{"%.2f"|format(r.recent_pf or 0)}}</td>
</tr>
{% endfor %}
</table></div>

{% if v47_periods %}
<h4 style="margin-top:16px">70 / 85 / 90 分年代稳定性</h4>
<div class="scroll"><table style="min-width:900px">
<tr><th>门槛</th><th>时期</th><th>交易数</th><th>胜率</th><th>平均收益</th><th>PF</th><th>最大连亏</th></tr>
{% for r in v47_periods %}
<tr><td><b>{{r.threshold}}</b></td><td>{{r.period}}</td><td>{{r.trades}}</td>
<td>{{"%.1f%%"|format((r.win_rate or 0)*100)}}</td>
<td>{{"%.2f%%"|format((r.avg_return or 0)*100)}}</td>
<td>{{"%.2f"|format(r.pf or 0)}}</td><td>{{r.max_losing_streak}}</td></tr>
{% endfor %}
</table></div>
{% endif %}
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
document.addEventListener("DOMContentLoaded",function(){
  const mount=document.getElementById("latest-version-mount");
  const v63=document.getElementById("v63-current");
  const current=document.getElementById("v62-current");
  const fast=document.getElementById("v61-fast");
  if(mount){
    if(v63) mount.appendChild(v63);
    if(current) mount.appendChild(current);
    const diag=document.getElementById("v628-diagnostic");
    if(diag) mount.appendChild(diag);
    const rsdiag=document.getElementById("v629-rsdiag");
    if(rsdiag) mount.appendChild(rsdiag);
    if(fast) mount.appendChild(fast);

    // V6.0 was historically nested inside the V5 validation section. Pull it out so V6 stays above V5.
    const h3s=[...document.querySelectorAll("h3")];
    const v60h=h3s.find(x=>x.textContent.includes("V6.0 J Law Decision Shadow"));
    if(v60h){
      const v60=v60h.closest(".card");
      if(v60 && v60!==current && v60!==fast){
        v60.dataset.version="6.0";
        v60.classList.add("version-extracted");
        mount.appendChild(v60);
      }
    }

    // Also bring V6.1 Decision Lab directly under V6.1 Fast.
    const summaries=[...document.querySelectorAll("summary")];
    const labSummary=summaries.find(x=>x.textContent.includes("V6.1 Decision Lab"));
    if(labSummary){
      const lab=labSummary.closest("details.card");
      if(lab){lab.dataset.version="6.1";mount.appendChild(lab);}
    }
  }

  // Historical archive: collect remaining visible version blocks and sort newest -> oldest.
  const archive=document.getElementById("research-archive");
  if(archive){
    const root=document.querySelector(".wrap");
    const candidates=[...root.children].filter(el=>{
      if(el===mount || el===archive) return false;
      const t=(el.textContent||"").replace(/\s+/g," ");
      return /\bV(?:2(?:\.\d+)?|3(?:\.\d+)?|4(?:\.\d+)?|5(?:\.\d+)?)\b/i.test(t);
    });
    function versionOf(el){
      const t=(el.textContent||"");
      const nums=[...t.matchAll(/\bV(\d+)(?:\.(\d+))?/gi)].map(m=>parseFloat(m[1]+"."+(m[2]||"0")));
      return nums.length?Math.max(...nums):0;
    }
    candidates.sort((a,b)=>versionOf(b)-versionOf(a));
    let cursor=archive;
    candidates.forEach(el=>{
      cursor.insertAdjacentElement("afterend",el);
      cursor=el;
      // Old versions default collapsed when possible.
      if(el.tagName==="DETAILS" && versionOf(el)<6) el.removeAttribute("open");
    });
  }
});
</script>
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
async function poll(){try{const r=await fetch('/api/status',{cache:'no-store'});const s=await r.json();const b=await fetch('/api/backtest-status',{cache:'no-store'});const bs=await b.json();const vr=await fetch('/api/v3-status',{cache:'no-store'});const vs=await vr.json();const vv=await fetch('/api/v5-validation-status',{cache:'no-store'});const vvs=await vv.json();const v6r=await fetch('/api/v6-shadow-status',{cache:'no-store'});const v6s=await v6r.json();const f61=await fetch('/api/v61-fast-status',{cache:'no-store'});const f61s=await f61.json();if(s.status==='updating'||bs.status==='running'||(vs.status==='running'||vs.status==='stopping')||vvs.status==='running'||v6s.status==='running'||f61s.status==='running')setTimeout(()=>location.reload(),3000);}catch(e){}}
setTimeout(poll,5000);
</script>
<script>
document.addEventListener("DOMContentLoaded", function(){
  try{
    const top=document.querySelector(".top");
    if(top && !document.getElementById("v65-shortcut")){
      const box=document.createElement("div");
      box.id="v65-shortcut";
      box.className="card";
      box.style.cssText="margin:10px 0;border:2px solid #c4b5fd";
      box.innerHTML='<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap"><div><h2 style="margin:0">🎯 V6.5 Ranking Diagnostic</h2><div class="muted">不重跑：先看现有 Ranking 到底会不会排</div></div><a href="/v65/ranking-diagnostic"><button type="button">打开诊断</button></a></div>';
      top.insertAdjacentElement("afterend",box);
    }
  }catch(e){}

  try{
    const top=document.querySelector(".top");
    if(top && !document.getElementById("v64-shortcut")){
      const box=document.createElement("div");
      box.id="v64-shortcut";
      box.className="card";
      box.style.cssText="margin:10px 0;border:2px solid #fde68a";
      box.innerHTML='<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap"><div><h2 style="margin:0">🧪 V6.4 Veto Validation</h2><div class="muted">不重跑，直接比较过滤前后</div></div><a href="/v64/veto-validation"><button type="button">打开诊断</button></a></div>';
      top.insertAdjacentElement("afterend",box);
    }
  }catch(e){}

  try{
    // 1) Move existing publisher card to the very top, directly under page header.
    const pub = document.getElementById("publisher-top");
    const top = document.querySelector(".top");
    if(pub && top){
      top.insertAdjacentElement("afterend", pub);
      pub.style.marginTop = "10px";
    }

    // 2) Collapse the long V6.3 card without changing Jinja/HTML server structure.
    const v63 = document.getElementById("v63-current");
    if(v63 && !document.getElementById("v63-safe-toggle")){
      const kids = Array.from(v63.children);
      const keep = kids.length ? kids[0] : null; // keep title
      const btn = document.createElement("button");
      btn.id = "v63-safe-toggle";
      btn.type = "button";
      btn.textContent = "▶ 展开 V6.3 完整结果";
      btn.style.margin = "10px 0";
      btn.onclick = function(){
        const collapsed = btn.dataset.open !== "1";
        kids.forEach((el,i)=>{ if(i>0 && el!==btn) el.style.display = collapsed ? "" : "none"; });
        btn.dataset.open = collapsed ? "1" : "0";
        btn.textContent = collapsed ? "▼ 收起 V6.3 完整结果" : "▶ 展开 V6.3 完整结果";
      };
      if(keep) keep.insertAdjacentElement("afterend", btn);
      else v63.prepend(btn);
      kids.forEach((el,i)=>{ if(i>0) el.style.display="none"; });
      btn.dataset.open="0";
    }

    // 3) Floating shortcut always visible on mobile/desktop.
    if(!document.getElementById("publisher-fab-safe")){
      const a=document.createElement("a");
      a.id="publisher-fab-safe";
      a.href="/publisher/control";
      a.textContent="☁️ 发布";
      a.style.cssText="position:fixed;right:14px;bottom:84px;z-index:9999;text-decoration:none;background:#111827;color:white;padding:12px 16px;border-radius:999px;font-weight:900;box-shadow:0 6px 24px rgba(0,0,0,.18)";
      document.body.appendChild(a);
    }

    // 4) Fix quick-nav current version target.
    const currentLink = document.querySelector('.quick-nav a.primary');
    if(currentLink){
      currentLink.href="#v63-current";
      currentLink.textContent="🚀 V6.5.0 当前版";
    }
  }catch(e){
    console.error("safe UI init failed", e);
  }
});
</script>
</body></html>"""


@APP.get("/youtube")
def youtube_home():
    return render_template_string(
        YOUTUBE_HTML, result=None, error=None, url="", history=youtube_ingest_list()
    )

@APP.post("/youtube/ingest")
def youtube_ingest_route():
    from flask import request
    url = (request.form.get("url") or "").strip()
    try:
        result = youtube_ingest(url)
        return render_template_string(
            YOUTUBE_HTML, result=result, error=None, url=url, history=youtube_ingest_list()
        )
    except Exception as e:
        return render_template_string(
            YOUTUBE_HTML, result=None, error=f"{type(e).__name__}: {e}",
            url=url, history=youtube_ingest_list()
        ), 400


@APP.post("/api/youtube/upload")
def youtube_api_upload():
    from flask import request
    if not YOUTUBE_UPLOAD_TOKEN:
        return jsonify({"ok": False, "error": "YOUTUBE_UPLOAD_TOKEN_not_configured"}), 503

    supplied = (request.headers.get("X-Upload-Token") or "").strip()
    if not supplied or not hmac.compare_digest(supplied, YOUTUBE_UPLOAD_TOKEN):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    try:
        payload = request.get_json(force=True, silent=False) or {}
        result = save_uploaded_youtube_transcript(payload)
        return jsonify({
            "ok": True,
            "video_id": result["video_id"],
            "char_count": result["char_count"],
            "language": result["language"],
            "source_url": result["source_url"],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 400

@APP.get("/api/youtube/transcript")
def youtube_api_transcript():
    from flask import request
    value = (request.args.get("url") or request.args.get("video_id") or "").strip()
    video_id = youtube_video_id(value)
    if not video_id:
        return jsonify({"ok": False, "error": "invalid_youtube_url_or_id"}), 400
    try:
        result = youtube_ingest(value or video_id)
        # Keep API compact by default; full transcript is enough for ChatGPT ingestion.
        return jsonify({
            "ok": True,
            "video_id": result["video_id"],
            "source_url": result["source_url"],
            "language": result["language"],
            "language_code": result["language_code"],
            "is_generated": result["is_generated"],
            "fetched_at_utc": result["fetched_at_utc"],
            "char_count": result["char_count"],
            "transcript": result["transcript"],
            "cache_hit": result.get("cache_hit", False),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 502

@APP.get("/youtube/text/<video_id>")
def youtube_text(video_id):
    result = youtube_cached(video_id)
    if not result:
        return "Not found", 404
    return (
        f"YouTube: {result.get('source_url','')}\n"
        f"Language: {result.get('language','')} ({result.get('language_code','')})\n"
        f"Generated captions: {result.get('is_generated',False)}\n\n"
        + result.get("transcript",""),
        200, {"Content-Type": "text/plain; charset=utf-8"}
    )

@APP.get("/youtube/download/<video_id>")
def youtube_download(video_id):
    jp, tp = _yt_paths(video_id)
    if not tp.exists():
        return "Not found", 404
    return send_file(tp, mimetype="text/plain", as_attachment=True,
                     download_name=f"youtube_{video_id}_transcript.txt")


@APP.get("/api/youtube/index")
def youtube_public_index():
    """Public, read-only transcript catalog for ChatGPT and other readers."""
    rows = youtube_ingest_list()
    items = []
    for r in rows:
        vid = str(r.get("video_id") or "").strip()
        if not vid:
            continue
        items.append({
            "video_id": vid,
            "source_url": r.get("source_url") or f"https://www.youtube.com/watch?v={vid}",
            "language": r.get("language") or "",
            "language_code": r.get("language_code") or "",
            "is_generated": bool(r.get("is_generated", False)),
            "fetched_at_utc": r.get("fetched_at_utc") or "",
            "char_count": int(r.get("char_count") or 0),
            "transcript_url": f"{request.url_root.rstrip('/')}/youtube/text/{vid}",
            "api_url": f"{request.url_root.rstrip('/')}/api/youtube/transcript?video_id={vid}",
        })
    return jsonify({
        "ok": True,
        "count": len(items),
        "items": items,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "read_only": True,
    })

@APP.get("/api/youtube/library")
def youtube_public_library_alias():
    """Human-friendly alias of the public read-only index."""
    return youtube_public_index()

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
        v3_processed=read_v3_status().get("processed",0),v3_mode=read_v3_status().get("mode","full"),v3_results=v3_rows(),v3_diag=v3_diagnostics(),v3_trades=v3_trade_details(),v3_signals=v3_signal_details(),v3_shadow=v3_shadow_results(),v3_shadow_trades=v3_shadow_trade_details(),v4_sweep=v4_weight_sweep(),v4_best=v4_best_by_size(),v41_rows=v41_sweet_spot_sweep(),v41_best=v41_best_rows(),v41_regime=v41_regime_check(),v42_regime=v42_regime_summary(),v42_regime_best=v42_regime_best(),v42_ablation=v42_ablation_best(),v43_rows=v43_adaptive_sweep(),v43_years=v43_year_stability(),v43_policy=v43_factor_policy(),v44_walk=v44_walk_forward(),v44_bands=v44_score_band_check(),v44_recent=v44_recent_validation(),v45_cc=v45_champion_challenger(),v45_wf=v45_walkforward_verdict(),v45_decision=v45_research_decision(),v46_engine=v46_engine_summary(),v461_diag=v461_diag(),v462_probe=v462_deploy_probe(),v47_sweep=v47_true_engine_threshold_sweep(),v47_periods=v47_true_engine_periods(),v47_plateau=v47_threshold_plateau(),v48_tiers=v48_dual_tier_summary(),v48_bins=v48_score_bins(),v50_status=v50_expanded_universe_status(),v50_results=v50_validation_results(),v5v_status=read_v5_validation_status(),v5v_summary=v5_validation_summary(),v5_autopsy=v5_oos_autopsy(),v6_status=read_v6_shadow_status(),v6_summary=v6_shadow_summary(),v6_characters=v6_shadow_character_rows(),v61_lab=v61_decision_lab(),v61_status=read_v61_fast_status(),v61_quick=v61_fast_summary("quick50"),v61_full=v61_fast_summary("full"),v62_status=v62_cache_status(),v62_lab=v62_instant_lab(),v63_status=read_v63_status(),v63_summary=v63_summary(),
        **s)






@APP.post("/v62/build-cache")
def v62_build_cache_route():
    build_v62_research_cache()
    return redirect(url_for("home"))



@APP.get("/v63/context-veto")
def v63_context_veto_page():
    s=v63_summary()
    rows=s.get("context_veto",[]) if s.get("available") else []
    recent=s.get("context_veto_recent",[]) if s.get("available") else []
    groups=["TIER","EDGE","SUPPLY","ENTRY","COMBO"]
    tpl=r"""
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>V6.3.9.3 Context Veto</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f4f7fb;color:#111827;margin:0;padding:16px}
.wrap{max-width:900px;margin:auto}.card{background:#fff;border-radius:18px;padding:18px;margin:14px 0;box-shadow:0 1px 3px rgba(0,0,0,.06)}
h1{font-size:26px;margin:0 0 8px}h2{font-size:21px;margin:18px 0 10px}
.grid{display:grid;grid-template-columns:1fr;gap:10px}.item{border:1px solid #e5e7eb;border-radius:14px;padding:14px}
.name{font-size:19px;font-weight:800;margin-bottom:8px}.metrics{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.metric{background:#f8fafc;border-radius:10px;padding:10px}.metric small{display:block;color:#64748b}.metric b{font-size:20px}
.back{display:inline-block;padding:10px 14px;background:#111827;color:#fff;border-radius:10px;text-decoration:none;font-weight:800}
@media(min-width:700px){.grid{grid-template-columns:1fr 1fr}}
@media print{body{background:#fff;padding:0}.card{box-shadow:none;border:1px solid #ddd;break-inside:avoid}.back{display:none}}
</style>
</head>
<body><div class="wrap">
<a class="back" href="/">← 返回主仪表盘</a>
<div class="card">
<h1>🧱 V6.3.9.3 Context Veto Diagnostic</h1>
<p>独立页面，避免主仪表盘过长导致手机/PDF截断。全部直接读取现有 V6.3 trades，不重跑。</p>
</div>

{% for group in groups %}
<div class="card">
<h2>{{group}}</h2>
<div class="grid">
{% for r in rows if r.group==group %}
<div class="item">
<div class="name">{{r.name}}</div>
<div class="metrics">
<div class="metric"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</div>
{% endfor %}

<div class="card">
<h2>2022–2026 · Context Veto</h2>
<div class="grid">
{% for r in recent %}
<div class="item">
<div class="name">{{r.group}} · {{r.name}}</div>
<div class="metrics">
<div class="metric"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div>
</div>
{% endfor %}
</div>
</div>
</div></body></html>
"""
    return render_template_string(tpl,rows=rows,recent=recent,groups=groups)



@APP.get("/v65/ranking-diagnostic")
def v65_ranking_diagnostic_page():
    s=v63_summary()
    rank_rows=s.get("ranking_diag",[]) if s.get("available") else []
    opp_rows=s.get("opportunity_diag",[]) if s.get("available") else []
    rank_groups=["ALL","POWER_TREND","BULL_TREND","VALIDATION_324","2022–2026"]
    tpl=r"""
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>V6.5 Ranking Diagnostic</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f4f7fb;color:#111827;margin:0;padding:16px}
.wrap{max-width:900px;margin:auto}.card{background:white;border-radius:18px;padding:18px;margin:14px 0;box-shadow:0 1px 4px rgba(0,0,0,.06)}
h1{font-size:28px;margin:0 0 8px}h2{font-size:22px}.grid{display:grid;grid-template-columns:1fr;gap:10px}
.item{border:1px solid #e5e7eb;border-radius:14px;padding:14px}.name{font-weight:900;font-size:18px}
.metrics{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}.metric{background:#f8fafc;border-radius:10px;padding:10px}
.metric small{display:block;color:#64748b}.metric b{font-size:20px}.back{display:inline-block;background:#111827;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:800}
.note{background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;padding:12px}
@media(min-width:700px){.grid{grid-template-columns:1fr 1fr}}
</style></head>
<body><div class="wrap">
<a class="back" href="/">← 返回主仪表盘</a>
<div class="card">
<h1>🎯 V6.5 Opportunity Ranking Diagnostic</h1>
<div class="note">不重跑。先确认现有 Rank 1/2/3 是否真的有排序能力；再看 POWER_TREND 下哪些“机会质量特征”值得进入下一版 Ranking，而不是继续加硬门槛。</div>
</div>

{% for group in rank_groups %}
<div class="card">
<h2>{{group}} · Rank表现</h2>
<div class="grid">
{% for r in rank_rows if r.group==group %}
<div class="item"><div class="name">{{r.name}}</div><div class="metrics">
<div class="metric"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div></div>
{% endfor %}
</div>
</div>
{% endfor %}

<div class="card">
<h2>POWER_TREND · Opportunity Quality</h2>
<div class="grid">
{% for r in opp_rows %}
<div class="item"><div class="name">{{r.name}}</div><div class="metrics">
<div class="metric"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div></div>
{% endfor %}
</div>
</div>
</div></body></html>
"""
    return render_template_string(tpl,rank_rows=rank_rows,opp_rows=opp_rows,rank_groups=rank_groups)

@APP.get("/v64/veto-validation")
def v64_veto_validation_page():
    s=v63_summary()
    rows=s.get("veto_validation",[]) if s.get("available") else []
    scopes=["ALL_466","ORIGINAL_122","VALIDATION_324","2007–2021","2022–2026"]
    tpl=r"""
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>V6.4 Veto Validation</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f4f7fb;color:#111827;margin:0;padding:16px}
.wrap{max-width:900px;margin:auto}.card{background:white;border-radius:18px;padding:18px;margin:14px 0;box-shadow:0 1px 4px rgba(0,0,0,.06)}
h1{font-size:28px;margin:0 0 8px}h2{font-size:22px}.grid{display:grid;grid-template-columns:1fr;gap:10px}
.item{border:1px solid #e5e7eb;border-radius:14px;padding:14px}.name{font-weight:900;font-size:18px}
.metrics{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}.metric{background:#f8fafc;border-radius:10px;padding:10px}
.metric small{display:block;color:#64748b}.metric b{font-size:20px}.back{display:inline-block;background:#111827;color:#fff;padding:10px 14px;border-radius:10px;text-decoration:none;font-weight:800}
.note{background:#fff7ed;border:1px solid #fed7aa;border-radius:12px;padding:12px}
@media(min-width:700px){.grid{grid-template-columns:1fr 1fr}}
</style></head>
<body><div class="wrap">
<a class="back" href="/">← 返回主仪表盘</a>
<div class="card">
<h1>🧪 V6.4 Candidate Veto Validation</h1>
<div class="note">不重跑。直接用现有 V6.3 trades 比较过滤前后。重点看：是否在 ALL、原122、新验证组、早期、最近都一致改善。</div>
</div>

{% for scope in scopes %}
<div class="card">
<h2>{{scope}}</h2>
<div class="grid">
{% for r in rows if r.scope==scope %}
<div class="item">
<div class="name">{{r.name}}</div>
<div class="metrics">
<div class="metric"><small>PF</small><b>{{"%.2f"|format(r.pf)}}</b></div>
<div class="metric"><small>平均收益</small><b>{{"%.2f%%"|format(r.avg_return*100)}}</b></div>
<div class="metric"><small>胜率</small><b>{{"%.1f%%"|format(r.win_rate*100)}}</b></div>
<div class="metric"><small>交易数 / 最大连亏</small><b>{{r.trades}} / {{r.max_losing_streak}}</b></div>
</div></div>
{% endfor %}
</div>
</div>
{% endfor %}
</div></body></html>
"""
    return render_template_string(tpl,rows=rows,scopes=scopes)

@APP.post("/v63/run")
def v63_run_route():
    start_v63()
    return redirect(url_for("home"))

@APP.post("/v63/stop")
def v63_stop_route():
    stop_v63()
    return redirect(url_for("home"))

@APP.get("/api/v63/status")
def api_v63_status():
    return jsonify(read_v63_status())

@APP.post("/v61/quick50")
def v61_quick50_route():
    start_v61_fast("quick50")
    return redirect(url_for("home"))

@APP.post("/v61/full")
def v61_full_route():
    start_v61_fast("full")
    return redirect(url_for("home"))

@APP.post("/v61/stop")
def v61_stop_route():
    stop_v61_fast()
    return redirect(url_for("home"))

@APP.get("/api/v61-fast-status")
def api_v61_fast_status():
    return jsonify(read_v61_fast_status())

@APP.post("/v6/shadow/start")
def v6_shadow_start_route():
    start_v6_shadow()
    return redirect(url_for("home"))

@APP.post("/v6/shadow/stop")
def v6_shadow_stop_route():
    stop_v6_shadow()
    return redirect(url_for("home"))

@APP.get("/api/v6-shadow-status")
def api_v6_shadow_status():
    return jsonify(read_v6_shadow_status())

@APP.get("/v5/autopsy.csv")
def v5_autopsy_csv_route():
    p=export_v5_oos_autopsy_csv()
    if p is None or not p.exists():
        return "OOS autopsy unavailable",404
    return send_file(p,mimetype="text/csv",as_attachment=True,download_name="v5_oos_autopsy.csv")

@APP.post("/v5/validation/start")
def v5_validation_start_route():
    start_v5_validation()
    return redirect(url_for("home"))

@APP.post("/v5/validation/stop")
def v5_validation_stop_route():
    stop_v5_validation()
    return redirect(url_for("home"))

@APP.get("/api/v5-validation-status")
def api_v5_validation_status():
    return jsonify(read_v5_validation_status())

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



@APP.post("/publisher/test")
def publisher_test():
    """
    Safe end-to-end test:
    writes a harmless jlaw_publisher_probe.py file to GitHub.
    This should trigger the repo-linked Railway auto-deploy,
    without touching app.py or the trading engine.
    """
    try:
        stamp = datetime.now(timezone.utc).isoformat()
        probe = (
            "# VCP Radar Cloud Publisher probe\n"
            f"# Generated at: {stamp}\n"
            "PUBLISHER_PROBE_OK = True\n"
        ).encode("utf-8")
        result = _gh_publish(
            "jlaw_publisher_probe.py",
            probe,
            message=f"Publisher probe {stamp}"
        )
        return jsonify({"ok": True, "message": "publisher test commit created", **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@APP.get("/publisher/control")
def publisher_control():
    return """
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>VCP Cloud Publisher</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f4f7fb;color:#111827;margin:0;padding:16px}
.wrap{max-width:720px;margin:auto}
.card{background:#fff;border-radius:18px;padding:18px;margin:14px 0;box-shadow:0 2px 10px rgba(0,0,0,.06)}
h1{font-size:26px;margin:0 0 8px}label{font-weight:800;display:block;margin:14px 0 6px}
input[type=file],input[type=password],input[type=text]{width:100%;box-sizing:border-box;font-size:16px;padding:12px;border:1px solid #d1d5db;border-radius:12px;background:white}
button{width:100%;font-size:18px;font-weight:900;padding:14px;border:0;border-radius:12px;background:#111827;color:white;margin-top:14px}
.small{font-size:14px;color:#64748b}.ok{color:#166534}.bad{color:#991b1b}
pre{white-space:pre-wrap;word-break:break-word;background:#f8fafc;padding:12px;border-radius:10px;min-height:80px}
.warn{background:#fff7ed;border:1px solid #fed7aa;padding:12px;border-radius:12px}
</style>
</head>
<body><div class="wrap">
<div class="card">
<h1>☁️ VCP Cloud Publisher</h1>
<p>以后不用去 GitHub 改名上传。这里选我给你的 Python 文件，系统会自动发布到 GitHub，Railway 再自动 Deploy。</p>
<div class="warn">
<b>安全：</b>只允许 <code>app.py / app_v*.py / app-*.py / jlaw_*.py</code>。<br>
发布前会检查 UTF-8 和 Python 语法。
</div>

<form id="f">
<label>1. 选择新版 Python 文件</label>
<input id="file" name="file" type="file" accept=".py,text/x-python" required>

<label>2. Publisher 密码</label>
<input id="secret" name="secret" type="password" autocomplete="current-password" placeholder="Railway 里设置的 PUBLISH_SECRET" required>
<label style="font-weight:600;margin-top:8px;display:flex;align-items:center;gap:8px">
<input id="remember" type="checkbox" checked style="width:auto"> 在这台手机记住密码
</label>

<label>3. Commit 备注（可留空）</label>
<input id="message" name="message" type="text" placeholder="例如：V6.4 Context Veto">

<button type="submit">🚀 发布到 GitHub + 触发 Railway</button>
</form>

<p class="small">app_v*.py 会自动作为 <b>app.py</b> 发布；jlaw_*.py 保持原文件名。</p>
<p class="small"><b>建议：</b>把这个页面加到 iPhone 主屏幕，以后点图标就直接上传，不必先打开仪表盘。</p>
<pre id="out">等待发布…</pre>
</div>
</div>

<script>
const secretInput=document.getElementById("secret");
const rememberBox=document.getElementById("remember");
try{
  const saved=localStorage.getItem("vcp_publish_secret")||"";
  if(saved){ secretInput.value=saved; rememberBox.checked=true; }
}catch(e){}

document.getElementById("f").addEventListener("submit", async (e)=>{
  e.preventDefault();
  const out=document.getElementById("out");
  const file=document.getElementById("file").files[0];
  const secret=document.getElementById("secret").value;
  const message=document.getElementById("message").value;
  try{
    if(rememberBox.checked){ localStorage.setItem("vcp_publish_secret",secret); }
    else { localStorage.removeItem("vcp_publish_secret"); }
  }catch(e){}
  if(!file){ out.textContent="请选择 .py 文件"; return; }

  const fd=new FormData();
  fd.append("file",file);
  fd.append("message",message);

  out.className="";
  out.textContent="正在检查并提交 GitHub…";

  try{
    const r=await fetch("/publisher/upload",{
      method:"POST",
      headers:{"X-Publish-Secret":secret},
      body:fd
    });
    const j=await r.json();
    out.className=j.ok?"ok":"bad";
    out.textContent=JSON.stringify(j,null,2);
  }catch(err){
    out.className="bad";
    out.textContent=String(err);
  }
});
</script>
</body></html>
"""

@APP.post("/publisher/upload")
def publisher_upload():
    if not _publisher_auth_ok(request):
        return jsonify({"ok":False,"error":"Publisher 密码错误"}), 401

    if "file" not in request.files:
        return jsonify({"ok":False,"error":"没有收到文件"}), 400

    f=request.files["file"]
    original=(f.filename or "").strip()
    if not original:
        return jsonify({"ok":False,"error":"文件名为空"}), 400

    # Safe target mapping.
    if original == "app.py" or original.startswith("app_v") or original.startswith("app-"):
        target="app.py"
    elif original.startswith("jlaw_") and original.endswith(".py"):
        target=original
    else:
        return jsonify({
            "ok":False,
            "error":"只允许 app.py / app_v*.py / app-*.py / jlaw_*.py"
        }), 400

    data=f.read()
    if len(data) > PUBLISH_MAX_BYTES:
        return jsonify({"ok":False,"error":f"文件过大：{len(data)} bytes"}), 400

    try:
        source=data.decode("utf-8")
    except Exception:
        return jsonify({"ok":False,"error":"文件必须是 UTF-8 Python 文本"}), 400

    # Syntax-check before publishing.
    try:
        compile(source, original, "exec")
    except SyntaxError as e:
        return jsonify({
            "ok":False,
            "error":f"Python 语法错误：line {e.lineno}: {e.msg}"
        }), 400

    msg=(request.form.get("message") or "").strip()
    if not msg:
        msg=f"Cloud publish {target} via VCP Publisher"

    try:
        result=_gh_publish(target,data,msg)
        return jsonify({
            "ok":True,
            "message":"GitHub 发布成功；Railway 应自动开始 Deploy",
            "original_file":original,
            "target":target,
            **result
        })
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}), 500

@APP.get("/publisher/test-page")
def publisher_test_page():
    return """
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Publisher Test</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f6f8fb;color:#111827;padding:18px}
.card{max-width:680px;margin:30px auto;background:white;border-radius:18px;padding:20px;box-shadow:0 2px 10px rgba(0,0,0,.08)}
button{font-size:18px;font-weight:800;padding:14px 18px;border:0;border-radius:12px;background:#111827;color:white;width:100%}
pre{white-space:pre-wrap;word-break:break-word;background:#f3f4f6;padding:12px;border-radius:10px}
.ok{color:#166534}.bad{color:#991b1b}
</style>
</head>
<body>
<div class="card">
<h2>☁️ Cloud Publisher 测试</h2>
<p>这个测试只会在 GitHub 新建/更新 <b>jlaw_publisher_probe.py</b>，不会改 app.py，也不会改模型。</p>
<button onclick="runTest()">▶ 开始安全测试</button>
<pre id="out">等待测试…</pre>
</div>
<script>
async function runTest(){
  const out=document.getElementById('out');
  out.textContent='正在提交 GitHub…';
  try{
    const r=await fetch('/publisher/test',{method:'POST'});
    const j=await r.json();
    out.className=j.ok?'ok':'bad';
    out.textContent=JSON.stringify(j,null,2);
  }catch(e){
    out.className='bad';
    out.textContent=String(e);
  }
}
</script>
</body>
</html>
"""

@APP.get("/publisher/health")
def publisher_health():
    return jsonify({
        "ok": True,
        "publisher": "embedded",
        "repo": f"{GITHUB_OWNER}/{GITHUB_REPO}",
        "branch": GITHUB_BRANCH,
        "token_configured": bool(GITHUB_TOKEN),
        "secret_configured": bool(PUBLISH_SECRET),
    })

@APP.post("/publisher/publish")
def publisher_publish():
    if not _publisher_auth_ok(request):
        return jsonify({"ok":False,"error":"unauthorized"}), 401

    body = request.get_json(silent=True) or {}
    target = body.get("target")
    content = body.get("content")
    message = body.get("message")

    if target != "app.py" and not (isinstance(target,str) and target.startswith("jlaw_") and target.endswith(".py")):
        return jsonify({"ok":False,"error":"target must be app.py or jlaw_*.py"}), 400
    if not isinstance(content, str):
        return jsonify({"ok":False,"error":"content must be UTF-8 text"}), 400

    try:
        result = _gh_publish(target, content.encode("utf-8"), message)
        return jsonify({"ok":True, **result})
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}), 500

@APP.get("/health")
def health():
    info=storage_info()
    return jsonify({"ok":True,"version":"v5.1-free-tier-auto-resume","data_dir":info["data_dir"],"writable":info["writable"]})

recover_stale_v3_status()
threading.Thread(target=scheduler_loop, daemon=True).start()
