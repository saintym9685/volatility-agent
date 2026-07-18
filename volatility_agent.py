"""
코인 변동성 스크리너 에이전트 v6 (24시간 상시 봇)

v6 변경사항:
- 스캔 기준을 'MEXC 선물(무기한) 전체'로 변경 (바이낸스 상장 여부와 무관)
- 각 코인이 바이낸스 선물(🅱) / OKX 선물(🅾)에서 거래 가능한지 표시
- 차트 데이터 소스에 MEXC 선물 추가 (MEXC 선물 전용 코인도 차트 지원)

실행 모드:
  python volatility_agent.py scan    → 순위 스캔 후 리스트 1회 발송 (아침 알림용)
  python volatility_agent.py listen  → 상시 대기 모드 (텔레그램 명령 응답)

텔레그램 명령어:
  /show        최신 변동성 순위 리스트
  /1 ~ /10     7일 변동성 순위 코인 차트
  /m1 ~ /m10   30일 변동성 순위 코인 차트
  /q1 ~ /q10   3개월 변동성 순위 코인 차트
  /심볼        차트 직접 요청 (바이낸스 현물→선물→MEXC 현물→MEXC 선물 순으로 검색)
  /심볼 4h     시간봉 지정 (5m 15m 30m 1h 4h 1d 1w)
  /help        명령어 안내
"""

import os
import io
import re
import sys
import math
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # 서버 환경(화면 없음)용 설정
import mplfinance as mpf

# ============ 설정 ============
TOP_N = 10                        # 순위당 코인 개수
MEXC_MIN_VOLUME = 1_000_000       # MEXC 선물 24h 거래대금(USDT) 필터 — 낮추면 더 많은 코인 스캔
SCAN_DAYS = 91                    # 스캔용 일봉 개수 (3개월 계산에 필요)
CHART_DAYS = 60                   # 차트에 표시할 봉 개수
CACHE_MINUTES = 15                # /show 시 이 시간 내 캐시가 있으면 재사용
RUN_MINUTES = int(os.environ.get("RUN_MINUTES", "350"))  # listen 모드 1회 근무 시간
STALE_MESSAGE_MINUTES = 30        # 이보다 오래된 메시지는 무시
SCAN_WORKERS = 6                  # 동시 요청 수 (MEXC 레이트리밋 고려)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

BINANCE_API = "https://data-api.binance.vision"
MEXC_CONTRACT_API = "https://contract.mexc.com/api/v1/contract"

# 지원 시간봉
VALID_INTERVALS = ("5m", "15m", "30m", "1h", "4h", "1d", "1w")
MEXC_SPOT_INTERVAL_MAP = {"1h": "60m", "1w": "1W"}
MEXC_FUT_INTERVAL_MAP = {
    "5m": "Min5", "15m": "Min15", "30m": "Min30",
    "1h": "Min60", "4h": "Hour4", "1d": "Day1", "1w": "Week1",
}
INTERVAL_SECONDS = {
    "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400, "1w": 604800,
}

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
KST = timezone(timedelta(hours=9))

HELP_TEXT = (
    "🤖 <b>변동성 스크리너 명령어</b>\n\n"
    "/show — 최신 변동성 순위 리스트\n"
    f"/1 ~ /{TOP_N} — 7일 순위 코인 차트\n"
    f"/m1 ~ /m{TOP_N} — 30일 순위 코인 차트\n"
    f"/q1 ~ /q{TOP_N} — 3개월 순위 코인 차트\n"
    "/심볼 — 차트 직접 요청 (예: /eth, /lab)\n"
    "     바이낸스→MEXC 현물→MEXC 선물 순 자동 검색\n"
    "뒤에 시간봉을 붙이면 해당 봉 차트 (기본: 일봉)\n"
    "  예: /eth 4h · /1 1h · /q3 15m\n"
    "  지원: 5m 15m 30m 1h 4h 1d 1w\n"
    "/help — 이 안내 보기\n\n"
    "📌 순위는 <b>MEXC 선물 전체</b>를 스캔한 결과입니다.\n"
    "🅱 = 바이낸스 선물 거래 가능 · 🅾 = OKX 선물 거래 가능\n"
    "매일 아침 7:30(KST)에 순위가 자동 발송됩니다."
)


# ============ MEXC 선물 데이터 수집 ============
def get_mexc_futures_universe():
    """MEXC 선물 전체 심볼 → 24h 거래대금(USDT) 딕셔너리. 심볼 형식: BTC_USDT"""
    r = requests.get(f"{MEXC_CONTRACT_API}/ticker", timeout=20)
    r.raise_for_status()
    data = r.json().get("data", []) or []
    return {
        t["symbol"]: float(t.get("amount24", 0) or 0)
        for t in data
        if t.get("symbol", "").endswith("_USDT")
    }


def get_mexc_futures_klines(symbol_u, interval="1d", limit=SCAN_DAYS):
    """MEXC 선물 봉데이터. symbol_u는 언더스코어 형식(BTC_USDT).
    반환: [시간ms, 시가, 고가, 저가, 종가, 거래량] 리스트 (다른 소스와 동일 형식)"""
    iv = MEXC_FUT_INTERVAL_MAP[interval]
    end = int(time.time())
    start = end - (limit + 2) * INTERVAL_SECONDS[interval]
    r = requests.get(
        f"{MEXC_CONTRACT_API}/kline/{symbol_u}",
        params={"interval": iv, "start": start, "end": end},
        timeout=15,
    )
    r.raise_for_status()
    d = r.json().get("data") or {}
    times = d.get("time") or []
    if not times:
        return []
    rows = [
        [times[i] * 1000, d["open"][i], d["high"][i], d["low"][i], d["close"][i], d["vol"][i]]
        for i in range(len(times))
    ]
    return rows[-limit:]


def calc_volatility(klines, period):
    """데이터가 period일보다 적으면 None 반환 (신규 상장 코인)"""
    k = klines[-period:]
    if len(k) < period:
        return None
    closes = [float(c[4]) for c in k]
    highs = [float(c[2]) for c in k]
    lows = [float(c[3]) for c in k]
    returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes))]
    mean = sum(returns) / len(returns)
    std_vol = math.sqrt(sum((r - mean) ** 2 for r in returns) / len(returns)) * 100
    return {
        "std_vol": std_vol,
        "range_pct": (max(highs) - min(lows)) / min(lows) * 100,
        "change_pct": (closes[-1] - closes[0]) / closes[0] * 100,
    }


# ============ 바이낸스 / OKX 선물 상장 여부 ============
_futures_symbols = None
_futures_checked = False


def get_binance_futures_symbols():
    """바이낸스 선물(무기한) 상장 심볼 전체 목록. fapi 차단 시 None"""
    global _futures_symbols, _futures_checked
    if _futures_checked:
        return _futures_symbols
    _futures_checked = True
    try:
        r = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=15)
        if r.status_code == 200:
            _futures_symbols = {
                s["symbol"]
                for s in r.json().get("symbols", [])
                if s.get("status") == "TRADING"
                and s.get("contractType") == "PERPETUAL"
            }
            print(f"바이낸스 선물 목록 확보: {len(_futures_symbols)}개 (fapi)")
    except Exception as e:
        print(f"fapi 접속 불가 → 데이터 저장소 방식으로 확인: {e}")
    return _futures_symbols


def is_binance_futures_listed(symbol):
    """바이낸스 선물(무기한) 상장·거래 중인지 확인. symbol 형식: BTCUSDT"""
    fs = get_binance_futures_symbols()
    if fs is not None:
        return symbol in fs
    # fapi 차단 시 폴백: 공개 데이터 저장소에 최근 일봉 파일이 있는지 확인
    for d in (1, 2):
        date = (datetime.now(timezone.utc) - timedelta(days=d)).strftime("%Y-%m-%d")
        url = (
            f"https://data.binance.vision/data/futures/um/daily/klines/"
            f"{symbol}/1d/{symbol}-1d-{date}.zip"
        )
        try:
            if requests.head(url, timeout=10).status_code == 200:
                return True
        except Exception:
            pass
    return False


_okx_symbols = None


def get_okx_futures_symbols():
    """OKX USDT 무기한 스왑 상장 심볼 집합. 형식은 BTCUSDT로 변환해서 반환"""
    global _okx_symbols
    if _okx_symbols is not None:
        return _okx_symbols
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/public/instruments",
            params={"instType": "SWAP"},
            timeout=15,
        )
        r.raise_for_status()
        _okx_symbols = {
            i["instId"].replace("-USDT-SWAP", "") + "USDT"
            for i in r.json().get("data", [])
            if i.get("instId", "").endswith("-USDT-SWAP") and i.get("state") == "live"
        }
        print(f"OKX 선물 목록 확보: {len(_okx_symbols)}개")
    except Exception as e:
        print(f"OKX 목록 조회 실패: {e}")
        _okx_symbols = set()
    return _okx_symbols


def annotate_exchanges(*lists):
    """각 순위 리스트 코인에 바이낸스(fut)·OKX(okx) 선물 가능 여부 표시"""
    okx = get_okx_futures_symbols()
    union = {r["symbol"] for lst in lists for r in lst if "fut" not in r}
    listed = {}
    if union:
        get_binance_futures_symbols()  # 목록 캐싱 (있으면 스레드 작업이 즉시 끝남)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(is_binance_futures_listed, s): s for s in union}
            for f in as_completed(futs):
                try:
                    listed[futs[f]] = f.result()
                except Exception:
                    listed[futs[f]] = False
    for lst in lists:
        for r in lst:
            if "fut" not in r:
                r["fut"] = listed.get(r["symbol"], False)
            r["okx"] = r["symbol"] in okx
    return lists


# ============ 스캔 (MEXC 선물 전체) ============
def _build_entry(sym, sym_u, klines):
    """일봉 → 순위 항목. 최신 봉이 3일 이상 오래됐으면 제외 (거래 중단 대비)"""
    if not klines:
        return None
    last_open_ms = float(klines[-1][0])
    if time.time() * 1000 - last_open_ms > 3 * 86400 * 1000:
        return None
    v7 = calc_volatility(klines, 7)
    if not v7:
        return None  # 일봉 7개 미만은 순위 제외
    return {
        "symbol": sym,       # BTCUSDT (표시·거래소 확인용)
        "symbol_u": sym_u,   # BTC_USDT (MEXC 선물 조회용)
        "v7": v7,
        "v30": calc_volatility(klines, 30),
        "v90": calc_volatility(klines, 90),
    }


def _scan_one(sym_u):
    for attempt in (1, 2):  # 레이트리밋 대비 1회 재시도
        try:
            klines = get_mexc_futures_klines(sym_u, "1d", SCAN_DAYS)
            return _build_entry(sym_u.replace("_", ""), sym_u, klines)
        except Exception:
            if attempt == 1:
                time.sleep(1.5)
            else:
                raise
    return None


def _top_by(results, key):
    pool = [r for r in results if r.get(key)]
    return sorted(pool, key=lambda x: x[key]["std_vol"], reverse=True)[:TOP_N]


def scan():
    universe = get_mexc_futures_universe()
    targets = [s for s, vol in universe.items() if vol >= MEXC_MIN_VOLUME]
    print(f"MEXC 선물 전체 {len(universe)}개 중 거래대금 필터 통과: {len(targets)}개")

    results = []
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as pool:
        futures = {pool.submit(_scan_one, s): s for s in targets}
        for i, fut in enumerate(as_completed(futures)):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception:
                pass
            if (i + 1) % 100 == 0:
                print(f"  진행: {i + 1}/{len(futures)}")

    top7 = _top_by(results, "v7")
    top30 = _top_by(results, "v30")
    top90 = _top_by(results, "v90")
    annotate_exchanges(top7, top30, top90)
    return top7, top30, top90


# ============ 차트용 데이터 소스 (순서대로 시도) ============
def _fetch_binance_spot(symbol, interval, limit):
    r = requests.get(
        f"{BINANCE_API}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=15,
    )
    return r.json() if r.status_code == 200 else None


def _fetch_binance_fut(symbol, interval, limit):
    r = requests.get(
        "https://fapi.binance.com/fapi/v1/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=15,
    )
    return r.json() if r.status_code == 200 else None


def _fetch_mexc_spot(symbol, interval, limit):
    iv = MEXC_SPOT_INTERVAL_MAP.get(interval, interval)
    r = requests.get(
        "https://api.mexc.com/api/v3/klines",
        params={"symbol": symbol, "interval": iv, "limit": limit},
        timeout=15,
    )
    return r.json() if r.status_code == 200 else None


def _fetch_mexc_fut(symbol, interval, limit):
    sym_u = symbol[:-4] + "_USDT" if symbol.endswith("USDT") else symbol
    return get_mexc_futures_klines(sym_u, interval, limit)


KLINE_SOURCES = [
    ("Binance Spot", _fetch_binance_spot),
    ("Binance Futures", _fetch_binance_fut),
    ("MEXC Spot", _fetch_mexc_spot),
    ("MEXC Futures", _fetch_mexc_fut),
]


def get_klines_any(symbol, interval="1d", limit=CHART_DAYS):
    """바이낸스 현물 → 선물 → MEXC 현물 → MEXC 선물 순으로 시도"""
    for source_name, fetch in KLINE_SOURCES:
        try:
            k = fetch(symbol, interval, limit)
            if isinstance(k, list) and len(k) >= 2:
                return k, source_name
        except Exception as e:
            print(f"  {source_name} 조회 실패({symbol} {interval}): {e}")
    raise LookupError(symbol)


# ============ 유사 심볼 검색 ============
def find_similar_symbols(query, limit=5):
    """바이낸스 현물·선물 + MEXC 현물·선물 전체에서 query가 포함된 심볼 검색"""
    found = set()
    rest_sources = [
        f"{BINANCE_API}/api/v3/exchangeInfo",
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "https://api.mexc.com/api/v3/exchangeInfo",
    ]
    for url in rest_sources:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code != 200:
                continue
            for s in r.json().get("symbols", []):
                sym = s.get("symbol", "")
                status = s.get("status", "")
                if not sym.endswith("USDT"):
                    continue
                if status not in ("TRADING", "1", "ENABLED", ""):
                    continue
                base = sym[:-4]
                if query in base:
                    found.add(base)
        except Exception as e:
            print(f"유사 심볼 검색 실패({url}): {e}")
    # MEXC 선물 유니버스에서도 검색
    try:
        for sym_u in get_mexc_futures_universe():
            base = sym_u.replace("_USDT", "")
            if query in base:
                found.add(base)
    except Exception as e:
        print(f"MEXC 선물 심볼 검색 실패: {e}")
    matches = sorted(found, key=len)
    return matches[:limit]


# ============ 차트 생성 ============
def _fmt_vol_line(label, v):
    if v is None:
        return f"{label}: 데이터 부족 (신규 상장)"
    return (
        f"{label}: 일변동 {v['std_vol']:.1f}% / "
        f"레인지 {v['range_pct']:.0f}% / {v['change_pct']:+.1f}%"
    )


def make_chart(symbol, interval="1d"):
    """캔들차트 → (PNG 버퍼, 캡션). 시간봉 차트는 한국시간 축으로 표시"""
    klines, source = get_klines_any(symbol, interval, limit=CHART_DAYS)

    # 소스마다 컬럼 수가 다르므로 앞 6개만 사용
    rows = [k[:6] for k in klines]
    df = pd.DataFrame(rows, columns=["time", "Open", "High", "Low", "Close", "Volume"])
    idx = pd.to_datetime(df["time"].astype(float), unit="ms", utc=True)
    df.index = idx.dt.tz_convert("Asia/Seoul").dt.tz_localize(None)  # KST 축
    df = df[["Open", "High", "Low", "Close", "Volume"]].astype(float)

    style = mpf.make_mpf_style(
        base_mpf_style="nightclouds",
        marketcolors=mpf.make_marketcolors(
            up="#26a69a", down="#ef5350", edge="inherit",
            wick="inherit", volume="in",
        ),
        gridstyle=":",
    )

    # 데이터 길이에 맞는 이동평균만 표시 (신규 상장 코인 대응)
    mavs = tuple(m for m in (7, 25) if len(df) > m)

    iv_label = interval.upper()
    plot_kwargs = dict(
        type="candle", volume=True, style=style,
        title=f"\n{symbol}  ({len(df)} x {iv_label} · {source} · KST)",
        figsize=(11, 7), tight_layout=True,
    )
    if mavs:
        plot_kwargs["mav"] = mavs

    buf = io.BytesIO()
    mpf.plot(df, **plot_kwargs, savefig=dict(fname=buf, dpi=110, format="png"))
    buf.seek(0)

    last_close = float(klines[-1][4])
    bn_mark = "✅ 가능" if is_binance_futures_listed(symbol) else "❌ 불가"
    okx_mark = "✅ 가능" if symbol in get_okx_futures_symbols() else "❌ 불가"

    if interval == "1d":
        stat_lines = (
            f"{_fmt_vol_line('7일', calc_volatility(klines, 7))}\n"
            f"{_fmt_vol_line('30일', calc_volatility(klines, 30))}"
        )
    else:
        closes = [float(c[4]) for c in klines]
        highs = [float(c[2]) for c in klines]
        lows = [float(c[3]) for c in klines]
        range_pct = (max(highs) - min(lows)) / min(lows) * 100
        change_pct = (closes[-1] - closes[0]) / closes[0] * 100
        stat_lines = (
            f"표시구간({len(klines)}개 {iv_label}봉): "
            f"레인지 {range_pct:.1f}% / {change_pct:+.1f}%"
        )

    caption = (
        f"📈 {symbol}  현재가 {last_close:g}  [{source} · {iv_label}]\n"
        f"{stat_lines}\n"
        f"선물: 바이낸스 {bn_mark} · OKX {okx_mark}"
    )
    return buf, caption


# ============ 텔레그램 ============
def send_message(text):
    if not TELEGRAM_TOKEN:
        print("⚠️ 텔레그램 설정 없음 — 콘솔 출력\n" + text)
        return
    r = requests.post(
        f"{TG_API}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )
    r.raise_for_status()


def send_photo(photo_buf, caption):
    r = requests.post(
        f"{TG_API}/sendPhoto",
        data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
        files={"photo": ("chart.png", photo_buf, "image/png")},
        timeout=30,
    )
    r.raise_for_status()


def _badges(r):
    b = ""
    if r.get("fut"):
        b += "🅱"
    if r.get("okx"):
        b += "🅾"
    return b


def _format_section(title, entries, vkey):
    lines = [f"📊 <b>{title}</b>"]
    for i, r in enumerate(entries, 1):
        name = r["symbol"].replace("USDT", "")
        v = r[vkey]
        arrow = "🟢" if v["change_pct"] >= 0 else "🔴"
        lines.append(
            f"{i}. <b>{name}</b>{_badges(r)} | 일변동 {v['std_vol']:.1f}% | "
            f"레인지 {v['range_pct']:.0f}% | {arrow} {v['change_pct']:+.1f}%"
        )
    return lines


def format_message(top7, top30, top90):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [f"🔥 <b>변동성 스크리너</b> — MEXC 선물 전체 ({now} KST)\n"]
    lines += _format_section(f"7일 변동성 TOP {TOP_N}", top7, "v7")
    lines.append("")
    lines += _format_section(f"30일 변동성 TOP {TOP_N}", top30, "v30")
    lines.append("")
    lines += _format_section(f"3개월 변동성 TOP {TOP_N}", top90, "v90")
    lines.append(
        f"\n🅱 = 바이낸스 선물 가능 · 🅾 = OKX 선물 가능\n"
        f"(배지 없음 = MEXC 선물에서만 거래 가능)\n"
        f"💬 /1~/{TOP_N} 7일 | /m1~/m{TOP_N} 30일 | /q1~/q{TOP_N} 3개월 | /help"
    )
    return "\n".join(lines)


# ============ 명령 처리 ============
class RankCache:
    def __init__(self):
        self.top7, self.top30, self.top90, self.ts = [], [], [], 0.0

    def is_fresh(self):
        return self.top7 and (time.time() - self.ts) < CACHE_MINUTES * 60

    def update(self, top7, top30, top90):
        self.top7, self.top30, self.top90 = top7, top30, top90
        self.ts = time.time()


def parse_command(token, cache):
    """'/1' → 7일 1위, '/m3' → 30일 3위, '/q2' → 3개월 2위, '/tlm' → TLMUSDT"""
    t = token.strip().lstrip("/").split("@")[0].lower()
    if not t:
        return None
    for prefix, lst in (("m", cache.top30), ("q", cache.top90)):
        if t.startswith(prefix) and t[1:].isdigit():
            idx = int(t[1:]) - 1
            return lst[idx]["symbol"] if 0 <= idx < len(lst) else None
    if t.isdigit():
        idx = int(t) - 1
        return cache.top7[idx]["symbol"] if 0 <= idx < len(cache.top7) else None
    return t.upper() + "USDT" if not t.upper().endswith("USDT") else t.upper()


def _is_rank_command(cmd):
    return (
        cmd.isdigit()
        or (cmd[:1] in ("m", "q") and cmd[1:].isdigit())
    )


def handle_command(text, cache):
    # "/lab 4h" 형태: 첫 토큰은 명령, 두 번째 토큰은 시간봉 (기본 1d)
    parts = text.strip().split()
    first = parts[0]
    interval = parts[1].lower() if len(parts) > 1 else "1d"
    cmd = first.lstrip("/").split("@")[0].lower()

    if cmd in ("help", "start"):
        send_message(HELP_TEXT)
        return

    if cmd == "show":
        if not cache.is_fresh():
            send_message("🔄 MEXC 선물 전체 스캔 중... (2~3분)")
            cache.update(*scan())
        send_message(format_message(cache.top7, cache.top30, cache.top90))
        return

    if interval not in VALID_INTERVALS:
        send_message(
            f"❓ 지원하지 않는 시간봉입니다: {interval}\n"
            f"사용 가능: {' '.join(VALID_INTERVALS)}"
        )
        return

    if _is_rank_command(cmd) and not cache.top7:
        send_message("🔄 순위 데이터가 없어 먼저 스캔합니다... (2~3분)")
        cache.update(*scan())

    symbol = parse_command(first, cache)
    if not symbol:
        send_message("❓ 알 수 없는 명령입니다. /help 를 입력해 보세요.")
        return

    try:
        buf, caption = make_chart(symbol, interval)
        send_photo(buf, caption)
    except LookupError:
        query = symbol.replace("USDT", "")
        similar = find_similar_symbols(query)
        if similar:
            suggestions = " ".join(f"/{s.lower()}" for s in similar)
            send_message(
                f"⚠️ '{query}' 코인을 바이낸스·MEXC 어디에서도 찾을 수 없습니다.\n"
                f"혹시 이 중에 있나요? 👉 {suggestions}"
            )
        else:
            send_message(f"⚠️ '{query}' 코인을 찾을 수 없고, 비슷한 이름도 없습니다.")
    except Exception as e:
        send_message(f"⚠️ {symbol} 차트 생성 실패: {e}")


# ============ 상시 대기 루프 ============
def listen_loop():
    cache = RankCache()
    cache.update(*scan())  # 시작하자마자 순위 준비
    print(f"🤖 봇 근무 시작 ({RUN_MINUTES}분)")

    deadline = time.time() + RUN_MINUTES * 60
    offset = None

    while time.time() < deadline:
        try:
            r = requests.get(
                f"{TG_API}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=35,
            ).json()
        except Exception as e:
            print(f"getUpdates 오류: {e}")
            time.sleep(5)
            continue

        if not r.get("ok"):
            print(f"응답 오류: {r.get('description', r)}")
            time.sleep(5)
            continue

        for upd in r.get("result", []):
            offset = upd["update_id"] + 1
            msg = upd.get("message", {})
            text = msg.get("text", "")
            chat_id = str(msg.get("chat", {}).get("id", ""))
            msg_time = msg.get("date", 0)

            if chat_id != str(TELEGRAM_CHAT_ID):
                continue
            if not text.startswith("/"):
                continue
            if time.time() - msg_time > STALE_MESSAGE_MINUTES * 60:
                print(f"오래된 메시지 무시: {text}")
                continue

            print(f"명령 수신: {text}")
            try:
                handle_command(text, cache)
            except Exception as e:
                print(f"명령 처리 오류: {e}")

    print("⏰ 근무 종료 — 다음 봇이 곧 교대합니다")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if mode == "listen":
        listen_loop()
    else:
        top7, top30, top90 = scan()
        send_message(format_message(top7, top30, top90))
