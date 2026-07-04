"""
코인 변동성 스크리너 에이전트 v4 (24시간 상시 봇)

실행 모드:
  python volatility_agent.py scan    → 순위 스캔 후 리스트 1회 발송 (아침 알림용)
  python volatility_agent.py listen  → 상시 대기 모드 (텔레그램 명령 응답)

텔레그램 명령어:
  /show        최신 변동성 순위 리스트
  /1 ~ /10     7일 변동성 순위 코인 차트
  /m1 ~ /m10   30일 변동성 순위 코인 차트
  /심볼        차트 직접 요청 (현물→선물→MEXC 순으로 검색)
  /help        명령어 안내
"""

import os
import io
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
TOP_N = 10                    # 상위 몇 개 코인을 보여줄지
MIN_VOLUME_USDT = 10_000_000  # 최소 24h 거래대금 필터 (순위 스캔용)
CHART_DAYS = 60               # 차트에 표시할 최대 일봉 개수
CACHE_MINUTES = 15            # /show 시 이 시간 내 캐시가 있으면 재사용
RUN_MINUTES = int(os.environ.get("RUN_MINUTES", "350"))  # listen 모드 1회 근무 시간
STALE_MESSAGE_MINUTES = 30    # 이보다 오래된 메시지는 무시

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# 순위 스캔용 기본 소스 (GitHub Actions에서 접속 가능한 바이낸스 현물 공개 데이터)
BINANCE_API = "https://data-api.binance.vision"

# 차트용 데이터 소스 (순서대로 시도)
KLINE_SOURCES = [
    ("Binance Spot", f"{BINANCE_API}/api/v3/klines"),
    ("Binance Futures", "https://fapi.binance.com/fapi/v1/klines"),
    ("MEXC", "https://api.mexc.com/api/v3/klines"),
]

TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
KST = timezone(timedelta(hours=9))

HELP_TEXT = (
    "🤖 <b>변동성 스크리너 명령어</b>\n\n"
    "/show — 최신 변동성 순위 리스트\n"
    f"/1 ~ /{TOP_N} — 7일 순위 코인 차트\n"
    f"/m1 ~ /m{TOP_N} — 30일 순위 코인 차트\n"
    "/심볼 — 차트 직접 요청 (예: /eth, /lab)\n"
    "     현물에 없으면 선물·MEXC까지 자동 검색\n"
    "/help — 이 안내 보기\n\n"
    "⚡ = 바이낸스 선물 상장 코인 (차트 캡션에도 표시)\n"
    "매일 아침 7:30(KST)에 순위가 자동 발송됩니다."
)


# ============ 데이터 수집 ============
def get_usdt_symbols():
    r = requests.get(f"{BINANCE_API}/api/v3/ticker/24hr", timeout=15)
    r.raise_for_status()
    symbols = []
    for t in r.json():
        s = t["symbol"]
        if not s.endswith("USDT"):
            continue
        if any(x in s for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        if float(t["quoteVolume"]) >= MIN_VOLUME_USDT:
            symbols.append(s)
    return symbols


def get_daily_klines(symbol, days=31):
    """순위 스캔용 (바이낸스 현물 전용, 빠름)"""
    r = requests.get(
        f"{BINANCE_API}/api/v3/klines",
        params={"symbol": symbol, "interval": "1d", "limit": days},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def get_daily_klines_any(symbol, days=CHART_DAYS):
    """차트용: 현물 → 선물 → MEXC 순으로 시도. 성공 시 (일봉, 소스이름) 반환"""
    for source_name, url in KLINE_SOURCES:
        try:
            r = requests.get(
                url,
                params={"symbol": symbol, "interval": "1d", "limit": days},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            k = r.json()
            if isinstance(k, list) and len(k) >= 2:
                return k, source_name
        except Exception as e:
            print(f"  {source_name} 조회 실패({symbol}): {e}")
    raise LookupError(symbol)


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


def _scan_one(sym):
    klines = get_daily_klines(sym, days=31)
    v7 = calc_volatility(klines, 7)
    v30 = calc_volatility(klines, 30)
    if v7 and v30:
        return {"symbol": sym, "v7": v7, "v30": v30}
    return None


def scan():
    symbols = get_usdt_symbols()
    print(f"스캔 대상: {len(symbols)}개 코인")
    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_scan_one, s): s for s in symbols}
        for i, fut in enumerate(as_completed(futures)):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception as e:
                print(f"  {futures[fut]} 실패: {e}")
            if (i + 1) % 100 == 0:
                print(f"  진행: {i + 1}/{len(symbols)}")
    top7 = sorted(results, key=lambda x: x["v7"]["std_vol"], reverse=True)[:TOP_N]
    top30 = sorted(results, key=lambda x: x["v30"]["std_vol"], reverse=True)[:TOP_N]
    return annotate_futures(top7, top30)


# ============ 바이낸스 선물 상장 확인 ============
_futures_symbols = None
_futures_checked = False


def get_binance_futures_symbols():
    """선물 상장 심볼 전체 목록 (fapi 접속 가능할 때). 차단 시 None"""
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
            print(f"선물 상장 목록 확보: {len(_futures_symbols)}개 (fapi)")
    except Exception as e:
        print(f"fapi 접속 불가 → 데이터 저장소 방식으로 확인: {e}")
    return _futures_symbols


def is_binance_futures_listed(symbol):
    """바이낸스 선물(무기한) 상장·거래 중인지 확인
    1순위: fapi exchangeInfo (정확, 지역 차단 가능)
    2순위: data.binance.vision에 어제/그제 데이터 파일 존재 여부 (차단 없음, 하루 지연)
    """
    fs = get_binance_futures_symbols()
    if fs is not None:
        return symbol in fs
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


def annotate_futures(top7, top30):
    """순위 리스트의 각 코인에 선물 상장 여부(fut) 표시 (병렬 확인)"""
    union = {r["symbol"] for r in top7} | {r["symbol"] for r in top30}
    listed = {}
    get_binance_futures_symbols()  # fapi 가능 여부 먼저 1회 확인
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = {pool.submit(is_binance_futures_listed, s): s for s in union}
        for f in as_completed(futs):
            try:
                listed[futs[f]] = f.result()
            except Exception:
                listed[futs[f]] = False
    for r in top7 + top30:
        r["fut"] = listed.get(r["symbol"], False)
    return top7, top30


# ============ 유사 심볼 검색 ============
def find_similar_symbols(query, limit=5):
    """현물+선물+MEXC 전체에서 query가 포함된 심볼 검색"""
    sources = [
        f"{BINANCE_API}/api/v3/exchangeInfo",
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "https://api.mexc.com/api/v3/exchangeInfo",
    ]
    found = set()
    for url in sources:
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


def make_chart(symbol):
    """일봉 캔들차트 → (PNG 버퍼, 캡션). 데이터가 짧으면 있는 만큼만 그림"""
    klines, source = get_daily_klines_any(symbol, days=CHART_DAYS)

    # 소스마다 컬럼 수가 다르므로 (바이낸스 12개, MEXC 8개) 앞 6개만 사용
    rows = [k[:6] for k in klines]
    df = pd.DataFrame(rows, columns=["time", "Open", "High", "Low", "Close", "Volume"])
    df.index = pd.to_datetime(df["time"].astype(float), unit="ms")
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

    plot_kwargs = dict(
        type="candle", volume=True, style=style,
        title=f"\n{symbol}  ({len(df)}D Daily · {source})",
        figsize=(11, 7), tight_layout=True,
    )
    if mavs:
        plot_kwargs["mav"] = mavs

    buf = io.BytesIO()
    mpf.plot(df, **plot_kwargs, savefig=dict(fname=buf, dpi=110, format="png"))
    buf.seek(0)

    v7 = calc_volatility(klines, 7)
    v30 = calc_volatility(klines, 30)
    last_close = float(klines[-1][4])
    fut_mark = "✅ 상장" if is_binance_futures_listed(symbol) else "❌ 미상장"
    caption = (
        f"📈 {symbol}  현재가 {last_close:g}  [{source}]\n"
        f"{_fmt_vol_line('7일', v7)}\n"
        f"{_fmt_vol_line('30일', v30)}\n"
        f"바이낸스 선물: {fut_mark}"
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


def format_message(top7, top30):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [f"🔥 <b>변동성 스크리너</b> ({now} KST)\n"]
    lines.append(f"📊 <b>7일 변동성 TOP {TOP_N}</b>")
    for i, r in enumerate(top7, 1):
        name = r["symbol"].replace("USDT", "")
        fut = "⚡" if r.get("fut") else ""
        v = r["v7"]
        arrow = "🟢" if v["change_pct"] >= 0 else "🔴"
        lines.append(
            f"{i}. <b>{name}</b>{fut} | 일변동 {v['std_vol']:.1f}% | "
            f"레인지 {v['range_pct']:.0f}% | {arrow} {v['change_pct']:+.1f}%"
        )
    lines.append(f"\n📊 <b>30일 변동성 TOP {TOP_N}</b>")
    for i, r in enumerate(top30, 1):
        name = r["symbol"].replace("USDT", "")
        fut = "⚡" if r.get("fut") else ""
        v = r["v30"]
        arrow = "🟢" if v["change_pct"] >= 0 else "🔴"
        lines.append(
            f"{i}. <b>{name}</b>{fut} | 일변동 {v['std_vol']:.1f}% | "
            f"레인지 {v['range_pct']:.0f}% | {arrow} {v['change_pct']:+.1f}%"
        )
    lines.append(
        f"\n⚡ = 바이낸스 선물 상장\n"
        f"💬 /1 ~ /{TOP_N} 차트 | /m1 ~ /m{TOP_N} 30일 차트 | /help 도움말"
    )
    return "\n".join(lines)


# ============ 명령 처리 ============
class RankCache:
    def __init__(self):
        self.top7, self.top30, self.ts = [], [], 0.0

    def is_fresh(self):
        return self.top7 and (time.time() - self.ts) < CACHE_MINUTES * 60

    def update(self, top7, top30):
        self.top7, self.top30, self.ts = top7, top30, time.time()


def parse_command(text, cache):
    t = text.strip().lstrip("/").split("@")[0].lower()
    if not t:
        return None
    if t.startswith("m") and t[1:].isdigit():
        idx = int(t[1:]) - 1
        return cache.top30[idx]["symbol"] if 0 <= idx < len(cache.top30) else None
    if t.isdigit():
        idx = int(t) - 1
        return cache.top7[idx]["symbol"] if 0 <= idx < len(cache.top7) else None
    return t.upper() + "USDT" if not t.upper().endswith("USDT") else t.upper()


def handle_command(text, cache):
    cmd = text.strip().lstrip("/").split("@")[0].lower()

    if cmd in ("help", "start"):
        send_message(HELP_TEXT)
        return

    if cmd == "show":
        if not cache.is_fresh():
            send_message("🔄 최신 데이터 스캔 중... (30초~1분)")
            cache.update(*scan())
        send_message(format_message(cache.top7, cache.top30))
        return

    if (cmd.isdigit() or (cmd.startswith("m") and cmd[1:].isdigit())) and not cache.top7:
        send_message("🔄 순위 데이터가 없어 먼저 스캔합니다... (30초~1분)")
        cache.update(*scan())

    symbol = parse_command(text, cache)
    if not symbol:
        send_message("❓ 알 수 없는 명령입니다. /help 를 입력해 보세요.")
        return

    try:
        buf, caption = make_chart(symbol)
        send_photo(buf, caption)
    except LookupError:
        query = symbol.replace("USDT", "")
        similar = find_similar_symbols(query)
        if similar:
            suggestions = " ".join(f"/{s.lower()}" for s in similar)
            send_message(
                f"⚠️ '{query}' 코인을 현물·선물·MEXC 어디에서도 찾을 수 없습니다.\n"
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
        top7, top30 = scan()
        send_message(format_message(top7, top30))
