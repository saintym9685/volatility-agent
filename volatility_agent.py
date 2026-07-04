"""
코인 변동성 스크리너 에이전트 v3 (24시간 상시 봇)

실행 모드 (커맨드라인 인자):
  python volatility_agent.py scan    → 순위 스캔 후 리스트 1회 발송 (아침 알림용)
  python volatility_agent.py listen  → 상시 대기 모드 (텔레그램 명령 응답)

텔레그램 명령어:
  /show        최신 변동성 순위 리스트
  /1 ~ /10     7일 변동성 순위 코인 차트
  /m1 ~ /m10   30일 변동성 순위 코인 차트
  /심볼        차트 직접 요청 (예: /eth, /tlm)
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
MIN_VOLUME_USDT = 10_000_000  # 최소 24h 거래대금 필터
CHART_DAYS = 60               # 차트에 표시할 일봉 개수
CACHE_MINUTES = 15            # /show 시 이 시간 내 캐시가 있으면 재사용
RUN_MINUTES = int(os.environ.get("RUN_MINUTES", "350"))  # listen 모드 1회 근무 시간
STALE_MESSAGE_MINUTES = 30    # 이보다 오래된 메시지는 무시 (봇 재시작 시 폭주 방지)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# GitHub Actions(미국 IP)에서도 접속 가능한 바이낸스 공개 데이터 전용 주소
BINANCE_API = "https://data-api.binance.vision"
TG_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
KST = timezone(timedelta(hours=9))

HELP_TEXT = (
    "🤖 <b>변동성 스크리너 명령어</b>\n\n"
    "/show — 최신 변동성 순위 리스트\n"
    f"/1 ~ /{TOP_N} — 7일 순위 코인 차트\n"
    f"/m1 ~ /m{TOP_N} — 30일 순위 코인 차트\n"
    "/심볼 — 차트 직접 요청 (예: /eth, /btc)\n"
    "/help — 이 안내 보기\n\n"
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
    r = requests.get(
        f"{BINANCE_API}/api/v3/klines",
        params={"symbol": symbol, "interval": "1d", "limit": days},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def calc_volatility(klines, period):
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
    """전체 스캔 (병렬 8개 동시 요청으로 속도 개선)"""
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
    return top7, top30


# ============ 차트 생성 ============
def make_chart(symbol):
    """60일 일봉 캔들차트 + MA7/MA25 + 거래량 → PNG 바이트 반환"""
    klines = get_daily_klines(symbol, days=CHART_DAYS)
    df = pd.DataFrame(
        klines,
        columns=["time", "Open", "High", "Low", "Close", "Volume",
                 "ct", "qv", "n", "tb", "tq", "ig"],
    )
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
    buf = io.BytesIO()
    mpf.plot(
        df, type="candle", volume=True, mav=(7, 25), style=style,
        title=f"\n{symbol}  ({CHART_DAYS}D Daily)",
        figsize=(11, 7), tight_layout=True,
        savefig=dict(fname=buf, dpi=110, format="png"),
    )
    buf.seek(0)

    v7 = calc_volatility(klines, 7)
    v30 = calc_volatility(klines, 30)
    last_close = float(klines[-1][4])
    caption = (
        f"📈 {symbol}  현재가 {last_close:g}\n"
        f"7일: 일변동 {v7['std_vol']:.1f}% / 레인지 {v7['range_pct']:.0f}% / {v7['change_pct']:+.1f}%\n"
        f"30일: 일변동 {v30['std_vol']:.1f}% / 레인지 {v30['range_pct']:.0f}% / {v30['change_pct']:+.1f}%"
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
        v = r["v7"]
        arrow = "🟢" if v["change_pct"] >= 0 else "🔴"
        lines.append(
            f"{i}. <b>{name}</b> | 일변동 {v['std_vol']:.1f}% | "
            f"레인지 {v['range_pct']:.0f}% | {arrow} {v['change_pct']:+.1f}%"
        )
    lines.append(f"\n📊 <b>30일 변동성 TOP {TOP_N}</b>")
    for i, r in enumerate(top30, 1):
        name = r["symbol"].replace("USDT", "")
        v = r["v30"]
        arrow = "🟢" if v["change_pct"] >= 0 else "🔴"
        lines.append(
            f"{i}. <b>{name}</b> | 일변동 {v['std_vol']:.1f}% | "
            f"레인지 {v['range_pct']:.0f}% | {arrow} {v['change_pct']:+.1f}%"
        )
    lines.append(f"\n💬 /1 ~ /{TOP_N} 차트 | /m1 ~ /m{TOP_N} 30일 차트 | /help 도움말")
    return "\n".join(lines)


def find_similar_symbols(query, limit=5):
    """바이낸스 전체 USDT 페어에서 query가 포함된 심볼 검색 (예: 'LAB' → LABUBU 등)"""
    try:
        r = requests.get(f"{BINANCE_API}/api/v3/exchangeInfo", timeout=15)
        r.raise_for_status()
        matches = []
        for s in r.json()["symbols"]:
            sym = s["symbol"]
            if not sym.endswith("USDT") or s.get("status") != "TRADING":
                continue
            base = sym[:-4]  # USDT 제거
            if query in base:
                matches.append(base)
        # 이름이 짧은(=더 비슷한) 순서로 정렬
        matches.sort(key=len)
        return matches[:limit]
    except Exception as e:
        print(f"유사 심볼 검색 실패: {e}")
        return []


# ============ 명령 처리 ============
class RankCache:
    """마지막 스캔 결과를 기억해서 /1, /m1 명령과 /show 캐시에 사용"""
    def __init__(self):
        self.top7, self.top30, self.ts = [], [], 0.0

    def is_fresh(self):
        return self.top7 and (time.time() - self.ts) < CACHE_MINUTES * 60

    def update(self, top7, top30):
        self.top7, self.top30, self.ts = top7, top30, time.time()


def parse_command(text, cache):
    """'/1' → 7일 1위 심볼, '/m3' → 30일 3위, '/tlm' → TLMUSDT"""
    t = text.strip().lstrip("/").split("@")[0].lower()  # /1@봇이름 형태도 처리
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

    # 숫자/심볼 명령인데 순위 캐시가 없으면 먼저 스캔
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
    except requests.HTTPError:
        # 정확한 심볼이 없으면 비슷한 이름의 코인을 찾아서 추천
        query = symbol.replace("USDT", "")
        similar = find_similar_symbols(query)
        if similar:
            suggestions = " ".join(f"/{s.lower()}" for s in similar)
            send_message(
                f"⚠️ '{query}' 코인을 찾을 수 없습니다.\n"
                f"혹시 이 중에 있나요? 👉 {suggestions}"
            )
        else:
            send_message(f"⚠️ '{query}' 코인을 찾을 수 없고, 비슷한 이름도 없습니다.")
    except Exception as e:
        send_message(f"⚠️ {symbol} 차트 생성 실패: {e}")


# ============ 상시 대기 루프 ============
def listen_loop():
    """RUN_MINUTES 동안 텔레그램 명령에 응답. GitHub Actions가 5시간마다 교대."""
    cache = RankCache()
    cache.update(*scan())  # 시작하자마자 순위 준비 (즉답용)
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
            # 교대 직후 이전 봇과 잠깐 겹치면 409 에러 → 잠시 대기
            print(f"응답 오류: {r.get('description', r)}")
            time.sleep(5)
            continue

        for upd in r.get("result", []):
            offset = upd["update_id"] + 1
            msg = upd.get("message", {})
            text = msg.get("text", "")
            chat_id = str(msg.get("chat", {}).get("id", ""))
            msg_time = msg.get("date", 0)

            if chat_id != str(TELEGRAM_CHAT_ID):  # 본인 채팅만 처리
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
