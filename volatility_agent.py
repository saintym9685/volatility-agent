"""
코인 변동성 스크리너 에이전트
- 바이낸스 USDT 페어 전체를 스캔해서 7일/30일 변동성 상위 코인을 텔레그램으로 알림
- 매일 아침 자동 실행 (GitHub Actions 또는 cron)
"""

import os
import math
import requests
from datetime import datetime, timezone, timedelta

# ============ 설정 ============
TOP_N = 10                    # 상위 몇 개 코인을 보여줄지
MIN_VOLUME_USDT = 10_000_000  # 최소 24h 거래대금 (1천만 달러) - 잡코인 필터
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

BINANCE_API = "https://api.binance.com"
KST = timezone(timedelta(hours=9))


def get_usdt_symbols():
    """거래대금 필터를 통과한 USDT 현물 페어 목록"""
    r = requests.get(f"{BINANCE_API}/api/v3/ticker/24hr", timeout=15)
    r.raise_for_status()
    tickers = r.json()
    symbols = []
    for t in tickers:
        s = t["symbol"]
        # USDT 페어만, 레버리지 토큰(UP/DOWN/BULL/BEAR) 제외
        if not s.endswith("USDT"):
            continue
        if any(x in s for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        if float(t["quoteVolume"]) >= MIN_VOLUME_USDT:
            symbols.append(s)
    return symbols


def get_daily_klines(symbol, days=31):
    """일봉 데이터 (최근 days개)"""
    r = requests.get(
        f"{BINANCE_API}/api/v3/klines",
        params={"symbol": symbol, "interval": "1d", "limit": days},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


def calc_volatility(klines, period):
    """
    최근 period일 기준 변동성 계산
    - std_vol: 일간 수익률 표준편차 (%) — 출렁임 정도
    - range_pct: 기간 내 (최고가-최저가)/최저가 (%) — 가격 레인지
    - change_pct: 기간 수익률 (%)
    """
    k = klines[-period:]
    if len(k) < period:
        return None

    closes = [float(c[4]) for c in k]
    highs = [float(c[2]) for c in k]
    lows = [float(c[3]) for c in k]

    # 일간 수익률
    returns = [
        (closes[i] - closes[i - 1]) / closes[i - 1]
        for i in range(1, len(closes))
    ]
    mean = sum(returns) / len(returns)
    std_vol = math.sqrt(sum((r - mean) ** 2 for r in returns) / len(returns)) * 100

    hi, lo = max(highs), min(lows)
    range_pct = (hi - lo) / lo * 100
    change_pct = (closes[-1] - closes[0]) / closes[0] * 100

    return {"std_vol": std_vol, "range_pct": range_pct, "change_pct": change_pct}


def scan():
    """전체 스캔 후 7일/30일 변동성 순위 반환"""
    symbols = get_usdt_symbols()
    print(f"스캔 대상: {len(symbols)}개 코인")

    results = []
    for i, sym in enumerate(symbols):
        try:
            klines = get_daily_klines(sym, days=31)
            v7 = calc_volatility(klines, 7)
            v30 = calc_volatility(klines, 30)
            if v7 and v30:
                results.append({"symbol": sym, "v7": v7, "v30": v30})
        except Exception as e:
            print(f"  {sym} 실패: {e}")
        if (i + 1) % 50 == 0:
            print(f"  진행: {i + 1}/{len(symbols)}")

    top7 = sorted(results, key=lambda x: x["v7"]["std_vol"], reverse=True)[:TOP_N]
    top30 = sorted(results, key=lambda x: x["v30"]["std_vol"], reverse=True)[:TOP_N]
    return top7, top30


def format_message(top7, top30):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [f"🔥 <b>변동성 스크리너</b> ({now} KST)\n"]

    lines.append("📊 <b>7일 변동성 TOP {}</b>".format(TOP_N))
    for i, r in enumerate(top7, 1):
        name = r["symbol"].replace("USDT", "")
        v = r["v7"]
        arrow = "🟢" if v["change_pct"] >= 0 else "🔴"
        lines.append(
            f"{i}. <b>{name}</b> | 일변동 {v['std_vol']:.1f}% | "
            f"레인지 {v['range_pct']:.0f}% | {arrow} {v['change_pct']:+.1f}%"
        )

    lines.append("\n📊 <b>30일 변동성 TOP {}</b>".format(TOP_N))
    for i, r in enumerate(top30, 1):
        name = r["symbol"].replace("USDT", "")
        v = r["v30"]
        arrow = "🟢" if v["change_pct"] >= 0 else "🔴"
        lines.append(
            f"{i}. <b>{name}</b> | 일변동 {v['std_vol']:.1f}% | "
            f"레인지 {v['range_pct']:.0f}% | {arrow} {v['change_pct']:+.1f}%"
        )

    lines.append(f"\n필터: 24h 거래대금 ${MIN_VOLUME_USDT/1e6:.0f}M 이상")
    return "\n".join(lines)


def send_telegram(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ 텔레그램 설정 없음 — 콘솔에만 출력합니다.")
        print(text)
        return
    r = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )
    r.raise_for_status()
    print("✅ 텔레그램 전송 완료")


if __name__ == "__main__":
    top7, top30 = scan()
    msg = format_message(top7, top30)
    send_telegram(msg)
