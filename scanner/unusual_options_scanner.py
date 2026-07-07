"""
Unusual Options Activity (UOA) Scanner
========================================
러셀2000(혹은 원하는 티커 리스트) 종목들을 대상으로
옵션 거래량이 비정상적으로 급증한 종목을 찾아내는 스크립트.

데이터 소스: yfinance (무료, API 키 불필요)

핵심 로직
---------
1. 대상 종목의 옵션체인(가장 가까운 만기 1~2개)을 가져와
   콜+풋 전체 거래량(volume)과 미결제약정(openInterest) 합계를 구한다.
2. Volume / OpenInterest 비율이 높을수록 "오늘 새로 유입된 포지션"이
   많다는 뜻이므로 1차 신호로 사용한다.
3. 매일 실행 결과를 로컬 CSV(history.csv)에 누적 저장해서,
   실행 횟수가 쌓일수록 "최근 N일 평균 거래량 대비 오늘 거래량 배율"
   이라는 진짜 의미의 "급증" 탐지가 가능해진다.
   (처음 실행하는 날은 비교할 과거 데이터가 없으므로 Volume/OI 비율만으로 랭킹)

설치
----
pip install yfinance pandas requests --break-system-packages
(가상환경 쓰는 경우 --break-system-packages 생략 가능)

실행
----
python unusual_options_scanner.py

설정은 아래 CONFIG 섹션에서 조정하세요.
"""

import time
import sys
import os
import datetime
import concurrent.futures as cf

import pandas as pd
import requests

try:
    import yfinance as yf
except ImportError:
    print("yfinance가 설치되어 있지 않습니다. 아래 명령어로 설치 후 다시 실행하세요:")
    print("  pip install yfinance pandas requests --break-system-packages")
    sys.exit(1)


# =========================== CONFIG ===========================

# 병렬 처리 워커 수 (너무 높으면 yfinance/야후 서버에서 rate limit 걸릴 수 있음)
MAX_WORKERS = 8

# 종목당 요청 사이 최소 간격(초) - rate limit 방지용. 필요시 늘리세요.
REQUEST_DELAY = 0.15

# 옵션 만기 중 앞에서부터 몇 개를 합산할지 (가까운 만기 위주로 봄)
NUM_EXPIRIES_TO_CHECK = 2

# 결과 상위 몇 개를 보여줄지
TOP_N = 30

# Volume/OpenInterest 비율이 이 값 이상인 경우만 1차 후보로 포함
MIN_VOL_OI_RATIO = 0.5

# 오늘 총 옵션 거래량이 이 값 미만이면 무시 (유동성 없는 잡주 필터링)
MIN_TOTAL_VOLUME = 200

# 히스토리 저장 파일 (자동 누적됨 - 지우지 마세요, 쌓일수록 정확해집니다)
# 구글 코랩에서 실행 시: 세션이 끊기면 로컬 디스크가 초기화되므로
# 반드시 구글 드라이브 경로를 지정하세요. 예:
#   from google.colab import drive
#   drive.mount('/content/drive')
#   HISTORY_FILE = "/content/drive/MyDrive/options_scanner/options_volume_history.csv"
# 로컬 PC에서 실행할 때는 아래 기본값(현재 폴더)을 그대로 쓰면 됩니다.
HISTORY_FILE = "options_volume_history.csv"

# 오늘 결과 저장 파일 (마찬가지로 코랩에서는 드라이브 경로 권장)
OUTPUT_FILE = "unusual_options_today.csv"

# 과거 평균과 비교할 때 사용할 최근 일수
LOOKBACK_DAYS = 20

# --- 텔레그램 알림 설정 ---
# 보안을 위해 토큰/챗ID는 코드에 직접 쓰지 않고 환경변수에서 읽어옵니다.
# 코랩에서는 아래처럼 셀에서 미리 설정하세요:
#   import os
#   os.environ["TELEGRAM_BOT_TOKEN"] = "여기에_봇토큰"
#   os.environ["TELEGRAM_CHAT_ID"] = "여기에_챗ID"
# 로컬/GitHub Actions에서는 환경변수 또는 Secrets로 주입하면 됩니다.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# 텔레그램 메시지로 보낼 상위 종목 개수 (너무 많으면 메시지가 길어짐)
TELEGRAM_TOP_N = 15

# =================================================================


def get_russell2000_tickers():
    """
    iShares IWM(러셀2000 추종 ETF)의 공식 보유종목 CSV를 다운로드해서
    티커 리스트를 뽑아온다. 네트워크 상황에 따라 URL이 바뀔 수 있으니
    실패하면 fallback으로 직접 리스트를 넣게 안내한다.
    """
    url = (
        "https://www.ishares.com/us/products/239710/"
        "ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund"
    )
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        # iShares CSV는 상단에 메타데이터 몇 줄이 있어서 실제 헤더를 찾아야 함
        lines = resp.text.splitlines()
        header_idx = next(
            i for i, line in enumerate(lines) if line.startswith("Ticker")
        )
        csv_data = "\n".join(lines[header_idx:])
        from io import StringIO
        df = pd.read_csv(StringIO(csv_data))
        tickers = (
            df["Ticker"]
            .dropna()
            .astype(str)
            .str.strip()
            .tolist()
        )
        # 이상한 행(현금, 합계 등) 제거
        tickers = [t for t in tickers if t.isupper() and t.isalpha() or "-" in t or "." in t]
        tickers = [t for t in tickers if len(t) <= 6 and t not in ("USD", "CASH")]
        if len(tickers) < 500:
            raise ValueError("다운로드된 티커 수가 너무 적습니다. 파싱 실패로 추정.")
        return sorted(set(tickers))
    except Exception as e:
        print(f"[경고] iShares에서 러셀2000 리스트 자동 다운로드 실패: {e}")
        print("대신 fallback_tickers.txt 파일에서 읽거나, 아래 SAMPLE 리스트를 사용합니다.")
        if os.path.exists("fallback_tickers.txt"):
            with open("fallback_tickers.txt") as f:
                return [line.strip() for line in f if line.strip()]
        # 최소 동작 확인용 샘플 (실제 러셀2000 일부 종목)
        return [
            "SMCI", "IONQ", "SOUN", "RKLB", "CELH", "PLUG", "FUBO", "CLSK",
            "MARA", "RIOT", "UPST", "AFRM", "SIRI", "PARA", "CHPT", "BBAI",
        ]


def fetch_option_summary(ticker: str):
    """
    한 종목의 옵션체인에서 가까운 만기 N개를 합산해
    총 거래량, 총 OI, Volume/OI 비율을 계산한다.
    또한 콜/풋 각각에서 거래량이 가장 많이 몰린 행사가(strike)와 만기를 찾는다.
    """
    try:
        tk = yf.Ticker(ticker)
        expiries = tk.options
        if not expiries:
            return None

        expiries_to_use = expiries[:NUM_EXPIRIES_TO_CHECK]

        total_call_vol = 0
        total_put_vol = 0
        total_oi = 0

        # 콜/풋 각각에서 거래량이 가장 많은 단일 계약(행사가+만기)을 추적
        top_call = {"strike": None, "volume": -1, "expiry": None}
        top_put = {"strike": None, "volume": -1, "expiry": None}

        for exp in expiries_to_use:
            chain = tk.option_chain(exp)
            calls, puts = chain.calls, chain.puts

            total_call_vol += calls["volume"].fillna(0).sum()
            total_put_vol += puts["volume"].fillna(0).sum()
            total_oi += calls["openInterest"].fillna(0).sum()
            total_oi += puts["openInterest"].fillna(0).sum()

            if not calls.empty:
                call_max_idx = calls["volume"].fillna(0).idxmax()
                call_max_vol = calls.loc[call_max_idx, "volume"]
                if pd.notna(call_max_vol) and call_max_vol > top_call["volume"]:
                    top_call = {
                        "strike": calls.loc[call_max_idx, "strike"],
                        "volume": int(call_max_vol),
                        "expiry": exp,
                    }

            if not puts.empty:
                put_max_idx = puts["volume"].fillna(0).idxmax()
                put_max_vol = puts.loc[put_max_idx, "volume"]
                if pd.notna(put_max_vol) and put_max_vol > top_put["volume"]:
                    top_put = {
                        "strike": puts.loc[put_max_idx, "strike"],
                        "volume": int(put_max_vol),
                        "expiry": exp,
                    }

        total_vol = total_call_vol + total_put_vol
        if total_vol < MIN_TOTAL_VOLUME:
            return None

        vol_oi_ratio = total_vol / total_oi if total_oi > 0 else float("inf")
        put_call_ratio = (total_put_vol / total_call_vol) if total_call_vol > 0 else float("inf")

        return {
            "ticker": ticker,
            "date": datetime.date.today().isoformat(),
            "total_volume": int(total_vol),
            "call_volume": int(total_call_vol),
            "put_volume": int(total_put_vol),
            "open_interest": int(total_oi),
            "vol_oi_ratio": round(vol_oi_ratio, 2),
            "put_call_ratio": round(put_call_ratio, 2),
            "top_call_strike": top_call["strike"],
            "top_call_strike_volume": top_call["volume"] if top_call["volume"] >= 0 else None,
            "top_call_expiry": top_call["expiry"],
            "top_put_strike": top_put["strike"],
            "top_put_strike_volume": top_put["volume"] if top_put["volume"] >= 0 else None,
            "top_put_expiry": top_put["expiry"],
        }
    except Exception:
        return None


def scan_all(tickers):
    results = []
    total = len(tickers)
    done = 0

    def worker(t):
        time.sleep(REQUEST_DELAY)
        return fetch_option_summary(t)

    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, t): t for t in tickers}
        for future in cf.as_completed(futures):
            done += 1
            if done % 50 == 0 or done == total:
                print(f"  진행상황: {done}/{total}")
            r = future.result()
            if r:
                results.append(r)

    return pd.DataFrame(results)


def update_history_and_score(today_df: pd.DataFrame) -> pd.DataFrame:
    """
    히스토리 파일에 오늘 데이터를 누적 저장하고,
    최근 LOOKBACK_DAYS 평균 대비 오늘 거래량 배율(volume_ratio)을 계산한다.
    히스토리가 없는(처음 실행) 경우 volume_ratio는 NaN으로 남는다.
    """
    if os.path.exists(HISTORY_FILE):
        history = pd.read_csv(HISTORY_FILE)
    else:
        history = pd.DataFrame(columns=today_df.columns)

    # 오늘 데이터가 이미 들어있으면 덮어쓰기 (같은 날 재실행 대비)
    history = history[history["date"] != today_df["date"].iloc[0]] if not history.empty else history
    history = pd.concat([history, today_df], ignore_index=True)
    os.makedirs(os.path.dirname(HISTORY_FILE) or ".", exist_ok=True)
    history.to_csv(HISTORY_FILE, index=False)

    # 종목별 최근 평균 거래량 계산 (오늘 제외, 최근 LOOKBACK_DAYS)
    history["date"] = pd.to_datetime(history["date"])
    cutoff = pd.Timestamp(datetime.date.today()) - pd.Timedelta(days=LOOKBACK_DAYS)
    past = history[(history["date"] < pd.Timestamp(datetime.date.today())) & (history["date"] >= cutoff)]
    avg_volume = past.groupby("ticker")["total_volume"].mean().rename("avg_volume_20d")

    today_df = today_df.merge(avg_volume, on="ticker", how="left")
    today_df["volume_ratio_vs_avg"] = (
        today_df["total_volume"] / today_df["avg_volume_20d"]
    ).round(2)

    return today_df


def compute_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    최종 스코어 = vol_oi_ratio + (있으면) volume_ratio_vs_avg 가중 합산.
    히스토리가 부족한 초기에는 vol_oi_ratio 위주로 정렬된다.
    """
    df = df.copy()
    df["volume_ratio_vs_avg"] = df["volume_ratio_vs_avg"].fillna(1.0)

    df["score"] = (
        df["vol_oi_ratio"].clip(upper=10) * 1.0
        + df["volume_ratio_vs_avg"].clip(upper=10) * 1.5
    )
    return df.sort_values("score", ascending=False)


def send_telegram_message(text: str):
    """
    텔레그램 봇 API로 메시지를 전송한다.
    토큰/챗ID가 설정 안 되어 있으면 조용히 스킵한다 (에러로 전체 실행을 막지 않음).
    텔레그램 메시지는 4096자 제한이 있어 초과 시 잘라서 여러 번 보낸다.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("\n[알림] 텔레그램 토큰/챗ID가 설정되지 않아 알림을 건너뜁니다.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 4000  # 여유 두고 자름

    chunks = [text[i:i + max_len] for i in range(0, len(text), max_len)]

    for chunk in chunks:
        try:
            resp = requests.post(
                url,
                data={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": chunk,
                    "parse_mode": "HTML",
                },
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"[경고] 텔레그램 전송 실패: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"[경고] 텔레그램 전송 중 오류: {e}")


def format_telegram_message(df: pd.DataFrame) -> str:
    """
    상위 종목 데이터를 텔레그램용 텍스트로 포맷팅한다.
    - 히스토리가 없어 평균 대비 배율을 모를 경우, 해당 문구는 아예 생략한다.
    - 콜/풋 중 우위인 방향의 행사가(strike)와 만기, 그 행사가에 몰린 거래량을 함께 보여준다.
    """
    today_str = datetime.date.today().isoformat()
    lines = [f"<b>옵션 거래량 급증 스캔 결과 ({today_str})</b>", ""]

    top = df.head(TELEGRAM_TOP_N)
    if top.empty:
        lines.append("오늘은 조건에 맞는 종목이 없습니다.")
        return "\n".join(lines)

    for _, row in top.iterrows():
        is_call_heavy = row["put_call_ratio"] < 1
        direction = "콜 우위" if is_call_heavy else "풋 우위"

        # 첫 줄: 종목, 총거래량, Vol/OI, (있으면) 평균대비 배율, 방향
        first_line = (
            f"• <b>{row['ticker']}</b> — 거래량 {int(row['total_volume']):,} "
            f"(Vol/OI {row['vol_oi_ratio']}, {direction}"
        )
        if pd.notna(row.get("volume_ratio_vs_avg")) and row["volume_ratio_vs_avg"] != 1.0:
            first_line += f", 평균대비 {row['volume_ratio_vs_avg']}배"
        first_line += ")"
        lines.append(first_line)

        # 둘째 줄: 우위 방향의 행사가 집중 정보
        if is_call_heavy:
            strike = row.get("top_call_strike")
            vol = row.get("top_call_strike_volume")
            expiry = row.get("top_call_expiry")
        else:
            strike = row.get("top_put_strike")
            vol = row.get("top_put_strike_volume")
            expiry = row.get("top_put_expiry")

        if pd.notna(strike) and pd.notna(vol):
            option_word = "콜" if is_call_heavy else "풋"
            expiry_str = f", {expiry} 만기" if pd.notna(expiry) and expiry else ""
            lines.append(
                f"   └ ${strike:g} {option_word}에 집중{expiry_str} (거래량 {int(vol):,})"
            )

    lines.append("")
    lines.append("⚠️ 투자 조언이 아닙니다. 참고용 스크리닝 결과입니다.")
    return "\n".join(lines)


def main():
    print("=" * 60)
    print("Unusual Options Activity Scanner - Russell 2000")
    print("=" * 60)

    print("\n[1/4] 러셀2000 티커 리스트 가져오는 중...")
    tickers = get_russell2000_tickers()
    print(f"  대상 종목 수: {len(tickers)}개")

    print(f"\n[2/4] 옵션 데이터 수집 중 (병렬 워커 {MAX_WORKERS}개)...")
    print("  * 종목 수가 많으면 수 분~수십 분 소요될 수 있습니다.")
    df = scan_all(tickers)

    if df.empty:
        print("\n수집된 데이터가 없습니다. 네트워크 상태나 yfinance 상태를 확인하세요.")
        return

    print(f"\n[3/4] 히스토리 갱신 및 급증 배율 계산 중... (수집 성공: {len(df)}개 종목)")
    df = update_history_and_score(df)

    print("\n[4/4] 스코어 계산 및 필터링 중...")
    df = df[df["vol_oi_ratio"] >= MIN_VOL_OI_RATIO]
    df = compute_score(df)

    top = df.head(TOP_N)
    os.makedirs(os.path.dirname(OUTPUT_FILE) or ".", exist_ok=True)
    top.to_csv(OUTPUT_FILE, index=False)

    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 140)
    print("\n" + "=" * 60)
    print(f"옵션 거래량 급증 상위 {len(top)}개 종목")
    print("=" * 60)
    cols = [
        "ticker", "total_volume", "call_volume", "put_volume",
        "open_interest", "vol_oi_ratio", "put_call_ratio",
        "volume_ratio_vs_avg", "score",
    ]
    print(top[cols].to_string(index=False))

    print(f"\n결과 저장됨: {OUTPUT_FILE}")
    print(f"히스토리 누적됨: {HISTORY_FILE} (매일 실행할수록 volume_ratio_vs_avg 정확도 상승)")

    print("\n[5/5] 텔레그램 알림 전송 중...")
    message = format_telegram_message(top)
    send_telegram_message(message)
    print("완료.")


if __name__ == "__main__":
    main()
