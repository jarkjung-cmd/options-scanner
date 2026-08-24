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
import json
import datetime
import concurrent.futures as cf

import pandas as pd
import numpy as np
import requests

try:
    import yfinance as yf
except ImportError:
    print("yfinance가 설치되어 있지 않습니다. 아래 명령어로 설치 후 다시 실행하세요:")
    print("  pip install yfinance pandas requests --break-system-packages")
    sys.exit(1)


# =========================== CONFIG ===========================

# --- 스캔 대상 유니버스 설정 ---
# 어떤 지수의 종목들을 스캔 대상에 포함할지 선택 (여러 개 동시 가능, 중복은 자동 제거됨)
INCLUDE_RUSSELL2000 = True   # 소형주 (기존 기본값)
INCLUDE_SP500 = True         # 대형주 500개
INCLUDE_NASDAQ100 = True     # 나스닥 상위 100개 (S&P500과 상당 부분 겹침 - 자동 중복제거됨)

# 병렬 처리 워커 수 (너무 높으면 yfinance/야후 서버에서 rate limit 걸릴 수 있음)
MAX_WORKERS = 8

# 최종 결과를 대형주(S&P500+나스닥100) / 소형주(러셀2000) 두 그룹으로
# 나눠서 각각 최대 몇 개씩 뽑을지. 필터(MIN_VOL_OI_RATIO)를 통과하는 종목이
# 이 숫자보다 적으면 억지로 채우지 않고 그만큼만 나옵니다.
TOP_N_PER_GROUP = 5

# 종목당 요청 사이 최소 간격(초) - rate limit 방지용. 필요시 늘리세요.
REQUEST_DELAY = 0.15

# 옵션 만기 중 앞에서부터 몇 개를 합산할지 (가까운 만기 위주로 봄)
NUM_EXPIRIES_TO_CHECK = 2

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

# --- 홍보 자동화 설정 ---
# 매일 스캔이 끝나면, 오늘 가장 눈에 띄는 종목으로 SNS 홍보용 이미지와
# 캡션 문구를 자동 생성해서 "관리자용 개인 챗"으로 따로 보내줍니다.
# (고객이 보는 채널과는 다른, 사장님 본인 텔레그램 챗 ID를 넣으세요)
TELEGRAM_ADMIN_CHAT_ID = os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "")

# 홍보 이미지에 같이 보여줄 피드 개수 (너무 많으면 이미지가 복잡해짐)
PROMO_FEED_COUNT = 6

# 홈페이지 반영용 설정 ---
# 오늘의 거래량 상위 3개 종목을 랜딩페이지(index.html)가 읽어갈 수 있도록
# JSON 파일로 저장합니다. 리포지토리 최상단(index.html과 같은 위치)에
# 저장되어야 하므로, 기본적으로 "scanner" 폴더 밖(상위 폴더)을 가리킵니다.
# 로컬에서 폴더 구조가 다르면 이 경로를 맞게 조정하세요.
HOMEPAGE_DATA_FILE = "../homepage_data.json"
HOMEPAGE_TOP_N = 3

# --- 성과 추적(적중률) 설정 ---
# 플래그된 종목이 이후 실제로 어떻게 움직였는지 자동으로 추적합니다.
FLAGS_LOG_FILE = "flags_log.csv"              # 플래그된 종목 + 당시 주가 기록
PERFORMANCE_FILE = "performance_tracking.csv"  # 체크포인트별 성과 기록
# 며칠(거래일 기준) 후에 성과를 체크할지
TRACKING_CHECKPOINTS_DAYS = [1, 3, 5, 10]
# 성과 추적 대상: 매일 상위 몇 개 종목까지 기록할지 (너무 많으면 API 호출 늘어남)
TRACKING_TOP_N = 15

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


def get_sp500_tickers():
    """
    위키피디아 'List of S&P 500 companies' 표에서 티커 리스트를 가져온다.
    실패하면 GitHub에 공개 호스팅된 데이터셋 CSV로 재시도한다.
    """
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]  # 첫 번째 표가 구성종목 리스트
        tickers = df["Symbol"].astype(str).str.strip().str.replace(".", "-", regex=False).tolist()
        tickers = [t for t in tickers if t and t != "nan"]
        if len(tickers) < 400:
            raise ValueError("파싱된 종목 수가 너무 적습니다.")
        return sorted(set(tickers))
    except Exception as e:
        print(f"[경고] 위키피디아에서 S&P500 리스트 가져오기 실패: {e}")
        try:
            url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
            df = pd.read_csv(url)
            tickers = df["Symbol"].astype(str).str.strip().tolist()
            if len(tickers) < 400:
                raise ValueError("파싱된 종목 수가 너무 적습니다.")
            return sorted(set(tickers))
        except Exception as e2:
            print(f"[경고] GitHub 데이터셋에서도 S&P500 가져오기 실패: {e2}")
            return []


def get_nasdaq100_tickers():
    """
    위키피디아 'Nasdaq-100' 표에서 티커 리스트를 가져온다.
    """
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        # 표 구조가 가끔 바뀌므로, 'Ticker'라는 컬럼이 있는 표를 찾는다
        target_df = None
        for t in tables:
            cols = [str(c) for c in t.columns]
            if any("Ticker" in c for c in cols):
                target_df = t
                break
        if target_df is None:
            raise ValueError("Ticker 컬럼이 있는 표를 못 찾음")
        ticker_col = next(c for c in target_df.columns if "Ticker" in str(c))
        tickers = target_df[ticker_col].astype(str).str.strip().tolist()
        tickers = [t for t in tickers if t and t != "nan"]
        if len(tickers) < 80:
            raise ValueError("파싱된 종목 수가 너무 적습니다.")
        return sorted(set(tickers))
    except Exception as e:
        print(f"[경고] 위키피디아에서 나스닥100 리스트 가져오기 실패: {e}")
        return []


def get_combined_universe():
    """
    설정(INCLUDE_RUSSELL2000/INCLUDE_SP500/INCLUDE_NASDAQ100)에 따라
    여러 지수의 종목 리스트를 가져와 합치고 중복 제거한 최종 스캔 대상을 만든다.

    반환값: (전체 티커 리스트, {티커: 그룹} 매핑 딕셔너리)
    그룹은 'large_cap'(S&P500/나스닥100) 또는 'small_cap'(러셀2000) 둘 중 하나.
    한 종목이 두 그룹에 다 속하면(드물지만) large_cap 우선.
    """
    all_tickers = set()
    sources_used = []
    group_map = {}

    # 먼저 소형주(러셀2000)로 채워두고, 대형주 목록으로 덮어써서 large_cap이 우선되게 함
    if INCLUDE_RUSSELL2000:
        r2000 = get_russell2000_tickers()
        all_tickers.update(r2000)
        for t in r2000:
            group_map[t] = "small_cap"
        sources_used.append(f"Russell 2000: {len(r2000)}개")

    if INCLUDE_SP500:
        sp500 = get_sp500_tickers()
        all_tickers.update(sp500)
        for t in sp500:
            group_map[t] = "large_cap"
        sources_used.append(f"S&P 500: {len(sp500)}개")

    if INCLUDE_NASDAQ100:
        ndx100 = get_nasdaq100_tickers()
        all_tickers.update(ndx100)
        for t in ndx100:
            group_map[t] = "large_cap"
        sources_used.append(f"Nasdaq 100: {len(ndx100)}개")

    for s in sources_used:
        print(f"  - {s}")
    print(f"  - 중복 제거 후 총 스캔 대상: {len(all_tickers)}개")

    return sorted(all_tickers), group_map


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

        # 성과 추적용 현재 주가 (실패해도 옵션 데이터 자체는 살려야 하므로 별도 예외 처리)
        stock_price = None
        try:
            fast_info = tk.fast_info
            stock_price = float(fast_info["last_price"])
        except Exception:
            try:
                hist = tk.history(period="1d")
                if not hist.empty:
                    stock_price = float(hist["Close"].iloc[-1])
            except Exception:
                pass

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
            "stock_price": stock_price,
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


def send_telegram_photo(chat_id: str, photo_path: str, caption: str = ""):
    """
    텔레그램 봇 API로 사진 파일을 전송한다 (홍보 이미지 발송용).
    """
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, "rb") as f:
            resp = requests.post(
                url,
                data={"chat_id": chat_id, "caption": caption[:1024], "parse_mode": "HTML"},
                files={"photo": f},
                timeout=30,
            )
        if resp.status_code != 200:
            print(f"[경고] 텔레그램 사진 전송 실패: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[경고] 텔레그램 사진 전송 중 오류: {e}")


def build_promo_html(top_df: pd.DataFrame, highlight: dict) -> str:
    """
    오늘의 하이라이트 종목 + 상위 피드를 담은 SNS 홍보용 정사각형(1080x1080)
    이미지 HTML을 만든다. 실제 오늘 데이터로 채워진다.
    """
    today_str = datetime.date.today().isoformat()
    rows_html = ""
    for _, row in top_df.head(PROMO_FEED_COUNT).iterrows():
        is_call = row["put_call_ratio"] < 1
        strike = row.get("top_call_strike") if is_call else row.get("top_put_strike")
        vol = row.get("top_call_strike_volume") if is_call else row.get("top_put_strike_volume")
        expiry = row.get("top_call_expiry") if is_call else row.get("top_put_expiry")
        expiry_short = str(expiry)[5:] if pd.notna(expiry) and expiry else ""
        pill_class = "call" if is_call else "put"
        pill_text = "CALL" if is_call else "PUT"
        strike_txt = f"${strike:g}{'c' if is_call else 'p'} {expiry_short}" if pd.notna(strike) else ""
        rows_html += f"""
          <div class="row">
            <span><span class="tk">{row['ticker']}</span><span class="meta">{strike_txt}</span></span>
            <span><span class="pill {pill_class}">{pill_text}</span> <span class="vol">{int(row['total_volume']):,}</span></span>
          </div>"""

    h_is_call = highlight["put_call_ratio"] < 1
    h_strike = highlight.get("top_call_strike") if h_is_call else highlight.get("top_put_strike")
    h_type = "call" if h_is_call else "put"

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700;800&family=Inter:wght@400;600;700;800&display=swap');
  :root {{ --bg:#070a08; --bg2:#0d1310; --line:#1b2521; --text:#eef2ef; --text-dim:#7d8c86; --green:#3ddc84; --gold:#f0c14b; --red:#ff6b6b; }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  html,body {{ width:1080px; height:1080px; background:var(--bg); overflow:hidden; font-family:'Inter',sans-serif; }}
  .canvas {{ position:relative; width:1080px; height:1080px;
    background: radial-gradient(ellipse 800px 500px at 50% 0%, rgba(61,220,132,0.09), transparent 60%),
                radial-gradient(ellipse 600px 400px at 90% 100%, rgba(240,193,75,0.07), transparent 60%), var(--bg); }}
  .grid {{ position:absolute; inset:0; background-image:
    linear-gradient(to right, rgba(255,255,255,0.025) 1px, transparent 1px),
    linear-gradient(to bottom, rgba(255,255,255,0.025) 1px, transparent 1px); background-size:44px 44px; }}
  .kicker {{ position:absolute; top:64px; left:64px; font-family:'JetBrains Mono',monospace; font-size:14px;
    letter-spacing:0.16em; color:var(--green); text-transform:uppercase; display:flex; align-items:center; gap:10px; }}
  .kicker .dot {{ width:8px; height:8px; border-radius:50%; background:var(--green); box-shadow:0 0 12px var(--green); }}
  .headline {{ position:absolute; top:108px; left:64px; width:820px; font-weight:800; font-size:52px;
    line-height:1.12; letter-spacing:-0.025em; color:var(--text); }}
  .headline .hl {{ color:var(--gold); }}
  .sub {{ position:absolute; top:322px; left:64px; width:640px; font-size:18px; line-height:1.55; color:var(--text-dim); }}
  .panel {{ position:absolute; top:414px; left:64px; width:952px; height:490px; background:var(--bg2);
    border:1px solid var(--line); border-radius:16px; overflow:hidden; display:flex; }}
  .feed-col {{ width:420px; border-right:1px solid var(--line); }}
  .panel-head {{ display:flex; justify-content:space-between; align-items:center; padding:18px 22px;
    border-bottom:1px solid var(--line); font-family:'JetBrains Mono',monospace; font-size:13px; color:var(--text-dim); }}
  .panel-head b {{ color:var(--text); font-weight:700; }}
  .feed {{ padding:6px 22px; }}
  .row {{ display:flex; justify-content:space-between; align-items:center; padding:13px 0;
    border-bottom:1px dashed var(--line); font-family:'JetBrains Mono',monospace; font-size:14px; }}
  .row:last-child {{ border-bottom:none; }}
  .tk {{ font-weight:700; color:var(--text); width:64px; display:inline-block; }}
  .meta {{ color:var(--text-dim); font-size:12.5px; }}
  .pill {{ font-size:11px; font-weight:700; padding:3px 9px; border-radius:4px; }}
  .pill.call {{ background:rgba(61,220,132,0.16); color:var(--green); }}
  .pill.put {{ background:rgba(255,107,107,0.16); color:var(--red); }}
  .vol {{ color:var(--gold); font-weight:700; }}
  .chart-col {{ flex:1; padding:18px 24px; display:flex; flex-direction:column; }}
  .chart-title {{ font-family:'JetBrains Mono',monospace; font-size:13px; color:var(--text); font-weight:700; margin-bottom:4px; }}
  .chart-label {{ font-family:'JetBrains Mono',monospace; font-size:12px; color:var(--text-dim); margin-bottom:14px; display:flex; justify-content:space-between; }}
  .chart-label .hl {{ color:var(--gold); }}
  .brandbar {{ position:absolute; bottom:56px; left:64px; display:flex; align-items:center; gap:16px; }}
  .logo {{ font-family:'JetBrains Mono',monospace; font-weight:800; font-size:24px; letter-spacing:-0.02em;
    color:var(--text); display:flex; align-items:center; gap:10px; }}
  .logo .sq {{ width:12px; height:12px; background:var(--green); border-radius:2px; box-shadow:0 0 12px var(--green); }}
  .tagline {{ font-family:'JetBrains Mono',monospace; font-size:14px; color:var(--text-dim); border-left:1px solid var(--line); padding-left:16px; }}
</style></head>
<body>
  <div class="canvas">
    <div class="grid"></div>
    <div class="kicker"><span class="dot"></span>OPTIONSCANNER · {today_str}</div>
    <div class="headline">Big money leaves<br>a trail <span class="hl">before</span><br>the price moves.</div>
    <div class="sub">Today's scan: <b style="color:var(--text)">${highlight['ticker']}</b> — {int(highlight['total_volume']):,} contracts,
      concentrated at the ${h_strike:g} {h_type}. Flagged before the crowd noticed.</div>
    <div class="panel">
      <div class="feed-col">
        <div class="panel-head"><span>TODAY'S SCAN</span><b>{len(top_df)} flagged</b></div>
        <div class="feed">{rows_html}</div>
      </div>
      <div class="chart-col">
        <div class="chart-title">${highlight['ticker']} — options volume vs. price</div>
        <div class="chart-label"><span>historical volume</span><span class="hl">flagged here ↓</span></div>
        <svg width="484" height="200" viewBox="0 0 484 200" style="margin-top:8px;">
          <line x1="0" y1="150" x2="484" y2="150" stroke="#1b2521" stroke-width="1"/>
          <g fill="#22302a">
            <rect x="6" y="136" width="12" height="14"/><rect x="28" y="132" width="12" height="18"/>
            <rect x="50" y="140" width="12" height="10"/><rect x="72" y="134" width="12" height="16"/>
            <rect x="94" y="138" width="12" height="12"/><rect x="116" y="130" width="12" height="20"/>
            <rect x="138" y="135" width="12" height="15"/>
          </g>
          <rect x="164" y="58" width="18" height="92" fill="#f0c14b"/>
          <circle cx="173" cy="50" r="6" fill="#f0c14b"/>
          <circle cx="173" cy="50" r="12" fill="none" stroke="#f0c14b" stroke-width="1.5" opacity="0.5"/>
          <g fill="#22302a"><rect x="200" y="137" width="12" height="13"/><rect x="222" y="135" width="12" height="15"/></g>
          <line x1="245" y1="12" x2="245" y2="172" stroke="#2a3a33" stroke-width="1" stroke-dasharray="3,4"/>
          <polyline points="0,122 35,120 70,124 105,119 140,122 175,120 190,116 220,108 250,96 280,80 310,64 340,50 370,38 400,28 430,18 460,10 484,7"
            fill="none" stroke="#3ddc84" stroke-width="3"/>
          <text x="248" y="188" font-family="JetBrains Mono, monospace" font-size="12" fill="#7d8c86">flow detected</text>
          <text x="395" y="38" font-family="JetBrains Mono, monospace" font-size="12" fill="#3ddc84" font-weight="700">price follows →</text>
        </svg>
      </div>
    </div>
    <div class="brandbar">
      <div class="logo"><span class="sq"></span>OPTIONSCANNER</div>
      <div class="tagline">7-day free trial · $30/mo</div>
    </div>
  </div>
</body></html>"""


def render_promo_image(top_df: pd.DataFrame, highlight: dict, out_path: str = "promo_today.png"):
    """
    playwright(headless chromium)로 홍보 이미지 HTML을 PNG로 렌더링한다.
    playwright가 설치되어 있지 않으면 조용히 건너뛴다.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[알림] playwright가 설치되어 있지 않아 홍보 이미지 생성을 건너뜁니다.")
        print("       pip install playwright && playwright install --with-deps chromium")
        return None

    html = build_promo_html(top_df, highlight)
    html_path = "promo_today.html"
    with open(html_path, "w") as f:
        f.write(html)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page(viewport={"width": 1080, "height": 1080}, device_scale_factor=2)
            page.goto(f"file://{os.path.abspath(html_path)}")
            page.wait_for_timeout(400)
            page.screenshot(path=out_path)
            browser.close()
        return out_path
    except Exception as e:
        print(f"[경고] 홍보 이미지 렌더링 실패: {e}")
        return None


def generate_captions(highlight: dict) -> str:
    """
    오늘의 하이라이트 종목 데이터로 X(트위터)/인스타·Threads용 캡션 문구를 만든다.
    글자수 제한(트위터 t.co 링크 23자 감안)을 지켜서 생성한다.
    """
    is_call = highlight["put_call_ratio"] < 1
    strike = highlight.get("top_call_strike") if is_call else highlight.get("top_put_strike")
    opt_word = "call" if is_call else "put"
    ticker = highlight["ticker"]
    vol = int(highlight["total_volume"])

    twitter = (
        f"${ticker} options volume just hit {vol:,} contracts, concentrated at the "
        f"${strike:g} {opt_word}.\n\n"
        f"That's not retail size. That's big money positioning early — before the chart shows it.\n\n"
        f"We flag this daily.\n\n"
        f"7-day free trial → [link]"
    )

    ig = (
        f"Today's flag: ${ticker}\n\n"
        f"{vol:,} contracts piling into the ${strike:g} {opt_word}.\n"
        f"Big money leaves a trail before price moves — we track it daily.\n\n"
        f"7-day free trial, link in bio.\n"
        f"#OptionsFlow #SmallCapStocks #OptionsTrading"
    )

    return (
        f"📣 <b>Today's promo material</b> ({datetime.date.today().isoformat()})\n\n"
        f"<b>--- X (Twitter) ---</b>\n{twitter}\n\n"
        f"<b>--- Instagram / Threads ---</b>\n{ig}\n\n"
        f"⚠️ Reminder: replace [link] with your real landing page URL before posting."
    )


def generate_promo_assets(top_df: pd.DataFrame):
    """
    오늘의 하이라이트(스코어 1위) 종목으로 홍보 이미지 + 캡션을 만들어
    관리자 텔레그램 챗으로 보낸다. TELEGRAM_ADMIN_CHAT_ID가 없으면 건너뛴다.
    """
    if not TELEGRAM_ADMIN_CHAT_ID:
        print("[알림] TELEGRAM_ADMIN_CHAT_ID가 설정되지 않아 홍보 자동화를 건너뜁니다.")
        return
    if top_df.empty:
        return

    highlight = top_df.iloc[0].to_dict()
    print(f"[홍보 자동화] 오늘의 하이라이트 종목: {highlight['ticker']}")

    image_path = render_promo_image(top_df, highlight)
    captions = generate_captions(highlight)

    if image_path and os.path.exists(image_path):
        send_telegram_photo(TELEGRAM_ADMIN_CHAT_ID, image_path, caption=f"Today's promo image: ${highlight['ticker']}")
    send_telegram_message_to(TELEGRAM_ADMIN_CHAT_ID, captions)


def send_telegram_message_to(chat_id: str, text: str):
    """send_telegram_message과 동일하지만 임의의 chat_id로 보낼 수 있는 버전."""
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    max_len = 4000
    for i in range(0, len(text), max_len):
        chunk = text[i:i + max_len]
        try:
            resp = requests.post(
                url,
                data={"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"},
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"[경고] 관리자 텔레그램 전송 실패: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"[경고] 관리자 텔레그램 전송 중 오류: {e}")


def _format_ticker_lines(row) -> list:
    """한 종목에 대한 텔레그램 메시지 두 줄(요약 + 행사가 정보)을 만든다."""
    is_call_heavy = row["put_call_ratio"] < 1
    direction = "Call-heavy" if is_call_heavy else "Put-heavy"

    first_line = (
        f"• <b>{row['ticker']}</b> — Vol {int(row['total_volume']):,} "
        f"(Vol/OI {row['vol_oi_ratio']}, {direction}"
    )
    if pd.notna(row.get("volume_ratio_vs_avg")) and row["volume_ratio_vs_avg"] != 1.0:
        first_line += f", {row['volume_ratio_vs_avg']}x avg"
    first_line += ")"

    result = [first_line]

    if is_call_heavy:
        strike = row.get("top_call_strike")
        vol = row.get("top_call_strike_volume")
        expiry = row.get("top_call_expiry")
    else:
        strike = row.get("top_put_strike")
        vol = row.get("top_put_strike_volume")
        expiry = row.get("top_put_expiry")

    if pd.notna(strike) and pd.notna(vol):
        option_word = "call" if is_call_heavy else "put"
        expiry_str = f", exp {expiry}" if pd.notna(expiry) and expiry else ""
        result.append(f"   └ Concentrated at ${strike:g} {option_word}{expiry_str} (vol {int(vol):,})")

    return result


def format_telegram_message(df: pd.DataFrame) -> str:
    """
    상위 종목 데이터를 텔레그램용 영어 텍스트로 포맷팅한다.
    (구독자가 해외 사용자 위주이므로 텔레그램 발송 메시지는 영어로 작성)
    - 히스토리가 없어 평균 대비 배율을 모를 경우, 해당 문구는 아예 생략한다.
    - 콜/풋 중 우위인 방향의 행사가(strike)와 만기, 그 행사가에 몰린 거래량을 함께 보여준다.
    - df에 'group' 컬럼(large_cap/small_cap)이 있으면 섹션을 나눠서 보여준다.
    """
    today_str = datetime.date.today().isoformat()
    lines = [f"<b>Unusual Options Activity Scan ({today_str})</b>", ""]

    top = df.head(TELEGRAM_TOP_N)
    if top.empty:
        lines.append("No tickers matched today's criteria.")
        return "\n".join(lines)

    if "group" in top.columns:
        large_cap = top[top["group"] == "large_cap"]
        small_cap = top[top["group"] == "small_cap"]

        if not large_cap.empty:
            lines.append("📈 <b>Large-Cap (S&P 500 / Nasdaq 100)</b>")
            for _, row in large_cap.iterrows():
                lines.extend(_format_ticker_lines(row))
            lines.append("")

        if not small_cap.empty:
            lines.append("📊 <b>Small-Cap (Russell 2000)</b>")
            for _, row in small_cap.iterrows():
                lines.extend(_format_ticker_lines(row))
            lines.append("")

        if large_cap.empty and small_cap.empty:
            lines.append("No tickers matched today's criteria.")
    else:
        # group 정보 없으면 기존 방식대로 그냥 나열 (하위 호환)
        for _, row in top.iterrows():
            lines.extend(_format_ticker_lines(row))
        lines.append("")

    lines.append("⚠️ Not investment advice. For informational/screening purposes only.")
    return "\n".join(lines)


def record_new_flags(top_df: pd.DataFrame):
    """
    오늘 플래그된 종목(상위 TRACKING_TOP_N개)을 flags_log.csv에 기록한다.
    같은 종목이 같은 날 다시 플래그되면 덮어쓴다 (중복 방지).
    """
    if top_df.empty:
        return

    today_str = datetime.date.today().isoformat()
    rows = []
    for _, row in top_df.head(TRACKING_TOP_N).iterrows():
        if pd.isna(row.get("stock_price")):
            continue  # 주가를 못 가져온 경우 추적 불가하므로 스킵
        is_call = row["put_call_ratio"] < 1
        rows.append({
            "ticker": row["ticker"],
            "flag_date": today_str,
            "direction": "call" if is_call else "put",
            "price_at_flag": round(float(row["stock_price"]), 4),
            "total_volume": int(row["total_volume"]),
            "vol_oi_ratio": row["vol_oi_ratio"] if row["vol_oi_ratio"] != float("inf") else None,
        })

    if not rows:
        return

    new_df = pd.DataFrame(rows)

    if os.path.exists(FLAGS_LOG_FILE):
        existing = pd.read_csv(FLAGS_LOG_FILE)
        # 같은 (ticker, flag_date) 조합은 오늘 새로 기록한 걸로 덮어씀
        key_cols = ["ticker", "flag_date"]
        existing = existing[~existing.set_index(key_cols).index.isin(new_df.set_index(key_cols).index)]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_csv(FLAGS_LOG_FILE, index=False)
    print(f"[성과 추적] 오늘 {len(new_df)}개 종목을 {FLAGS_LOG_FILE}에 기록")


def update_performance_tracking():
    """
    flags_log.csv를 읽어서, 체크포인트(1/3/5/10 거래일)에 도달한 종목들의
    현재 주가를 조회하고 등락률/적중여부를 performance_tracking.csv에 기록한다.
    이미 기록된 (ticker, flag_date, checkpoint_days) 조합은 건너뛴다.
    """
    if not os.path.exists(FLAGS_LOG_FILE):
        return

    flags = pd.read_csv(FLAGS_LOG_FILE)
    if flags.empty:
        return

    if os.path.exists(PERFORMANCE_FILE):
        perf = pd.read_csv(PERFORMANCE_FILE)
        done_keys = set(zip(perf["ticker"], perf["flag_date"], perf["checkpoint_days"]))
    else:
        perf = pd.DataFrame()
        done_keys = set()

    today = np.datetime64(datetime.date.today().isoformat())
    new_records = []
    price_cache = {}

    for _, row in flags.iterrows():
        flag_date = np.datetime64(row["flag_date"])
        elapsed = int(np.busday_count(flag_date, today))  # 거래일 기준 경과일

        for checkpoint in TRACKING_CHECKPOINTS_DAYS:
            key = (row["ticker"], row["flag_date"], checkpoint)
            if key in done_keys:
                continue
            if elapsed < checkpoint:
                continue  # 아직 그 시점이 안 됨

            # 같은 실행 안에서 같은 티커 여러 번 조회하지 않도록 캐싱
            if row["ticker"] not in price_cache:
                try:
                    tk = yf.Ticker(row["ticker"])
                    price_cache[row["ticker"]] = float(tk.fast_info["last_price"])
                except Exception:
                    price_cache[row["ticker"]] = None

            current_price = price_cache[row["ticker"]]
            if current_price is None:
                continue

            price_at_flag = row["price_at_flag"]
            pct_change = round((current_price - price_at_flag) / price_at_flag * 100, 2)
            direction = row["direction"]
            hit = (direction == "call" and pct_change > 0) or (direction == "put" and pct_change < 0)

            new_records.append({
                "ticker": row["ticker"],
                "flag_date": row["flag_date"],
                "direction": direction,
                "checkpoint_days": checkpoint,
                "checked_date": datetime.date.today().isoformat(),
                "price_at_flag": price_at_flag,
                "price_at_checkpoint": round(current_price, 4),
                "pct_change": pct_change,
                "hit": hit,
            })

    if not new_records:
        print("[성과 추적] 오늘 새로 체크포인트에 도달한 종목 없음")
        return

    new_perf_df = pd.DataFrame(new_records)
    combined = pd.concat([perf, new_perf_df], ignore_index=True) if not perf.empty else new_perf_df
    combined.to_csv(PERFORMANCE_FILE, index=False)
    print(f"[성과 추적] {len(new_records)}건 신규 체크포인트 결과 기록 완료")


# 이 값(%) 이상 움직인 종목을 "빅무버"로 따로 뽑아서 보여준다
BIG_MOVE_THRESHOLD_PCT = 20


def build_performance_summary() -> str:
    """
    performance_tracking.csv를 집계해서 체크포인트별 적중률 요약 텍스트를 만들고,
    BIG_MOVE_THRESHOLD_PCT(%) 이상 크게 움직인 종목은 실제 가격 변화까지 별도로 보여준다.
    관리자 텔레그램으로만 보내는 용도 (고객 대상 성과 주장은 규제 리스크가 있으므로 비공개).
    """
    if not os.path.exists(PERFORMANCE_FILE):
        return "아직 집계할 성과 데이터가 없습니다 (체크포인트 도달 대기 중)."

    perf = pd.read_csv(PERFORMANCE_FILE)
    if perf.empty:
        return "아직 집계할 성과 데이터가 없습니다."

    lines = ["📊 <b>적중률 통계 (내부 참고용)</b>", ""]
    for checkpoint in sorted(perf["checkpoint_days"].unique()):
        subset = perf[perf["checkpoint_days"] == checkpoint]
        hit_rate = subset["hit"].mean() * 100
        avg_move = subset["pct_change"].abs().mean()
        lines.append(
            f"• {checkpoint}거래일 후: 적중률 {hit_rate:.1f}% "
            f"(표본 {len(subset)}건, 평균 변동폭 {avg_move:.1f}%)"
        )

    lines.append("")
    lines.append("⚠️ 표본이 적을수록 신뢰도가 낮습니다. 고객 대상 마케팅에 활용 전 반드시 표본 크기를 확인하세요.")

    # --- 20% 이상 크게 움직인 종목 리스트 ---
    big_movers = perf[perf["pct_change"].abs() >= BIG_MOVE_THRESHOLD_PCT].copy()
    if not big_movers.empty:
        big_movers = big_movers.sort_values("pct_change", key=lambda s: s.abs(), ascending=False)

        lines.append("")
        lines.append(f"🚀 <b>{BIG_MOVE_THRESHOLD_PCT}% 이상 움직인 종목</b>")
        lines.append("")
        for _, row in big_movers.iterrows():
            direction_kr = "콜" if row["direction"] == "call" else "풋"
            hit_mark = "✅ 적중" if row["hit"] else "❌ 반대방향"
            sign = "+" if row["pct_change"] > 0 else ""
            lines.append(
                f"• <b>{row['ticker']}</b> ({direction_kr} 플래그, {row['checkpoint_days']}거래일 후) — "
                f"${row['price_at_flag']:.2f} → ${row['price_at_checkpoint']:.2f} "
                f"({sign}{row['pct_change']:.1f}%) {hit_mark}"
            )
            lines.append(f"   플래그일: {row['flag_date']} · 체크일: {row['checked_date']}")
    else:
        lines.append("")
        lines.append(f"🚀 {BIG_MOVE_THRESHOLD_PCT}% 이상 움직인 종목: 아직 없음")

    return "\n".join(lines)


def build_daily_social_digest() -> str:
    """
    오늘 새로 성과가 확인된 종목 중 가장 크게 움직인 종목으로
    X(트위터)/인스타·Threads용 '케이스 스터디' 초안을 자동으로 만든다.

    체리피킹 방지를 위해, 해당 체크포인트의 전체 적중률(맥락)을
    반드시 문구 안에 같이 포함시킨다. 오늘 새로운 빅무버가 없으면
    최신 적중률 통계만 참고용으로 보낸다.

    관리자 텔레그램으로만 보내는 초안입니다. 실제 포스팅 여부/문구
    수정은 사람이 검토 후 결정하세요.
    """
    if not os.path.exists(PERFORMANCE_FILE):
        return None

    perf = pd.read_csv(PERFORMANCE_FILE)
    if perf.empty:
        return None

    today_str = datetime.date.today().isoformat()
    new_today = perf[perf["checked_date"] == today_str]
    big_new = new_today[new_today["pct_change"].abs() >= BIG_MOVE_THRESHOLD_PCT]
    big_new = big_new.sort_values("pct_change", key=lambda s: s.abs(), ascending=False)

    lines = ["📱 <b>오늘의 SNS 소재 초안 (참고용, 검토 후 사용)</b>", ""]

    if big_new.empty:
        lines.append(f"오늘 새로 {BIG_MOVE_THRESHOLD_PCT}% 이상 움직인 종목은 없습니다.")
        lines.append("아래는 참고용 최신 적중률입니다:")
        lines.append("")
        for checkpoint in sorted(perf["checkpoint_days"].unique()):
            subset = perf[perf["checkpoint_days"] == checkpoint]
            hit_rate = subset["hit"].mean() * 100
            lines.append(f"• {checkpoint}거래일 적중률: {hit_rate:.1f}% (표본 {len(subset)}건)")
        return "\n".join(lines)

    hero = big_new.iloc[0]
    checkpoint = int(hero["checkpoint_days"])
    same_checkpoint = perf[perf["checkpoint_days"] == checkpoint]
    hit_rate_ctx = same_checkpoint["hit"].mean() * 100
    sample_n = len(same_checkpoint)

    direction_word = "call" if hero["direction"] == "call" else "put"
    sign = "+" if hero["pct_change"] > 0 else ""

    twitter_caption = (
        f"${hero['ticker']} was flagged {direction_word}-heavy {checkpoint} trading days ago.\n\n"
        f"Price moved {sign}{hero['pct_change']:.1f}% since (${hero['price_at_flag']:.2f} → "
        f"${hero['price_at_checkpoint']:.2f}).\n\n"
        f"Context: our {checkpoint}-day hit rate across all flags is {hit_rate_ctx:.1f}% "
        f"(n={sample_n}) — one signal is one data point, not a guarantee.\n\n"
        f"Free trial → [link]"
    )

    ig_caption = (
        f"Case study: ${hero['ticker']}\n\n"
        f"Flagged {direction_word}-heavy {checkpoint} trading days ago at ${hero['price_at_flag']:.2f}.\n"
        f"Now at ${hero['price_at_checkpoint']:.2f} ({sign}{hero['pct_change']:.1f}%).\n\n"
        f"To be transparent: across all our flags, the {checkpoint}-day hit rate is "
        f"{hit_rate_ctx:.1f}% (n={sample_n}). We show the real numbers, wins and losses alike.\n\n"
        f"Free trial, link in bio.\n"
        f"#OptionsFlow #SmallCapStocks #OptionsTrading"
    )

    lines.append(f"오늘의 하이라이트: ${hero['ticker']} ({sign}{hero['pct_change']:.1f}%, {checkpoint}거래일 후)")
    lines.append("")
    lines.append("--- X(트위터)용 ---")
    lines.append(twitter_caption)
    lines.append("")
    lines.append("--- 인스타/Threads용 ---")
    lines.append(ig_caption)
    lines.append("")
    lines.append("⚠️ 적중률 맥락 문구는 체리피킹 방지용이니 지우지 말고 그대로 사용하세요.")
    lines.append("⚠️ [link]는 실제 랜딩페이지 주소로 바꿔서 사용하세요.")

    return "\n".join(lines)


def write_homepage_data(top_df: pd.DataFrame):
    """
    오늘의 거래량 상위 N개 종목을 랜딩페이지가 읽을 JSON 파일로 저장한다.
    랜딩페이지(index.html)의 JS가 이 파일을 fetch해서 실시간으로 카드에 표시한다.
    """
    if top_df.empty:
        return

    ranked = top_df.sort_values("total_volume", ascending=False).head(HOMEPAGE_TOP_N)
    items = []
    for _, row in ranked.iterrows():
        is_call = row["put_call_ratio"] < 1
        strike = row.get("top_call_strike") if is_call else row.get("top_put_strike")
        strike_vol = row.get("top_call_strike_volume") if is_call else row.get("top_put_strike_volume")
        expiry = row.get("top_call_expiry") if is_call else row.get("top_put_expiry")
        items.append({
            "ticker": row["ticker"],
            "direction": "call" if is_call else "put",
            "strike": None if pd.isna(strike) else float(strike),
            "strike_volume": None if pd.isna(strike_vol) else int(strike_vol),
            "expiry": None if pd.isna(expiry) or not expiry else str(expiry),
            "total_volume": int(row["total_volume"]),
            "vol_oi_ratio": float(row["vol_oi_ratio"]) if pd.notna(row["vol_oi_ratio"]) and row["vol_oi_ratio"] != float("inf") else None,
            "volume_ratio_vs_avg": (
                float(row["volume_ratio_vs_avg"])
                if pd.notna(row.get("volume_ratio_vs_avg")) and row["volume_ratio_vs_avg"] != 1.0
                else None
            ),
        })

    payload = {
        "date": datetime.date.today().isoformat(),
        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "items": items,
    }

    try:
        os.makedirs(os.path.dirname(HOMEPAGE_DATA_FILE) or ".", exist_ok=True)
        with open(HOMEPAGE_DATA_FILE, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"[홈페이지 반영] {HOMEPAGE_DATA_FILE} 에 상위 {len(items)}개 종목 저장 완료")
    except Exception as e:
        print(f"[경고] 홈페이지 데이터 저장 실패: {e}")


def main():
    print("=" * 60)
    print("Unusual Options Activity Scanner - Multi-Index")
    print("=" * 60)

    print("\n[1/4] 스캔 대상 티커 리스트 가져오는 중...")
    tickers, group_map = get_combined_universe()
    print(f"  최종 대상 종목 수: {len(tickers)}개")

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

    # 대형주(S&P500/나스닥100) / 소형주(러셀2000) 그룹으로 나눠서
    # 각각 최대 TOP_N_PER_GROUP개씩 (필터 통과 종목이 적으면 그만큼만)
    df["group"] = df["ticker"].map(group_map).fillna("small_cap")
    large_cap_top = df[df["group"] == "large_cap"].sort_values("score", ascending=False).head(TOP_N_PER_GROUP)
    small_cap_top = df[df["group"] == "small_cap"].sort_values("score", ascending=False).head(TOP_N_PER_GROUP)
    top = pd.concat([large_cap_top, small_cap_top], ignore_index=True)

    print(f"  대형주(S&P500/나스닥100) 조건 통과: {len(large_cap_top)}개 선정")
    print(f"  소형주(러셀2000) 조건 통과: {len(small_cap_top)}개 선정")

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

    write_homepage_data(top)

    print("\n[5/5] 텔레그램 알림 전송 중...")
    message = format_telegram_message(top)
    send_telegram_message(message)

    generate_promo_assets(top)

    print("\n[성과 추적] 오늘 플래그 기록 및 체크포인트 성과 확인 중...")
    record_new_flags(top)
    update_performance_tracking()
    if TELEGRAM_ADMIN_CHAT_ID:
        summary = build_performance_summary()
        send_telegram_message_to(TELEGRAM_ADMIN_CHAT_ID, summary)

        digest = build_daily_social_digest()
        if digest:
            send_telegram_message_to(TELEGRAM_ADMIN_CHAT_ID, digest)

    print("완료.")


if __name__ == "__main__":
    main()
