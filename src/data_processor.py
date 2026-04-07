"""
FUNDI Project - Data Processor
================================
raw 데이터를 전처리하고 분석용 데이터셋을 생성한다.

출력 파일:
  1. btcusdt_funding_event_merged.csv  — funding 발생 시점(8h)만 병합
  2. btcusdt_1h_with_funding.csv       — 1h 전체 + 최신 funding asof 병합 (메인 분석용)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

# ──────────────────────────────────────────────────────────────
# 경로 설정
# ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# 1. 로드
# ──────────────────────────────────────────────────────────────
def load_raw_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """raw CSV 파일을 불러온다."""
    funding_path = RAW_DIR / "btcusdt_funding_rate.csv"
    kline_path = RAW_DIR / "btcusdt_1h_klines.csv"

    funding_df = pd.read_csv(funding_path)
    kline_df = pd.read_csv(kline_path)

    log.info(f"[로드] funding: {len(funding_df):,}행 | kline: {len(kline_df):,}행")
    return funding_df, kline_df


# ──────────────────────────────────────────────────────────────
# 2. 전처리
# ──────────────────────────────────────────────────────────────
def preprocess_funding_data(funding_df: pd.DataFrame) -> pd.DataFrame:
    """
    Funding Rate 전처리:
      - datetime 변환 (UTC aware, 시간 단위 floor)
      - 숫자형 변환
      - null 제거 → 중복 제거 → 정렬
      - 컬럼명 통일
    """
    df = funding_df.copy()

    # datetime: UTC aware로 통일
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], errors="coerce", utc=True)
    df["fundingTime"] = df["fundingTime"].dt.floor("h")

    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    if "markPrice" in df.columns:
        df["markPrice"] = pd.to_numeric(df["markPrice"], errors="coerce")

    # time key null 제거 (merge 전 필수)
    before = len(df)
    df = df.dropna(subset=["fundingTime"])
    if len(df) < before:
        log.warning(f"[Funding] null time 제거: {before - len(df)}행")

    df = df.rename(columns={
        "fundingTime": "time",
        "fundingRate": "funding_rate",
        "markPrice": "mark_price",
    })

    df = (
        df.drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    log.info(f"[Funding] 전처리 완료: {len(df):,}행")
    return df


def preprocess_kline_data(kline_df: pd.DataFrame) -> pd.DataFrame:
    """
    Kline 전처리:
      - datetime 변환 (UTC aware)
      - 숫자형 변환
      - 컬럼명 통일 (open_time → time)
      - 중복 제거 → 정렬
    """
    df = kline_df.copy()

    # open_time 컬럼명 방어 처리
    if "pen_time" in df.columns:
        df = df.rename(columns={"pen_time": "open_time"})

    # datetime: UTC aware로 통일
    df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce", utc=True)
    df["open_time"] = df["open_time"].dt.floor("h")  # 혹시 모를 서브분 제거

    if "close_time" in df.columns:
        df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce", utc=True)

    numeric_cols = [
        "open", "high", "low", "close", "volume",
        "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "number_of_trades" in df.columns:
        df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce")

    df = df.rename(columns={"open_time": "time"})

    df = (
        df.drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    log.info(f"[Kline] 전처리 완료: {len(df):,}행")
    return df


# ──────────────────────────────────────────────────────────────
# 3. 병합
# ──────────────────────────────────────────────────────────────
def merge_funding_events(
    funding_df: pd.DataFrame,
    kline_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Funding 이벤트 시점(8h)과 정확히 일치하는 kline만 inner join.
    → funding 발생 시점 분석용 (행 수 = funding 이벤트 수)
    """
    df = pd.merge(funding_df, kline_df, on="time", how="inner")
    df = (
        df.drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )
    log.info(f"[Event Merge] {len(df):,}행 (funding 이벤트 기준)")
    return df


def merge_asof_funding(
    kline_df: pd.DataFrame,
    funding_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    1시간봉 전체에 대해 가장 최근 funding rate를 붙인다. (메인 분석용)
    merge_asof(direction='backward') → 각 kline 시점 이전 마지막 funding 사용.

    ※ inner join을 쓰면 funding 없는 1h 캔들(전체의 87.5%)이 사라지므로 사용 금지.
    """
    left = kline_df.sort_values("time").copy()
    right = funding_df[["time", "funding_rate", "mark_price"]].sort_values("time").copy() \
        if "mark_price" in funding_df.columns \
        else funding_df[["time", "funding_rate"]].sort_values("time").copy()

    df = pd.merge_asof(left, right, on="time", direction="backward")
    log.info(f"[Asof Merge] {len(df):,}행 (1h 전체 기준)")
    return df


# ──────────────────────────────────────────────────────────────
# 4. 파생변수
# ──────────────────────────────────────────────────────────────
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    분석용 파생변수 추가:
      - 수익률: 현재(1h), 미래(1h / 4h / 8h)
      - Funding Rate: 변화량(diff), 3기간 이동평균
    """
    result = df.copy()

    result["return_1h"] = result["close"].pct_change()

    result["future_return_1h"] = result["close"].shift(-1) / result["close"] - 1
    result["future_return_4h"] = result["close"].shift(-4) / result["close"] - 1
    result["future_return_8h"] = result["close"].shift(-8) / result["close"] - 1

    if "funding_rate" in result.columns:
        result["funding_rate_diff"] = result["funding_rate"].diff()
        result["funding_rate_ma3"] = result["funding_rate"].rolling(3).mean()

    return result


# ──────────────────────────────────────────────────────────────
# 5. 검증
# ──────────────────────────────────────────────────────────────
def validate(df: pd.DataFrame, name: str) -> None:
    """기본 품질 검증 출력"""
    print(f"\n{'='*50}")
    print(f"📊 {name}")
    print(f"{'='*50}")
    print(f"기간      : {df['time'].min()} → {df['time'].max()}")
    print(f"총 행 수  : {len(df):,}행")
    print(f"결측치    :\n{df.isna().sum()[df.isna().sum() > 0].to_string() or '  없음'}")

    # 1h 연속성 체크 (asof merged 데이터 대상)
    gaps = df["time"].diff().dropna()
    expected = pd.Timedelta("1h")
    irregular = gaps[gaps != expected]
    if not irregular.empty:
        print(f"⚠️  비정상 시간 간격 {len(irregular)}개 발견")
        print(irregular.value_counts().head())
    else:
        print("✅ 시계열 연속성 OK")
    print(f"{'='*50}\n")


# ──────────────────────────────────────────────────────────────
# 6. 저장
# ──────────────────────────────────────────────────────────────
def save_data(df: pd.DataFrame, file_name: str) -> None:
    path = PROCESSED_DIR / file_name
    df.to_csv(path, index=False)
    log.info(f"[저장] {path} ({len(df):,}행, {df.shape[1]}열)")


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main() -> None:
    log.info("=" * 50)
    log.info("FUNDI Data Processor 시작")
    log.info("=" * 50)

    # 로드
    funding_df, kline_df = load_raw_data()

    # 전처리
    funding_df = preprocess_funding_data(funding_df)
    kline_df = preprocess_kline_data(kline_df)

    # 병합 1: funding 이벤트 시점만
    event_df = merge_funding_events(funding_df, kline_df)
    event_df = add_features(event_df)
    save_data(event_df, "btcusdt_funding_event_merged.csv")
    validate(event_df, "Funding Event Merged (8h 기준)")

    # 병합 2: 1h 전체 + asof funding (메인 분석용)
    asof_df = merge_asof_funding(kline_df, funding_df)
    asof_df = add_features(asof_df)
    save_data(asof_df, "btcusdt_1h_with_funding.csv")
    validate(asof_df, "1h Full + Asof Funding (메인 분석용)")

    log.info("✅ 처리 완료!")


if __name__ == "__main__":
    main()
