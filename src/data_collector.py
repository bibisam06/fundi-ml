"""
FUNDI Project - Data Collector
================================
Binance Futures에서 BTCUSDT 데이터를 수집한다.
  - Funding Rate (8h 원본)
  - 1시간봉 Kline (OHLCV)

수집 범위: 2019-09-08(BTCUSDT Perp 상장일) ~ 현재
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

# ──────────────────────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────────────────────
BASE_URL = "https://fapi.binance.com"
SYMBOL = "BTCUSDT"
START_DATE = "2019-09-08"   # BTCUSDT Perp 최초 상장일

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(BASE_DIR / "collector.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# 공통 유틸
# ──────────────────────────────────────────────────────────────
def to_ms(date_str: str) -> int:
    """'YYYY-MM-DD' → UTC milliseconds"""
    return int(pd.Timestamp(date_str, tz="UTC").timestamp() * 1000)


def now_ms() -> int:
    return int(pd.Timestamp.utcnow().timestamp() * 1000)


def request_json(
    endpoint: str,
    params: dict | None = None,
    retries: int = 5,
    sleep_sec: float = 0.2,
) -> Any:
    """
    재시도 로직이 포함된 GET 요청.
    실패 시 지수 백오프(exponential backoff)로 재시도.
    """
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            time.sleep(sleep_sec)
            return resp.json()
        except requests.RequestException as e:
            wait = 2 ** attempt
            log.warning(f"요청 실패 ({e}), {wait}초 후 재시도 ({attempt + 1}/{retries})")
            time.sleep(wait)
    raise RuntimeError(f"{retries}회 재시도 후 실패: {url}")


# ──────────────────────────────────────────────────────────────
# 1. Funding Rate 수집
# ──────────────────────────────────────────────────────────────
def get_funding_rate_history(
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> pd.DataFrame:

    all_rows: list[dict] = []
    current = start_ms

    log.info(f"[Funding] 수집 시작: {symbol}")

    while current <= end_ms:
        params = {
            "symbol": symbol,
            "startTime": current,
            "endTime": end_ms,
            "limit": limit,
        }

        rows = request_json("/fapi/v1/fundingRate", params)

        if not rows:
            break

        all_rows.extend(rows)

        next_start = rows[-1]["fundingTime"] + 1
        if next_start <= current:
            break

        current = next_start

    log.info(f"[Funding] 완료: 총 {len(all_rows):,}개")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # 시간 변환
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)

    # 숫자 변환 (핵심)
    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")

    if "markPrice" in df.columns:
        df["markPrice"] = pd.to_numeric(df["markPrice"], errors="coerce")
    else:
        df["markPrice"] = pd.NA

    # 로그 확인
    log.info(f"[Funding] fundingRate NaN: {df['fundingRate'].isna().sum():,}")
    log.info(f"[Funding] markPrice NaN: {df['markPrice'].isna().sum():,}")

    # 🔥 핵심: markPrice 제거 안함
    before = len(df)
    df = df.dropna(subset=["fundingTime", "fundingRate"]).copy()
    after = len(df)

    log.info(f"[Funding] 유효 데이터: {after:,} / {before:,}")

    df = (
        df.drop_duplicates(subset=["fundingTime"])
        .sort_values("fundingTime")
        .reset_index(drop=True)
    )

    return df

# ──────────────────────────────────────────────────────────────
# 2. Kline(OHLCV) 수집
# ──────────────────────────────────────────────────────────────
def get_klines(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> pd.DataFrame:
    """
    /fapi/v1/klines 에서 OHLCV 캔들 데이터를 전체 수집.
    한 번에 최대 limit(1500)개 → 페이징 루프.
    """
    all_rows: list[list] = []
    current = start_ms

    log.info(f"[Kline] 수집 시작: {symbol} {interval}")

    while current <= end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current,
            "endTime": end_ms,
            "limit": limit,
        }
        rows = request_json("/fapi/v1/klines", params)
        if not rows:
            break

        all_rows.extend(rows)
        next_start = rows[-1][0] + 1  # open_time + 1ms
        if next_start <= current:
            break
        current = next_start

        log.debug(f"[Kline] {len(all_rows):,}개 수집 중...")

    log.info(f"[Kline] 완료: 총 {len(all_rows):,}개")

    if not all_rows:
        return pd.DataFrame()

    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
    ]
    df = pd.DataFrame(all_rows, columns=columns)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df["number_of_trades"] = df["number_of_trades"].astype(int)

    for col in [
        "open", "high", "low", "close", "volume",
        "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 혹시라도 이상치 있으면 제거
    before_drop = len(df)
    df = df.dropna(
        subset=[
            "open_time", "close_time", "open", "high", "low", "close",
            "volume", "quote_asset_volume", "taker_buy_base_volume",
            "taker_buy_quote_volume"
        ]
    ).copy()
    dropped = before_drop - len(df)

    if dropped > 0:
        log.warning(f"[Kline] 결측/이상치 행 제거: {dropped:,}개")

    df = df.drop(columns=["ignore"])
    df = (
        df.drop_duplicates(subset=["open_time"])
        .sort_values("open_time")
        .reset_index(drop=True)
    )
    return df


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main() -> None:
    start_ms = to_ms(START_DATE)
    end_ms = now_ms()

    log.info("=" * 50)
    log.info("FUNDI Data Collector 시작")
    log.info(f"심볼: {SYMBOL} | 기간: {START_DATE} ~ 현재")
    log.info("=" * 50)

    # Funding Rate
    funding_df = get_funding_rate_history(SYMBOL, start_ms, end_ms)
    funding_path = DATA_DIR / "btcusdt_funding_rate.csv"
    funding_df.to_csv(funding_path, index=False)
    log.info(f"[저장] {funding_path} ({len(funding_df):,}행)")

    # Kline
    kline_df = get_klines(SYMBOL, "1h", start_ms, end_ms)
    kline_path = DATA_DIR / "btcusdt_1h_klines.csv"
    kline_df.to_csv(kline_path, index=False)
    log.info(f"[저장] {kline_path} ({len(kline_df):,}행)")

    log.info("✅ 수집 완료!")


if __name__ == "__main__":
    main()