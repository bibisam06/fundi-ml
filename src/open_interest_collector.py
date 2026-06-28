"""
FUNDI Project - Open Interest Collector
=======================================
Binance USD-M Futures의 Open Interest 통계를 수집한다.

주의:
  - Binance의 open interest history 엔드포인트는 최근 약 30일 범위만 제공된다.
  - 따라서 장기 데이터셋 전체를 완전히 채우는 용도보다는,
    최근 구간 분석을 위한 보조 피처 수집기로 사용한다.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

BASE_URL = "https://fapi.binance.com"
SYMBOL = "BTCUSDT"
PERIOD = "1h"
LOOKBACK_DAYS = 30

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


def now_ms() -> int:
    return int(pd.Timestamp.utcnow().timestamp() * 1000)


def request_json(
    endpoint: str,
    params: dict[str, Any] | None = None,
    retries: int = 5,
    sleep_sec: float = 0.2,
) -> Any:
    """재시도 로직이 포함된 GET 요청."""
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


def period_to_ms(period: str) -> int:
    mapping = {
        "5m": 5 * 60 * 1000,
        "15m": 15 * 60 * 1000,
        "30m": 30 * 60 * 1000,
        "1h": 60 * 60 * 1000,
        "2h": 2 * 60 * 60 * 1000,
        "4h": 4 * 60 * 60 * 1000,
        "6h": 6 * 60 * 60 * 1000,
        "12h": 12 * 60 * 60 * 1000,
        "1d": 24 * 60 * 60 * 1000,
    }
    if period not in mapping:
        raise ValueError(f"Unsupported period: {period}")
    return mapping[period]


def get_open_interest_history(
    symbol: str,
    period: str = PERIOD,
    lookback_days: int = LOOKBACK_DAYS,
    limit: int = 500,
) -> pd.DataFrame:
    """
    /futures/data/openInterestHist 에서 최근 구간 Open Interest 통계를 수집한다.
    """
    end_ms = now_ms()
    start_ms = end_ms - lookback_days * 24 * 60 * 60 * 1000
    step_ms = period_to_ms(period)

    all_rows: list[dict[str, Any]] = []
    current = start_ms

    log.info(f"[Open Interest] 수집 시작: {symbol} | period={period} | lookback={lookback_days}d")

    while current <= end_ms:
        params = {
            "symbol": symbol,
            "period": period,
            "startTime": current,
            "endTime": end_ms,
            "limit": limit,
        }
        rows = request_json("/futures/data/openInterestHist", params=params)
        if not rows:
            break

        all_rows.extend(rows)
        last_timestamp = int(rows[-1]["timestamp"])
        next_start = last_timestamp + step_ms
        if next_start <= current:
            break
        current = next_start

    if not all_rows:
        log.warning("[Open Interest] 수집 결과가 비어 있습니다.")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["sumOpenInterest"] = pd.to_numeric(df["sumOpenInterest"], errors="coerce")
    df["sumOpenInterestValue"] = pd.to_numeric(df["sumOpenInterestValue"], errors="coerce")

    df = (
        df.drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    log.info(f"[Open Interest] 완료: 총 {len(df):,}개")
    return df


def main() -> None:
    df = get_open_interest_history(SYMBOL)
    output_path = DATA_DIR / "btcusdt_open_interest_hist.csv"
    df.to_csv(output_path, index=False)
    log.info(f"[저장] {output_path} ({len(df):,}행)")
    log.info("✅ Open Interest 수집 완료!")


if __name__ == "__main__":
    main()
