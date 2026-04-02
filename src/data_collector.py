from pathlib import Path
from typing import Any, List, Dict
import time

import requests
import pandas as pd

BASE_URL = "https://fapi.binance.com"

# .py 파일로 실행할 때
BASE_DIR = Path(__file__).resolve().parent.parent

# 만약 주피터/코랩이면 위 줄 대신 아래 사용
# BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def request_json(endpoint: str, params=None):
    """
    바이낸스 API에 GET 요청을 보내고 JSON 응답을 반환한다.
    """
    url = f"{BASE_URL}{endpoint}"
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def get_funding_rate_history(
    symbol: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int = 1000,
    sleep_sec: float = 0.2
) -> pd.DataFrame:
    """
    funding rate history를 가져오는 함수.
    이력을 limit 단위로 페이징하여 수집한다.
    """
    all_rows: List[Dict[str, Any]] = []
    current_start = start_time_ms

    while current_start <= end_time_ms:
        params = {
            "symbol": symbol,
            "startTime": current_start,
            "endTime": end_time_ms,
            "limit": limit
        }

        rows = request_json("/fapi/v1/fundingRate", params)

        if not rows:
            break

        all_rows.extend(rows)
        last_funding_time = rows[-1]["fundingTime"]

        # 같은 값 중복 조회 방지
        next_start = last_funding_time + 1

        if next_start <= current_start:
            break

        current_start = next_start
        time.sleep(sleep_sec)

    df = pd.DataFrame(all_rows)

    if df.empty:
        return df

    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["fundingRate"] = df["fundingRate"].astype(float)

    if "markPrice" in df.columns:
        df["markPrice"] = df["markPrice"].astype(float)

    df = (
        df.drop_duplicates(subset=["fundingTime"])
          .sort_values("fundingTime")
          .reset_index(drop=True)
    )

    return df


def get_klines(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int = 1500,
    sleep_sec: float = 0.2,
) -> pd.DataFrame:
    """
    캔들 데이터를 페이징하여 모두 수집한다.
    """
    all_rows: List[List[Any]] = []
    current_start = start_time_ms

    while current_start <= end_time_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_time_ms,
            "limit": limit,
        }

        rows = request_json("/fapi/v1/klines", params=params)

        if not rows:
            break

        all_rows.extend(rows)

        last_open_time = rows[-1][0]
        next_start = last_open_time + 1

        if next_start <= current_start:
            break

        current_start = next_start
        time.sleep(sleep_sec)

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    df = pd.DataFrame(all_rows, columns=columns)

    if df.empty:
        return df

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df["number_of_trades"] = df["number_of_trades"].astype(int)

    for col in numeric_cols:
        df[col] = df[col].astype(float)

    df = (
        df.drop_duplicates(subset=["open_time"])
          .sort_values("open_time")
          .reset_index(drop=True)
    )

    return df


def to_ms(date_str: str) -> int:
    """
    'YYYY-MM-DD' 문자열을 UTC 기준 ms timestamp로 변환
    """
    return int(pd.Timestamp(date_str, tz="UTC").timestamp() * 1000)


def main() -> None:
    symbol = "BTCUSDT"

    start_date = "2025-01-01"
    end_date = "2025-06-01"

    start_ms = to_ms(start_date)
    end_ms = to_ms(end_date)

    print("[1/2] funding rate 수집 중...")
    funding_df = get_funding_rate_history(
        symbol=symbol,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
    )

    funding_path = DATA_DIR / "btcusdt_funding_rate.csv"
    funding_df.to_csv(funding_path, index=False)
    print(f"저장 완료: {funding_path} / rows={len(funding_df)}")

    print("[2/2] 1시간봉 kline 수집 중...")
    kline_df = get_klines(
        symbol=symbol,
        interval="1h",
        start_time_ms=start_ms,
        end_time_ms=end_ms,
    )

    kline_path = DATA_DIR / "btcusdt_1h_klines.csv"
    kline_df.to_csv(kline_path, index=False)
    print(f"저장 완료: {kline_path} / rows={len(kline_df)}")

    print("전체 완료.")


if __name__ == "__main__":
    main()