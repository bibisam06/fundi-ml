# 데이터 전처리를 담당하는 파이썬 파일입니다
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def load_raw_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    raw 데이터(csv)를 불러온다.
    """
    funding_path = RAW_DIR / "btcusdt_funding_rate.csv"
    kline_path = RAW_DIR / "btcusdt_1h_klines.csv"

    funding_df = pd.read_csv(funding_path)
    kline_df = pd.read_csv(kline_path)

    return funding_df, kline_df


def preprocess_funding_data(funding_df: pd.DataFrame) -> pd.DataFrame:
    """
    funding rate 데이터 전처리
    """
    df = funding_df.copy()

    df["fundingTime"] = pd.to_datetime(df["fundingTime"], errors="coerce")
    df["fundingTime"] = df["fundingTime"].dt.floor("h")

    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")

    if "markPrice" in df.columns:
        df["markPrice"] = pd.to_numeric(df["markPrice"], errors="coerce")

    # 핵심: time key null 제거 전, 원본 컬럼 기준으로 먼저 제거해도 됨
    df = df.dropna(subset=["fundingTime"])

    df = df.rename(columns={
        "fundingTime": "time",
        "fundingRate": "funding_rate",
        "markPrice": "mark_price"
    })

    df = (
        df.drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    return df

def preprocess_kline_data(kline_df: pd.DataFrame) -> pd.DataFrame:
    """
    1시간봉 kline 데이터 전처리
    """
    df = kline_df.copy()

    # 혹시 pen_time으로 깨진 경우 방어
    if "pen_time" in df.columns:
        df = df.rename(columns={"pen_time": "open_time"})

    df["open_time"] = pd.to_datetime(df["open_time"], errors="coerce")
    df["open_time"] = df["open_time"].dt.floor("h")

    df["close_time"] = pd.to_datetime(df["close_time"], errors="coerce")

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

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "number_of_trades" in df.columns:
        df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce")

    df = df.rename(columns={"open_time": "time"})

    df = (
        df.drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    return df


def merge_funding_events(funding_df: pd.DataFrame, kline_df: pd.DataFrame) -> pd.DataFrame:
    """
    funding 발생 시점과 정확히 일치하는 kline만 병합
    """
    df = pd.merge(funding_df, kline_df, on="time", how="inner")

    df = (
        df.drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    return df


def merge_asof_funding(kline_df: pd.DataFrame, funding_df: pd.DataFrame) -> pd.DataFrame:
    """
    각 1시간봉에 대해 가장 최근 funding 데이터를 붙인다.
    """
    left = kline_df.sort_values("time").copy()
    right = funding_df.sort_values("time").copy()

    df = pd.merge_asof(
        left,
        right,
        on="time",
        direction="backward"
    )

    return df


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    분석용 파생 변수 추가
    """
    result = df.copy()

    # 현재 봉 수익률
    result["return_1h"] = result["close"].pct_change()

    # 미래 수익률
    result["future_return_1h"] = result["close"].shift(-1) / result["close"] - 1
    result["future_return_4h"] = result["close"].shift(-4) / result["close"] - 1
    result["future_return_8h"] = result["close"].shift(-8) / result["close"] - 1

    # funding 변화량
    if "funding_rate" in result.columns:
        result["funding_rate_diff"] = result["funding_rate"].diff()
        result["funding_rate_ma3"] = result["funding_rate"].rolling(3).mean()

    return result


def save_data(df: pd.DataFrame, file_name: str) -> None:
    """
    데이터 저장
    """
    data_path = PROCESSED_DIR / file_name
    df.to_csv(data_path, index=False)
    print(f"Data saved to {data_path} / rows={len(df)}")


def main() -> None:
    print("Start processing data...")

    funding_df, kline_df = load_raw_data()

    funding_df = preprocess_funding_data(funding_df)
    kline_df = preprocess_kline_data(kline_df)

    print(f"funding rows: {len(funding_df)}")
    print(f"kline rows: {len(kline_df)}")

    # 1) funding 이벤트 시점만 병합
    event_merged_df = merge_funding_events(funding_df, kline_df)
    event_merged_df = add_features(event_merged_df)
    save_data(event_merged_df, "btcusdt_funding_event_merged.csv")

    # 2) 1시간봉 전체 + 최신 funding 붙이기
    asof_merged_df = merge_asof_funding(kline_df, funding_df)
    asof_merged_df = add_features(asof_merged_df)
    save_data(asof_merged_df, "btcusdt_1h_with_funding.csv")

    print("End processing data...")


if __name__ == "__main__":
    main()