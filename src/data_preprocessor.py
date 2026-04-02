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
    #데이터확인용프린트
    #print(funding_df, kline_df)

    return funding_df, kline_df

def processing_funding_data(funding_df : pd.DataFrame) -> pd.DataFrame:
    """
        funding rate 데이터 전처리
        """
    df = funding_df.copy()

    # datetime 변환
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], errors="coerce")
    df["fundingTime"] = df["fundingTime"].dt.floor("h")
    # 숫자형 변환
    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")

    if "markPrice" in df.columns:
        df["markPrice"] = pd.to_numeric(df["markPrice"], errors="coerce")

    # 컬럼명 통일
    df = df.rename(columns={
        "fundingTime": "time",
        "fundingRate": "funding_rate",
        "markPrice": "mark_price"
    })

    # 정렬 및 중복 제거
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

    # datetime 변환
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], utc=True)

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

    # 컬럼명 맞추기
    df = df.rename(columns={
        "open_time": "time"
    })

    # 정렬 및 중복 제거
    df = (
        df.drop_duplicates(subset=["time"])
          .sort_values("time")
          .reset_index(drop=True)
    )

    return df
def merge_data(funding_df : pd.DataFrame, kline_df : pd.DataFrame) -> pd.DataFrame:
    """
    funding rate와 1시간봉 kline 데이터 합치기 및 전처리
    """
    df = pd.merge(funding_df, kline_df, on="time", how="inner")

    print(funding_df["time"].head(10).tolist())
    print(kline_df["time"].head(10).tolist())

    print(funding_df["time"].dtype)
    print(kline_df["time"].dtype)

    df = (
        df.drop_duplicates(subset=["time"])
        .sort_values("time")
        .reset_index(drop=True)
    )

    return df

def save_data(df: pd.DataFrame, file_name:str) -> None:
    """
    데이터저장
    """
    data_path = PROCESSED_DIR / file_name
    df.to_csv(data_path, index=False)
    print(f"Data saved to {data_path}")


def main() -> None:
    print("Start processing data...")

    funding_df, kline_df = load_raw_data()

    funding_df = processing_funding_data(funding_df)
    kline_df = preprocess_kline_data(kline_df)

    merged_df = merge_data(funding_df, kline_df)

    save_data(merged_df, "btcusdt_merged.csv")

    print("End processing data...")


if __name__ == "__main__":
    main()