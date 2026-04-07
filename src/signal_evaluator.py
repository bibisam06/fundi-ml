"""
FUNDI Project - Signal Evaluator
================================
계획서의 핵심 연구 질문 중 일부를 현재 processed 데이터셋에서 바로 검증할 수 있도록
극단 funding rate 정의와 4시간 방향성 지표를 계산한다.

입력:
  - data/processed/btcusdt_1h_with_funding.csv

출력:
  - reports/signal_summary.csv
  - reports/signal_events.csv
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = BASE_DIR / "data" / "processed" / "btcusdt_1h_with_funding.csv"
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def load_dataset(path: Path = INPUT_PATH) -> pd.DataFrame:
    """분석용 1시간 데이터셋을 로드하고 기본 타입을 정리한다."""
    df = pd.read_csv(path)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

    numeric_cols = [
        "close",
        "funding_rate",
        "future_return_1h",
        "future_return_4h",
        "future_return_8h",
        "funding_rate_diff",
        "funding_rate_ma3",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("time").reset_index(drop=True)


def add_common_features(df: pd.DataFrame) -> pd.DataFrame:
    """평가에 필요한 방향성과 보조 피처를 추가한다."""
    result = df.copy()
    result["future_direction_4h"] = np.where(
        result["future_return_4h"] > 0,
        1,
        np.where(result["future_return_4h"] < 0, -1, 0),
    )
    result["rolling_vol_24h"] = result["close"].pct_change().rolling(24).std()
    result["trend_return_24h"] = result["close"].pct_change(24)
    result["regime"] = np.select(
        [
            (result["trend_return_24h"] >= 0) & (result["rolling_vol_24h"] >= result["rolling_vol_24h"].median()),
            (result["trend_return_24h"] >= 0) & (result["rolling_vol_24h"] < result["rolling_vol_24h"].median()),
            (result["trend_return_24h"] < 0) & (result["rolling_vol_24h"] >= result["rolling_vol_24h"].median()),
        ],
        [
            "bull_high_vol",
            "bull_low_vol",
            "bear_high_vol",
        ],
        default="bear_low_vol",
    )
    return result


def apply_extreme_definition(
    df: pd.DataFrame,
    method: str,
    z_threshold: float = 2.0,
    rolling_window: int = 72,
    quantile: float = 0.1,
) -> pd.DataFrame:
    """세 가지 극단값 정의 방법 중 하나를 적용한다."""
    result = df.copy()

    if method == "zscore":
        mean = result["funding_rate"].mean()
        std = result["funding_rate"].std(ddof=0)
        result["signal_score"] = (result["funding_rate"] - mean) / std if std else np.nan
        result["is_extreme_pos"] = result["signal_score"] >= z_threshold
        result["is_extreme_neg"] = result["signal_score"] <= -z_threshold

    elif method == "rolling_zscore":
        rolling_mean = result["funding_rate"].rolling(rolling_window).mean()
        rolling_std = result["funding_rate"].rolling(rolling_window).std(ddof=0)
        result["signal_score"] = (result["funding_rate"] - rolling_mean) / rolling_std.replace(0, np.nan)
        result["is_extreme_pos"] = result["signal_score"] >= z_threshold
        result["is_extreme_neg"] = result["signal_score"] <= -z_threshold

    elif method == "quantile":
        upper = result["funding_rate"].quantile(1 - quantile)
        lower = result["funding_rate"].quantile(quantile)
        result["signal_score"] = result["funding_rate"]
        result["is_extreme_pos"] = result["funding_rate"] >= upper
        result["is_extreme_neg"] = result["funding_rate"] <= lower

    else:
        raise ValueError(f"Unsupported method: {method}")

    result["predicted_direction"] = np.select(
        [result["is_extreme_pos"], result["is_extreme_neg"]],
        [-1, 1],
        default=0,
    )
    result["is_extreme"] = result["predicted_direction"] != 0
    result["direction_correct"] = (
        (result["predicted_direction"] != 0)
        & (result["predicted_direction"] == result["future_direction_4h"])
    )
    result["method"] = method
    return result


def summarise_method(df: pd.DataFrame) -> dict[str, object]:
    """방법별 핵심 성과 지표를 요약한다."""
    events = df[df["is_extreme"]].copy()
    pos_events = events[events["predicted_direction"] == -1]
    neg_events = events[events["predicted_direction"] == 1]

    if events.empty:
        return {
            "method": df["method"].iloc[0],
            "extreme_events": 0,
            "pos_extreme_events": 0,
            "neg_extreme_events": 0,
            "directional_accuracy": np.nan,
            "avg_future_return_4h": np.nan,
            "avg_future_return_4h_pos_extreme": np.nan,
            "avg_future_return_4h_neg_extreme": np.nan,
            "best_regime": np.nan,
            "best_regime_accuracy": np.nan,
        }

    regime_scores = (
        events.groupby("regime")["direction_correct"]
        .mean()
        .sort_values(ascending=False)
    )

    return {
        "method": df["method"].iloc[0],
        "extreme_events": int(len(events)),
        "pos_extreme_events": int(len(pos_events)),
        "neg_extreme_events": int(len(neg_events)),
        "directional_accuracy": float(events["direction_correct"].mean()),
        "avg_future_return_4h": float(events["future_return_4h"].mean()),
        "avg_future_return_4h_pos_extreme": float(pos_events["future_return_4h"].mean()) if not pos_events.empty else np.nan,
        "avg_future_return_4h_neg_extreme": float(neg_events["future_return_4h"].mean()) if not neg_events.empty else np.nan,
        "best_regime": regime_scores.index[0] if not regime_scores.empty else np.nan,
        "best_regime_accuracy": float(regime_scores.iloc[0]) if not regime_scores.empty else np.nan,
    }


def build_reports(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """세 가지 방법을 모두 적용해 summary와 event-level report를 생성한다."""
    evaluated_frames: list[pd.DataFrame] = []
    summaries: list[dict[str, object]] = []

    for method in ("zscore", "rolling_zscore", "quantile"):
        evaluated = apply_extreme_definition(df, method=method)
        evaluated_frames.append(evaluated)
        summaries.append(summarise_method(evaluated))

    all_events = pd.concat(evaluated_frames, ignore_index=True)
    all_events = all_events[all_events["is_extreme"]].copy()
    summary_df = pd.DataFrame(summaries).sort_values("directional_accuracy", ascending=False)
    return summary_df, all_events


def print_console_summary(summary_df: pd.DataFrame) -> None:
    """터미널에서 바로 읽기 좋은 형태로 상위 결과를 출력한다."""
    print("\nFUNDI Signal Evaluation")
    print("=" * 60)
    for row in summary_df.itertuples(index=False):
        accuracy = "nan" if pd.isna(row.directional_accuracy) else f"{row.directional_accuracy:.2%}"
        avg_return = "nan" if pd.isna(row.avg_future_return_4h) else f"{row.avg_future_return_4h:.4%}"
        print(
            f"{row.method:>15} | events={row.extreme_events:>5} | "
            f"accuracy={accuracy:>8} | avg_4h_return={avg_return:>9} | "
            f"best_regime={row.best_regime}"
        )
    print("=" * 60)


def main() -> None:
    df = load_dataset()
    df = add_common_features(df)
    summary_df, events_df = build_reports(df)

    summary_path = REPORTS_DIR / "signal_summary.csv"
    events_path = REPORTS_DIR / "signal_events.csv"

    summary_df.to_csv(summary_path, index=False)
    events_df.to_csv(events_path, index=False)

    print_console_summary(summary_df)
    print(f"Saved summary to {summary_path}")
    print(f"Saved events to {events_path}")


if __name__ == "__main__":
    main()
