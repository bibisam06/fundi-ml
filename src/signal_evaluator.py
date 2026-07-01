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
        "open_interest",
        "open_interest_value",
        "future_return_1h",
        "future_return_4h",
        "future_return_8h",
        "funding_rate_diff",
        "funding_rate_ma3",
        "open_interest_diff",
        "open_interest_pct_change",
        "oi_volume_ratio",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("time").reset_index(drop=True)


def add_common_features(
    df: pd.DataFrame,
    fit_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """평가에 필요한 방향성과 보조 피처를 추가한다.

    rolling_vol_24h/trend_return_24h는 df 자신의 연속된 시계열로 계산한다(causal rolling).
    regime/oi_regime을 나누는 median 임계값은 fit_df가 있으면 fit_df(보통 train) 기준으로
    고정하고, 없으면 df 자신 기준으로 계산한다 (기존 동작과 동일, 하위호환).

    주의: train/test로 자른 뒤 이 함수를 각각 호출하지 말 것 — 자르기 전에 한 번 호출해서
    rolling 값이 경계에서 NaN이 되는 걸 피해야 한다. (build_train_test_report 참고)
    """
    result = df.copy()
    result["future_direction_4h"] = np.where(
        result["future_return_4h"] > 0,
        1,
        np.where(result["future_return_4h"] < 0, -1, 0),
    )
    result["rolling_vol_24h"] = result["close"].pct_change().rolling(24).std()
    result["trend_return_24h"] = result["close"].pct_change(24)

    if fit_df is not None:
        vol_median = fit_df["close"].pct_change().rolling(24).std().median()
    else:
        vol_median = result["rolling_vol_24h"].median()

    result["regime"] = np.select(
        [
            (result["trend_return_24h"] >= 0) & (result["rolling_vol_24h"] >= vol_median),
            (result["trend_return_24h"] >= 0) & (result["rolling_vol_24h"] < vol_median),
            (result["trend_return_24h"] < 0) & (result["rolling_vol_24h"] >= vol_median),
        ],
        [
            "bull_high_vol",
            "bull_low_vol",
            "bear_high_vol",
        ],
        default="bear_low_vol",
    )

    if "open_interest_pct_change" in result.columns:
        if fit_df is not None and "open_interest_pct_change" in fit_df.columns:
            oi_threshold = fit_df["open_interest_pct_change"].abs().median()
        else:
            oi_threshold = result["open_interest_pct_change"].abs().median()
        result["oi_regime"] = np.select(
            [
                result["open_interest_pct_change"] >= oi_threshold,
                result["open_interest_pct_change"] <= -oi_threshold,
            ],
            [
                "oi_rising",
                "oi_falling",
            ],
            default="oi_stable",
        )
    else:
        result["oi_regime"] = "oi_unavailable"
    return result


def apply_extreme_definition(
    df: pd.DataFrame,
    method: str,
    z_threshold: float = 2.0,
    rolling_window: int = 72,
    quantile: float = 0.1,
    fit_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """세 가지 극단값 정의 방법 중 하나를 적용한다."""
    result = df.copy()
    # fit_source: 임계값(mean/std, quantile)을 "학습"할 데이터.
    # fit_df가 없으면 자기 자신으로 학습 (기존 전체구간 평가와 동일하게 동작, 하위호환).
    fit_source = fit_df if fit_df is not None else result

    if method == "zscore":
        mean = fit_source["funding_rate"].mean()
        std = fit_source["funding_rate"].std(ddof=0)
        result["signal_score"] = (result["funding_rate"] - mean) / std if std else np.nan
        result["is_extreme_pos"] = result["signal_score"] >= z_threshold
        result["is_extreme_neg"] = result["signal_score"] <= -z_threshold

    elif method == "rolling_zscore":
        # 직전 window(72시간)만 보는 trailing 통계라 구조적으로 causal.
        # fit_df를 따로 줄 필요가 없어 result 자신의 rolling 값을 그대로 사용한다.
        rolling_mean = result["funding_rate"].rolling(rolling_window).mean()
        rolling_std = result["funding_rate"].rolling(rolling_window).std(ddof=0)
        result["signal_score"] = (result["funding_rate"] - rolling_mean) / rolling_std.replace(0, np.nan)
        result["is_extreme_pos"] = result["signal_score"] >= z_threshold
        result["is_extreme_neg"] = result["signal_score"] <= -z_threshold

    elif method == "quantile":
        upper = fit_source["funding_rate"].quantile(1 - quantile)
        lower = fit_source["funding_rate"].quantile(quantile)
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
            "best_oi_regime": np.nan,
            "best_oi_regime_accuracy": np.nan,
        }

    regime_scores = (
        events.groupby("regime")["direction_correct"]
        .mean()
        .sort_values(ascending=False)
    )
    oi_regime_scores = (
        events.groupby("oi_regime")["direction_correct"]
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
        "best_oi_regime": oi_regime_scores.index[0] if not oi_regime_scores.empty else np.nan,
        "best_oi_regime_accuracy": float(oi_regime_scores.iloc[0]) if not oi_regime_scores.empty else np.nan,
    }


def build_reports(
    df: pd.DataFrame,
    fit_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """세 가지 방법을 모두 적용해 summary와 event-level report를 생성한다.

    fit_df를 주면 극단값 임계값을 fit_df로 학습해서 df에 적용한다 (train -> test 평가용).
    fit_df가 없으면 기존처럼 df 자신으로 임계값을 학습한다 (전체구간 평가, 하위호환).
    """
    evaluated_frames: list[pd.DataFrame] = []
    summaries: list[dict[str, object]] = []

    for method in ("zscore", "rolling_zscore", "quantile"):
        evaluated = apply_extreme_definition(df, method=method, fit_df=fit_df)
        evaluated_frames.append(evaluated)
        summaries.append(summarise_method(evaluated))

    all_events = pd.concat(evaluated_frames, ignore_index=True)
    all_events = all_events[all_events["is_extreme"]].copy()
    summary_df = pd.DataFrame(summaries).sort_values("directional_accuracy", ascending=False)
    return summary_df, all_events


def build_train_test_report(
    df: pd.DataFrame,
    split_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """split_date 기준 train/test out-of-sample 비교 리포트를 만든다.

    - regime/oi_regime 임계값과 extreme 정의 임계값(mean/std, quantile) 모두
      train 구간으로만 학습한 뒤, 그 고정값을 train/test 양쪽에 동일하게 적용한다.
    - rolling_zscore는 trailing window라 구조적으로 causal이라 별도 처리가 필요 없다.
    """
    raw_train = df[df["time"] < split_date]

    # rolling 피처는 끊기지 않은 전체 시계열로 계산 (경계 NaN 방지),
    # regime을 가르는 median 임계값만 train 구간 기준으로 고정.
    featured = add_common_features(df, fit_df=raw_train)

    train_df = featured[featured["time"] < split_date].reset_index(drop=True)
    test_df = featured[featured["time"] >= split_date].reset_index(drop=True)

    summaries: list[dict[str, object]] = []
    event_frames: list[pd.DataFrame] = []

    for method in ("zscore", "rolling_zscore", "quantile"):
        # mean/std, quantile 임계값도 train_df로만 학습해서 test_df에 그대로 적용
        train_eval = apply_extreme_definition(train_df, method=method, fit_df=train_df)
        test_eval = apply_extreme_definition(test_df, method=method, fit_df=train_df)

        train_summary = summarise_method(train_eval)
        train_summary["split"] = "train"
        test_summary = summarise_method(test_eval)
        test_summary["split"] = "test"
        summaries.extend([train_summary, test_summary])

        event_frames.append(train_eval.assign(split="train"))
        event_frames.append(test_eval.assign(split="test"))

    comparison_df = pd.DataFrame(summaries)
    comparison_df = comparison_df[
        ["method", "split"] + [c for c in comparison_df.columns if c not in ("method", "split")]
    ].sort_values(["method", "split"]).reset_index(drop=True)

    all_events = pd.concat(event_frames, ignore_index=True)
    all_events = all_events[all_events["is_extreme"]].copy()

    return comparison_df, all_events


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


def print_train_test_summary(comparison_df: pd.DataFrame, split_date: pd.Timestamp) -> None:
    """method별 train accuracy와 test accuracy를 나란히 비교 출력한다."""
    print(f"\nFUNDI Out-of-Sample Evaluation (split={split_date.date()})")
    print("=" * 80)
    for method in comparison_df["method"].unique():
        sub = comparison_df[comparison_df["method"] == method]
        train_row = sub[sub["split"] == "train"].iloc[0]
        test_row = sub[sub["split"] == "test"].iloc[0]
        train_acc = "nan" if pd.isna(train_row.directional_accuracy) else f"{train_row.directional_accuracy:.2%}"
        test_acc = "nan" if pd.isna(test_row.directional_accuracy) else f"{test_row.directional_accuracy:.2%}"
        print(
            f"{method:>15} | train acc={train_acc:>8} (n={train_row.extreme_events:>5}) | "
            f" test acc={test_acc:>8} (n={test_row.extreme_events:>5})"
        )
    print("=" * 80)


def main() -> None:
    df = load_dataset()

    # 1) 전체 구간 평가 (기존 리포트, 동작 유지)
    full_df = add_common_features(df)
    summary_df, events_df = build_reports(full_df)

    summary_path = REPORTS_DIR / "signal_summary.csv"
    events_path = REPORTS_DIR / "signal_events.csv"

    summary_df.to_csv(summary_path, index=False)
    events_df.to_csv(events_path, index=False)

    print_console_summary(summary_df)
    print(f"Saved summary to {summary_path}")
    print(f"Saved events to {events_path}")

    # 2) train/test out-of-sample 평가 (신규)
    split_date = pd.Timestamp("2024-01-01", tz="UTC")
    traintest_summary_df, traintest_events_df = build_train_test_report(df, split_date)

    traintest_summary_path = REPORTS_DIR / "signal_summary_traintest.csv"
    traintest_events_path = REPORTS_DIR / "signal_events_traintest.csv"

    traintest_summary_df.to_csv(traintest_summary_path, index=False)
    traintest_events_df.to_csv(traintest_events_path, index=False)

    print_train_test_summary(traintest_summary_df, split_date)
    print(f"Saved train/test summary to {traintest_summary_path}")
    print(f"Saved train/test events to {traintest_events_path}")


if __name__ == "__main__":
    main()