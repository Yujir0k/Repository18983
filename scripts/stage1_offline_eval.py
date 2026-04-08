import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


CALIB_DAYS = 7
FORECAST_HORIZONS = list(range(1, 11))


def get_train_window_days(horizon: int) -> int:
    if horizon <= 3:
        return 28
    if horizon <= 7:
        return 42
    return 56


def metric(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float]:
    y_true = np.asarray(y_true, dtype="float64")
    y_pred = np.clip(np.asarray(y_pred, dtype="float64"), 0, None)

    denom = float(np.sum(y_true))
    if denom <= 0:
        return float("nan"), float("nan"), float("nan")

    wape = float(np.sum(np.abs(y_pred - y_true)) / denom)
    rbias = float(abs(np.sum(y_pred) / denom - 1.0))
    return wape + rbias, wape, rbias


def first_existing_path(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


@dataclass
class BaselineMetrics:
    horizon: int
    rows: int
    strong_score: float
    strong_wape: float
    strong_rbias: float
    primitive_score: float
    primitive_wape: float
    primitive_rbias: float


def prepare_baseline_features(train: pd.DataFrame) -> pd.DataFrame:
    train = train.copy()
    train["timestamp"] = pd.to_datetime(train["timestamp"])
    train = train.sort_values(["route_id", "timestamp"]).reset_index(drop=True)

    route_group = train.groupby("route_id", sort=False)
    lag_values = [48, 96, 144, 192, 240, 288, 336, 672, 1008, 1344]
    for lag in lag_values:
        train[f"target_lag_{lag}"] = route_group["target_2h"].shift(lag).astype("float32")

    train["target_roll_mean_48"] = (
        route_group["target_2h"]
        .rolling(48)
        .mean()
        .reset_index(level=0, drop=True)
        .astype("float32")
    )

    strong_lags = [
        "target_lag_48",
        "target_lag_96",
        "target_lag_144",
        "target_lag_192",
        "target_lag_240",
        "target_lag_288",
        "target_lag_336",
    ]
    primitive_lags = [
        "target_lag_336",
        "target_lag_672",
        "target_lag_1008",
        "target_lag_1344",
    ]

    train["baseline_same_7d"] = train[strong_lags].mean(axis=1).astype("float32")
    train["baseline_blend_roll48_same7d"] = (
        train[["target_roll_mean_48", "baseline_same_7d"]].mean(axis=1).astype("float32")
    )
    train["baseline_same_4w"] = train[primitive_lags].mean(axis=1).astype("float32")

    return train


def evaluate_baselines(train: pd.DataFrame) -> list[BaselineMetrics]:
    train_end = train["timestamp"].max()
    calib_start = train_end - pd.Timedelta(days=CALIB_DAYS) + pd.Timedelta(minutes=30)
    route_group = train.groupby("route_id", sort=False)

    rows: list[BaselineMetrics] = []
    for horizon in FORECAST_HORIZONS:
        y_future = route_group["target_2h"].shift(-horizon).astype("float32")

        fit_start = train_end - pd.Timedelta(days=get_train_window_days(horizon))
        eval_mask = (
            (train["timestamp"] >= fit_start)
            & (train["timestamp"] >= calib_start)
            & y_future.notna()
            & train["baseline_blend_roll48_same7d"].notna()
            & train["baseline_same_4w"].notna()
        )

        y_true = y_future[eval_mask].to_numpy(dtype="float64")
        pred_strong = train.loc[eval_mask, "baseline_blend_roll48_same7d"].to_numpy(
            dtype="float64"
        )
        pred_primitive = train.loc[eval_mask, "baseline_same_4w"].to_numpy(dtype="float64")

        strong_score, strong_wape, strong_rbias = metric(y_true, pred_strong)
        primitive_score, primitive_wape, primitive_rbias = metric(y_true, pred_primitive)

        rows.append(
            BaselineMetrics(
                horizon=horizon,
                rows=int(eval_mask.sum()),
                strong_score=strong_score,
                strong_wape=strong_wape,
                strong_rbias=strong_rbias,
                primitive_score=primitive_score,
                primitive_wape=primitive_wape,
                primitive_rbias=primitive_rbias,
            )
        )

    return rows


def load_ml_metrics(ml_metrics_path: Path) -> pd.DataFrame:
    ml = pd.read_csv(ml_metrics_path)
    ml = ml.copy()
    ml["horizon"] = ml["horizon"].astype(int)
    ml["ml_score"] = ml["wape"].astype(float) + ml["rbias"].abs().astype(float)
    return ml[["horizon", "score", "wape", "rbias", "ml_score"]].rename(
        columns={"score": "ml_score_from_train", "wape": "ml_wape", "rbias": "ml_rbias"}
    )


def write_outputs(
    output_dir: Path,
    ml_metrics_path: Path,
    merged_by_horizon: pd.DataFrame,
    long_metrics: pd.DataFrame,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    by_horizon_path = output_dir / "offline_eval_stage1_by_horizon.csv"
    long_path = output_dir / "offline_eval_stage1_long.csv"
    summary_path = output_dir / "offline_eval_stage1_summary.json"

    merged_by_horizon.to_csv(by_horizon_path, index=False)
    long_metrics.to_csv(long_path, index=False)

    summary_rows = []
    for variant in ["ml", "strong_baseline_blend_roll48_same7d", "primitive_baseline_same_4w"]:
        part = long_metrics[long_metrics["variant"] == variant]
        summary_rows.append(
            {
                "variant": variant,
                "horizons": int(part["horizon"].nunique()),
                "mean_score": float(part["score"].mean()),
                "mean_wape": float(part["wape"].mean()),
                "mean_rbias": float(part["rbias"].mean()),
            }
        )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "stage": "stage1_offline_eval",
        "metric_definition": "WAPE + |Relative Bias|",
        "calibration_days": CALIB_DAYS,
        "horizons": FORECAST_HORIZONS,
        "ml_metrics_source": str(ml_metrics_path),
        "averages_macro": summary_rows,
        "output_files": {
            "by_horizon": str(by_horizon_path),
            "long": str(long_path),
        },
    }

    summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 1 offline comparison for ML vs baselines.")
    parser.add_argument(
        "--train-path",
        type=Path,
        default=Path("train_team_track.parquet"),
        help="Path to train parquet.",
    )
    parser.add_argument(
        "--ml-metrics-path",
        type=Path,
        default=None,
        help="Path to per-horizon ML metrics from train pipeline.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("model"),
        help="Directory where stage1 artifacts will be written.",
    )
    args = parser.parse_args()

    if args.ml_metrics_path is None:
        default_metrics_candidates = [
            Path("model/metrics_by_horizon.csv"),
            Path("model_artifacts/status_semantics_export_bundle_artifacts/reports/metrics_by_horizon.csv"),
            Path("model_artifacts/stable_export_bundle_artifacts/reports/metrics_by_horizon.csv"),
        ]
        resolved_metrics_path = first_existing_path(default_metrics_candidates)
        if resolved_metrics_path is None:
            raise FileNotFoundError(
                "Could not find metrics_by_horizon.csv. "
                "Pass --ml-metrics-path explicitly or place artifacts under model_artifacts/*_artifacts/reports."
            )
        args.ml_metrics_path = resolved_metrics_path

    train = pd.read_parquet(args.train_path, columns=["route_id", "timestamp", "target_2h"])
    train = prepare_baseline_features(train)
    baseline_rows = evaluate_baselines(train)
    ml_metrics = load_ml_metrics(args.ml_metrics_path)

    baseline_df = pd.DataFrame(
        [
            {
                "horizon": row.horizon,
                "eval_rows": row.rows,
                "strong_score": row.strong_score,
                "strong_wape": row.strong_wape,
                "strong_rbias": row.strong_rbias,
                "primitive_score": row.primitive_score,
                "primitive_wape": row.primitive_wape,
                "primitive_rbias": row.primitive_rbias,
            }
            for row in baseline_rows
        ]
    )

    merged = baseline_df.merge(ml_metrics, on="horizon", how="left").sort_values("horizon")

    long_rows = []
    for row in merged.itertuples(index=False):
        long_rows.append(
            {
                "variant": "ml",
                "horizon": int(row.horizon),
                "eval_rows": int(row.eval_rows),
                "score": float(row.ml_score),
                "wape": float(row.ml_wape),
                "rbias": float(abs(row.ml_rbias)),
            }
        )
        long_rows.append(
            {
                "variant": "strong_baseline_blend_roll48_same7d",
                "horizon": int(row.horizon),
                "eval_rows": int(row.eval_rows),
                "score": float(row.strong_score),
                "wape": float(row.strong_wape),
                "rbias": float(row.strong_rbias),
            }
        )
        long_rows.append(
            {
                "variant": "primitive_baseline_same_4w",
                "horizon": int(row.horizon),
                "eval_rows": int(row.eval_rows),
                "score": float(row.primitive_score),
                "wape": float(row.primitive_wape),
                "rbias": float(row.primitive_rbias),
            }
        )

    long_df = pd.DataFrame(long_rows).sort_values(["variant", "horizon"]).reset_index(drop=True)
    write_outputs(args.output_dir, args.ml_metrics_path, merged, long_df)

    print("Stage 1 offline comparison completed.")
    print(f"Artifacts written to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
