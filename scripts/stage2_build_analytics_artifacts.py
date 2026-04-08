import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd


CALIB_DAYS = 7
FORECAST_HORIZONS = list(range(1, 11))

TARGET_LAGS = [
    1,
    2,
    3,
    4,
    6,
    8,
    12,
    24,
    48,
    96,
    144,
    192,
    240,
    288,
    336,
    672,
    1008,
    1344,
]
TARGET_ROLL_WINDOWS = [2, 4, 8, 16, 48, 96]


def get_train_window_days(horizon: int) -> int:
    if horizon <= 3:
        return 28
    if horizon <= 7:
        return 42
    return 56


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def discover_artifact_root() -> Path:
    preferred = [
        Path("model_artifacts/status_semantics_export_bundle_artifacts"),
        Path("model_artifacts/stable_export_bundle_artifacts"),
    ]
    discovered = list(sorted(Path("model_artifacts").glob("*_artifacts")))

    for candidate in preferred + discovered:
        if (candidate / "feature_schema.json").exists():
            return candidate

    raise FileNotFoundError(
        "Could not find artifact root under model_artifacts/*_artifacts. "
        "Pass --artifact-root explicitly."
    )


def metric(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[float, float, float]:
    y_true = np.asarray(y_true, dtype="float64")
    y_pred = np.clip(np.asarray(y_pred, dtype="float64"), 0, None)

    denom = float(np.sum(y_true))
    if denom <= 0:
        return float("nan"), float("nan"), float("nan")

    wape = float(np.sum(np.abs(y_pred - y_true)) / denom)
    rbias = float(abs(np.sum(y_pred) / denom - 1.0))
    return wape + rbias, wape, rbias


def ensure_columns(df: pd.DataFrame, required: list[str], context: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {context}: {missing}")


def prepare_feature_frame(train: pd.DataFrame, enable_status_semantics: bool = False) -> pd.DataFrame:
    feat = train.copy()
    feat["timestamp"] = pd.to_datetime(feat["timestamp"])
    feat = feat.sort_values(["route_id", "timestamp"]).reset_index(drop=True)

    status_cols = [col for col in feat.columns if col.startswith("status_")]

    feat["hour"] = feat["timestamp"].dt.hour.astype("int8")
    feat["minute"] = feat["timestamp"].dt.minute.astype("int8")
    feat["dow"] = feat["timestamp"].dt.dayofweek.astype("int8")
    feat["status_sum"] = feat[status_cols].sum(axis=1).astype("float32")
    feat["status_max"] = feat[status_cols].max(axis=1).astype("float32")

    route_group = feat.groupby("route_id", sort=False)

    if enable_status_semantics:
        feat["status_early_sum"] = feat[["status_1", "status_2", "status_3"]].sum(axis=1).astype(
            "float32"
        )
        feat["status_mid_sum"] = feat[["status_4", "status_5"]].sum(axis=1).astype("float32")
        feat["status_late_sum"] = feat[["status_6", "status_7", "status_8"]].sum(axis=1).astype(
            "float32"
        )
        feat["status_ready_sum"] = feat[["status_7", "status_8"]].sum(axis=1).astype("float32")
        feat["status_pre_ship_sum"] = feat[
            ["status_5", "status_6", "status_7", "status_8"]
        ].sum(axis=1).astype("float32")

        status_total_safe = feat["status_sum"].clip(lower=1.0)
        feat["status_early_share"] = (feat["status_early_sum"] / status_total_safe).astype("float32")
        feat["status_mid_share"] = (feat["status_mid_sum"] / status_total_safe).astype("float32")
        feat["status_late_share"] = (feat["status_late_sum"] / status_total_safe).astype("float32")
        feat["status_ready_share"] = (feat["status_ready_sum"] / status_total_safe).astype("float32")
        feat["status_pre_ship_share"] = (
            feat["status_pre_ship_sum"] / status_total_safe
        ).astype("float32")
        feat["status_weighted_stage"] = (
            (
                feat["status_1"]
                + 2 * feat["status_2"]
                + 3 * feat["status_3"]
                + 4 * feat["status_4"]
                + 5 * feat["status_5"]
                + 6 * feat["status_6"]
                + 7 * feat["status_7"]
                + 8 * feat["status_8"]
            )
            / status_total_safe
        ).astype("float32")
        feat["status_late_minus_early"] = (
            feat["status_late_sum"] - feat["status_early_sum"]
        ).astype("float32")
        feat["status_ship_pressure"] = (
            (feat["status_6"] + 2 * feat["status_7"] + 3 * feat["status_8"]) / status_total_safe
        ).astype("float32")

        for col_name in [
            "status_late_sum",
            "status_ready_sum",
            "status_weighted_stage",
            "status_ship_pressure",
        ]:
            feat[f"{col_name}_lag_1"] = route_group[col_name].shift(1).astype("float32")
            feat[f"{col_name}_delta_1"] = (feat[col_name] - feat[f"{col_name}_lag_1"]).astype(
                "float32"
            )
            feat[f"{col_name}_roll_mean_4"] = (
                route_group[col_name]
                .rolling(4)
                .mean()
                .reset_index(level=0, drop=True)
                .astype("float32")
            )

    for lag in TARGET_LAGS:
        feat[f"target_lag_{lag}"] = route_group["target_2h"].shift(lag).astype("float32")

    for window in TARGET_ROLL_WINDOWS:
        feat[f"target_roll_mean_{window}"] = (
            route_group["target_2h"]
            .rolling(window)
            .mean()
            .reset_index(level=0, drop=True)
            .astype("float32")
        )
        feat[f"target_roll_std_{window}"] = (
            route_group["target_2h"]
            .rolling(window)
            .std()
            .reset_index(level=0, drop=True)
            .astype("float32")
        )

    lag_7d_cols = [f"target_lag_{lag}" for lag in [48, 96, 144, 192, 240, 288, 336]]
    feat["same_slot_mean_7d"] = feat[lag_7d_cols].mean(axis=1).astype("float32")
    feat["base_blend_roll48_same7d"] = (
        feat[["target_roll_mean_48", "same_slot_mean_7d"]].mean(axis=1).astype("float32")
    )
    feat["baseline_same_4w"] = (
        feat[[f"target_lag_{lag}" for lag in [336, 672, 1008, 1344]]]
        .mean(axis=1)
        .astype("float32")
    )

    return feat


def load_stage1_official(stage1_by_horizon_path: Path) -> pd.DataFrame:
    official = pd.read_csv(stage1_by_horizon_path)
    ensure_columns(
        official,
        [
            "horizon",
            "eval_rows",
            "strong_score",
            "strong_wape",
            "strong_rbias",
            "primitive_score",
            "primitive_wape",
            "primitive_rbias",
            "ml_wape",
            "ml_rbias",
        ],
        context=str(stage1_by_horizon_path),
    )

    official = official.copy()
    official["horizon"] = official["horizon"].astype(int)
    if "ml_score" not in official.columns:
        official["ml_score"] = official["ml_wape"].astype(float) + official["ml_rbias"].abs().astype(
            float
        )

    official["ml_vs_strong_delta_score"] = official["ml_score"] - official["strong_score"]
    official["ml_vs_primitive_delta_score"] = official["ml_score"] - official["primitive_score"]
    return official.sort_values("horizon").reset_index(drop=True)


def make_office_scale_lookup(office_scales_path: Path) -> dict[int, pd.Series]:
    if not office_scales_path.exists():
        return {}

    office_scales = pd.read_parquet(office_scales_path)
    if office_scales.empty:
        return {}

    ensure_columns(office_scales, ["horizon", "office_from_id", "scale"], context=str(office_scales_path))

    office_scales = office_scales.copy()
    office_scales["horizon"] = office_scales["horizon"].astype(int)
    office_scales["office_from_id"] = office_scales["office_from_id"].astype(int)

    lookup: dict[int, pd.Series] = {}
    for horizon in sorted(office_scales["horizon"].unique()):
        part = office_scales[office_scales["horizon"] == horizon]
        lookup[int(horizon)] = part.set_index("office_from_id")["scale"].astype(float)
    return lookup


def aggregate_proxy_stats(
    office_ids: np.ndarray,
    y_true: np.ndarray,
    abs_ml: np.ndarray,
    abs_strong: np.ndarray,
    horizon: int,
) -> tuple[dict, pd.DataFrame]:
    gap = abs_ml - abs_strong
    loss = np.clip(gap, 0, None)
    gain = np.clip(-gap, 0, None)
    win = (gap < 0).astype(np.int32)
    tie = np.isclose(gap, 0.0, atol=1e-9).astype(np.int32)

    horizon_record = {
        "horizon": int(horizon),
        "proxy_rows": int(y_true.size),
        "proxy_y_sum": float(np.sum(y_true)),
        "proxy_wins": int(np.sum(win)),
        "proxy_ties": int(np.sum(tie)),
        "proxy_ml_abs_err_sum": float(np.sum(abs_ml)),
        "proxy_strong_abs_err_sum": float(np.sum(abs_strong)),
        "proxy_gap_abs_err_sum": float(np.sum(gap)),
        "proxy_loss_abs_sum": float(np.sum(loss)),
        "proxy_gain_abs_sum": float(np.sum(gain)),
    }

    office_df = pd.DataFrame(
        {
            "office_from_id": office_ids.astype(np.int64),
            "y_true": y_true,
            "abs_ml": abs_ml,
            "abs_strong": abs_strong,
            "gap": gap,
            "loss": loss,
            "gain": gain,
            "win": win,
            "tie": tie,
        }
    )

    grouped = (
        office_df.groupby("office_from_id", as_index=False)
        .agg(
            rows=("y_true", "size"),
            y_sum=("y_true", "sum"),
            wins=("win", "sum"),
            ties=("tie", "sum"),
            ml_abs_err_sum=("abs_ml", "sum"),
            strong_abs_err_sum=("abs_strong", "sum"),
            gap_abs_err_sum=("gap", "sum"),
            loss_abs_sum=("loss", "sum"),
            gain_abs_sum=("gain", "sum"),
        )
        .copy()
    )

    grouped["horizon"] = int(horizon)
    grouped["win_rate"] = grouped["wins"] / grouped["rows"]
    grouped["tie_rate"] = grouped["ties"] / grouped["rows"]
    grouped["ml_mean_abs_err"] = grouped["ml_abs_err_sum"] / grouped["rows"]
    grouped["strong_mean_abs_err"] = grouped["strong_abs_err_sum"] / grouped["rows"]
    grouped["delta_abs_err_mean"] = grouped["gap_abs_err_sum"] / grouped["rows"]
    grouped["expected_loss_abs_mean"] = grouped["loss_abs_sum"] / grouped["rows"]
    grouped["expected_gain_abs_mean"] = grouped["gain_abs_sum"] / grouped["rows"]
    grouped["expected_loss_wape_like"] = np.where(
        grouped["y_sum"] > 0, grouped["loss_abs_sum"] / grouped["y_sum"], np.nan
    )
    grouped["expected_gain_wape_like"] = np.where(
        grouped["y_sum"] > 0, grouped["gain_abs_sum"] / grouped["y_sum"], np.nan
    )

    cols = [
        "office_from_id",
        "horizon",
        "rows",
        "y_sum",
        "wins",
        "ties",
        "win_rate",
        "tie_rate",
        "ml_mean_abs_err",
        "strong_mean_abs_err",
        "delta_abs_err_mean",
        "expected_loss_abs_mean",
        "expected_gain_abs_mean",
        "expected_loss_wape_like",
        "expected_gain_wape_like",
        "ml_abs_err_sum",
        "strong_abs_err_sum",
        "gap_abs_err_sum",
        "loss_abs_sum",
        "gain_abs_sum",
    ]
    return horizon_record, grouped[cols]


def build_proxy_tables(
    feat: pd.DataFrame,
    artifact_root: Path,
    base_col: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    model_dir = artifact_root / "models"
    calib_dir = artifact_root / "calibration"
    office_scale_lookup = make_office_scale_lookup(calib_dir / "office_scales.parquet")

    train_end = feat["timestamp"].max()
    calib_start = train_end - pd.Timedelta(days=CALIB_DAYS) + pd.Timedelta(minutes=30)
    route_target = feat.groupby("route_id", sort=False)["target_2h"]

    horizon_records: list[dict] = []
    office_rows: list[pd.DataFrame] = []

    for horizon in FORECAST_HORIZONS:
        meta_path = calib_dir / f"h{horizon:02d}_meta.json"
        if not meta_path.exists():
            raise FileNotFoundError(f"Calibration metadata not found: {meta_path}")

        meta = read_json(meta_path)
        model_features = list(meta["feature_cols"])
        categorical_cols = list(meta.get("categorical_cols", []))
        future_feature_names = list(
            meta.get(
                "future_feature_names",
                [f"future_hour_{horizon}", f"future_minute_{horizon}", f"future_dow_{horizon}"],
            )
        )
        if len(future_feature_names) != 3:
            raise ValueError(
                f"Unexpected future feature names for horizon={horizon}: {future_feature_names}"
            )

        future_ts = feat["timestamp"] + pd.Timedelta(minutes=30 * horizon)
        feat[future_feature_names[0]] = future_ts.dt.hour.astype("int8")
        feat[future_feature_names[1]] = future_ts.dt.minute.astype("int8")
        feat[future_feature_names[2]] = future_ts.dt.dayofweek.astype("int8")

        y_future = route_target.shift(-horizon).astype("float32")
        fit_start = train_end - pd.Timedelta(days=get_train_window_days(horizon))
        mask = (
            (feat["timestamp"] >= fit_start)
            & (feat["timestamp"] >= calib_start)
            & y_future.notna()
            & feat[base_col].notna()
            & feat["baseline_same_4w"].notna()
        )

        if not bool(mask.any()):
            horizon_records.append(
                {
                    "horizon": int(horizon),
                    "proxy_rows": 0,
                    "proxy_y_sum": 0.0,
                    "proxy_wins": 0,
                    "proxy_ties": 0,
                    "proxy_ml_abs_err_sum": 0.0,
                    "proxy_strong_abs_err_sum": 0.0,
                    "proxy_gap_abs_err_sum": 0.0,
                    "proxy_loss_abs_sum": 0.0,
                    "proxy_gain_abs_sum": 0.0,
                }
            )
            continue

        required_model_cols = sorted(set(model_features + ["office_from_id"]))
        calib_df = feat.loc[mask, required_model_cols].copy()
        y_true = y_future.loc[mask].to_numpy(dtype="float64")
        strong_pred = feat.loc[mask, base_col].to_numpy(dtype="float64")

        for col in categorical_cols:
            if col in calib_df.columns:
                calib_df[col] = calib_df[col].astype("category")

        model_path = model_dir / str(meta["model_path"])
        booster = lgb.Booster(model_file=str(model_path))
        resid_pred = booster.predict(calib_df[model_features])
        raw_pred = np.clip(strong_pred + resid_pred, 0, None)

        post_scale = float(meta.get("post_scale", 1.0))
        global_scale = float(meta.get("global_scale", 1.0))
        calib_mode = str(meta.get("calib_mode", "global"))

        if calib_mode == "office_shrink":
            office_map = office_scale_lookup.get(horizon)
            if office_map is None:
                row_scale = np.full(raw_pred.shape[0], global_scale, dtype="float64")
            else:
                row_scale = (
                    calib_df["office_from_id"]
                    .astype("int64")
                    .map(office_map)
                    .fillna(global_scale)
                    .to_numpy(dtype="float64")
                )
            ml_pred = raw_pred * row_scale * post_scale
        else:
            ml_pred = raw_pred * post_scale

        ml_pred = np.clip(ml_pred, 0, None)
        abs_ml = np.abs(ml_pred - y_true)
        abs_strong = np.abs(strong_pred - y_true)

        horizon_record, office_grouped = aggregate_proxy_stats(
            office_ids=calib_df["office_from_id"].to_numpy(),
            y_true=y_true,
            abs_ml=abs_ml,
            abs_strong=abs_strong,
            horizon=horizon,
        )
        horizon_records.append(horizon_record)
        office_rows.append(office_grouped)

    horizon_proxy_df = pd.DataFrame(horizon_records).sort_values("horizon").reset_index(drop=True)
    office_proxy_df = (
        pd.concat(office_rows, ignore_index=True).sort_values(["office_from_id", "horizon"]).reset_index(drop=True)
        if office_rows
        else pd.DataFrame()
    )
    return horizon_proxy_df, office_proxy_df


def build_summary_payload(
    stage1_summary_path: Path | None,
    official_df: pd.DataFrame,
    horizon_merged_df: pd.DataFrame,
    output_files: dict[str, str],
) -> dict:
    ml_mean = float(official_df["ml_score"].mean())
    strong_mean = float(official_df["strong_score"].mean())
    primitive_mean = float(official_df["primitive_score"].mean())

    total_rows = int(horizon_merged_df["proxy_rows"].sum())
    total_y_sum = float(horizon_merged_df["proxy_y_sum"].sum())
    total_wins = float(horizon_merged_df["proxy_wins"].sum())
    total_ties = float(horizon_merged_df["proxy_ties"].sum())
    total_loss_abs = float(horizon_merged_df["proxy_loss_abs_sum"].sum())
    total_gain_abs = float(horizon_merged_df["proxy_gain_abs_sum"].sum())
    total_gap_abs = float(horizon_merged_df["proxy_gap_abs_err_sum"].sum())

    stage1_summary = read_json(stage1_summary_path) if stage1_summary_path and stage1_summary_path.exists() else None

    summary_payload = {
        "generated_at_utc": now_utc_iso(),
        "stage": "stage2_analytics_artifacts",
        "official_metric_block": {
            "metric_name": "WAPE + |Relative Bias|",
            "variants": [
                "ml",
                "strong_baseline_blend_roll48_same7d",
                "primitive_baseline_same_4w",
            ],
            "macro_mean_score": {
                "ml": ml_mean,
                "strong_baseline_blend_roll48_same7d": strong_mean,
                "primitive_baseline_same_4w": primitive_mean,
            },
            "relative_improvement_pct": {
                "ml_vs_strong_baseline": (
                    float((strong_mean - ml_mean) / strong_mean * 100.0) if strong_mean > 0 else np.nan
                ),
                "ml_vs_primitive_baseline": (
                    float((primitive_mean - ml_mean) / primitive_mean * 100.0)
                    if primitive_mean > 0
                    else np.nan
                ),
            },
            "note": "Official block is sourced from stage1 offline evaluation only.",
        },
        "proxy_metric_block": {
            "scope": "Calibration-window rows, per office x horizon. Auxiliary only.",
            "comparison": "ML vs strong baseline (blend_roll48_same7d)",
            "definitions": {
                "win_rate": "share of rows where |error_ml| < |error_strong|",
                "tie_rate": "share of rows where |error_ml| == |error_strong|",
                "expected_loss_abs_mean": "mean(max(|error_ml| - |error_strong|, 0)) in target units",
                "delta_abs_err_mean": "mean(|error_ml| - |error_strong|), negative means ML better",
                "expected_loss_wape_like": "sum(max(|error_ml|-|error_strong|,0)) / sum(y_true)",
            },
            "global_aggregate": {
                "rows": total_rows,
                "sum_y_true": total_y_sum,
                "win_rate": float(total_wins / total_rows) if total_rows > 0 else np.nan,
                "tie_rate": float(total_ties / total_rows) if total_rows > 0 else np.nan,
                "expected_loss_abs_mean": float(total_loss_abs / total_rows) if total_rows > 0 else np.nan,
                "expected_gain_abs_mean": float(total_gain_abs / total_rows) if total_rows > 0 else np.nan,
                "delta_abs_err_mean": float(total_gap_abs / total_rows) if total_rows > 0 else np.nan,
                "expected_loss_wape_like": float(total_loss_abs / total_y_sum) if total_y_sum > 0 else np.nan,
            },
            "note": "Proxy block is not the competition metric and must not be shown as official KPI.",
        },
        "sources": {
            "stage1_by_horizon": output_files["stage1_by_horizon_source"],
            "stage1_summary": output_files.get("stage1_summary_source"),
            "model_artifact_root": output_files["artifact_root_source"],
        },
        "output_files": output_files,
    }
    if stage1_summary is not None:
        summary_payload["stage1_snapshot"] = stage1_summary
    return summary_payload


def office_proxy_matrix_payload(office_proxy_df: pd.DataFrame) -> dict:
    if office_proxy_df.empty:
        return {"horizons": FORECAST_HORIZONS, "rows": []}

    rows = []
    for office_id, group in office_proxy_df.groupby("office_from_id", sort=False):
        group = group.sort_values("horizon")
        cells: dict[str, dict] = {}
        for row in group.itertuples(index=False):
            cells[f"h{int(row.horizon)}"] = {
                "rows": int(row.rows),
                "win_rate": float(row.win_rate),
                "tie_rate": float(row.tie_rate),
                "delta_abs_err_mean": float(row.delta_abs_err_mean),
                "expected_loss_abs_mean": float(row.expected_loss_abs_mean),
                "expected_loss_wape_like": float(row.expected_loss_wape_like),
            }
        rows.append(
            {
                "office_from_id": int(office_id),
                "rows_total": int(group["rows"].sum()),
                "cells": cells,
            }
        )

    rows.sort(key=lambda item: item["rows_total"], reverse=True)
    return {"horizons": FORECAST_HORIZONS, "rows": rows}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build stage2 analytics artifacts from stage1 offline eval and model artifacts."
    )
    parser.add_argument(
        "--train-path",
        type=Path,
        default=Path("train_team_track.parquet"),
        help="Path to training parquet.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=None,
        help="Path to model artifacts root from train.py.",
    )
    parser.add_argument(
        "--stage1-by-horizon-path",
        type=Path,
        default=Path("model/offline_eval_stage1_by_horizon.csv"),
        help="Path to stage1 per-horizon official comparison.",
    )
    parser.add_argument(
        "--stage1-summary-path",
        type=Path,
        default=Path("model/offline_eval_stage1_summary.json"),
        help="Path to stage1 summary json.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("model"),
        help="Output directory for stage2 artifacts.",
    )
    args = parser.parse_args()

    if args.artifact_root is None:
        args.artifact_root = discover_artifact_root()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    train = pd.read_parquet(args.train_path)
    ensure_columns(
        train,
        ["route_id", "office_from_id", "timestamp", "target_2h"],
        context=str(args.train_path),
    )
    feature_schema_path = args.artifact_root / "feature_schema.json"
    feature_schema = read_json(feature_schema_path)
    enable_status_semantics = bool(feature_schema.get("enable_status_semantics", False))

    feat = prepare_feature_frame(train, enable_status_semantics=enable_status_semantics)
    official_df = load_stage1_official(args.stage1_by_horizon_path)

    base_col = str(feature_schema.get("base_col", "base_blend_roll48_same7d"))

    horizon_proxy_df, office_proxy_df = build_proxy_tables(
        feat=feat,
        artifact_root=args.artifact_root,
        base_col=base_col,
    )

    horizon_merged = official_df.merge(horizon_proxy_df, on="horizon", how="left").sort_values("horizon")
    horizon_merged["proxy_win_rate"] = np.where(
        horizon_merged["proxy_rows"] > 0,
        horizon_merged["proxy_wins"] / horizon_merged["proxy_rows"],
        np.nan,
    )
    horizon_merged["proxy_tie_rate"] = np.where(
        horizon_merged["proxy_rows"] > 0,
        horizon_merged["proxy_ties"] / horizon_merged["proxy_rows"],
        np.nan,
    )
    horizon_merged["proxy_expected_loss_abs_mean"] = np.where(
        horizon_merged["proxy_rows"] > 0,
        horizon_merged["proxy_loss_abs_sum"] / horizon_merged["proxy_rows"],
        np.nan,
    )
    horizon_merged["proxy_delta_abs_err_mean"] = np.where(
        horizon_merged["proxy_rows"] > 0,
        horizon_merged["proxy_gap_abs_err_sum"] / horizon_merged["proxy_rows"],
        np.nan,
    )
    horizon_merged["proxy_expected_loss_wape_like"] = np.where(
        horizon_merged["proxy_y_sum"] > 0,
        horizon_merged["proxy_loss_abs_sum"] / horizon_merged["proxy_y_sum"],
        np.nan,
    )

    by_horizon_path = args.output_dir / "analytics_stage2_by_horizon.csv"
    by_horizon_json_path = args.output_dir / "analytics_stage2_by_horizon.json"
    office_proxy_path = args.output_dir / "analytics_stage2_office_horizon_proxy.csv"
    office_proxy_json_path = args.output_dir / "analytics_stage2_office_horizon_proxy.json"
    summary_path = args.output_dir / "analytics_stage2_summary.json"
    manifest_path = args.output_dir / "analytics_stage2_manifest.json"

    horizon_merged.to_csv(by_horizon_path, index=False)
    by_horizon_json_path.write_text(
        json.dumps(
            {
                "generated_at_utc": now_utc_iso(),
                "rows": horizon_merged.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    office_proxy_df.to_csv(office_proxy_path, index=False)
    office_proxy_json_path.write_text(
        json.dumps(
            {
                "generated_at_utc": now_utc_iso(),
                "matrix": office_proxy_matrix_payload(office_proxy_df),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    output_files = {
        "summary": str(summary_path),
        "by_horizon_csv": str(by_horizon_path),
        "by_horizon_json": str(by_horizon_json_path),
        "office_horizon_proxy_csv": str(office_proxy_path),
        "office_horizon_proxy_json": str(office_proxy_json_path),
        "stage1_by_horizon_source": str(args.stage1_by_horizon_path),
        "stage1_summary_source": str(args.stage1_summary_path),
        "artifact_root_source": str(args.artifact_root),
    }
    summary_payload = build_summary_payload(
        stage1_summary_path=args.stage1_summary_path,
        official_df=official_df,
        horizon_merged_df=horizon_merged,
        output_files=output_files,
    )
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    manifest_path.write_text(
        json.dumps(
            {
                "generated_at_utc": now_utc_iso(),
                "stage": "stage2_analytics_artifacts",
                "official_metric_block_source": str(args.stage1_by_horizon_path),
                "proxy_metric_block_source": "calibration window recalculation via saved model artifacts",
                "files": {
                    "summary": str(summary_path),
                    "by_horizon_csv": str(by_horizon_path),
                    "by_horizon_json": str(by_horizon_json_path),
                    "office_horizon_proxy_csv": str(office_proxy_path),
                    "office_horizon_proxy_json": str(office_proxy_json_path),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("Stage 2 analytics artifacts completed.")
    print(f"Summary: {summary_path.resolve()}")
    print(f"By horizon: {by_horizon_path.resolve()}")
    print(f"Office x horizon proxy: {office_proxy_path.resolve()}")


if __name__ == "__main__":
    main()
