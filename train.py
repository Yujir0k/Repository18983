import gc
import json
import os
import warnings
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


VARIANT_NAME = os.environ.get("MODEL_VARIANT_NAME", "stable_export_bundle").strip() or "stable_export_bundle"
ENABLE_STATUS_SEMANTICS = _env_bool("ENABLE_STATUS_SEMANTICS", False)

CALIB_DAYS = 7
FORECAST_HORIZONS = list(range(1, 11))
USE_REFIT_FULL = True
ROUND_PREDICTIONS = True
FIXED_BASE_NAME = "blend_roll48_same7d"

TARGET_LAGS = [1, 2, 3, 4, 6, 8, 12, 24, 48, 96, 144, 192, 240, 288, 336, 672]
TARGET_ROLL_WINDOWS = [2, 4, 8, 16, 48, 96]

OFFICE_STRENGTH_GRID = [0.5, 1.0, 2.0, 4.0]
SCALE_CLIP = (0.75, 1.30)
MIN_PRED_SUM = 1.0

SEED = 42
NUM_THREADS = min(4, os.cpu_count() or 4)
HISTORY_TAIL_ROWS = max(TARGET_LAGS)

LGB_PARAMS = {
    "objective": "regression_l1",
    "metric": "l1",
    "learning_rate": 0.05,
    "n_estimators": 7000,
    "num_leaves": 63,
    "min_child_samples": 100,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.0,
    "reg_lambda": 0.0,
    "random_state": SEED,
    "n_jobs": NUM_THREADS,
    "verbosity": -1,
    "force_col_wise": True,
}


def get_train_window_days(horizon: int) -> int:
    if horizon <= 3:
        return 28
    if horizon <= 7:
        return 42
    return 56


def autodiscover_input(filename: str, search_roots: list[Path]) -> Path:
    for root in search_roots:
        candidate = root / filename
        if candidate.exists():
            return candidate
    for root in search_roots:
        matches = list(root.rglob(filename))
        if matches:
            return matches[0]
    raise FileNotFoundError(
        f"File not found in local search roots: {filename}. "
        f"Checked: {[str(r) for r in search_roots]}"
    )


def metric(y_true: np.ndarray, y_pred: np.ndarray):
    y_pred = np.clip(np.asarray(y_pred), 0, None)
    y_true = np.asarray(y_true)
    wape = np.abs(y_pred - y_true).sum() / y_true.sum()
    rbias = abs(y_pred.sum() / y_true.sum() - 1)
    return float(wape + rbias), float(wape), float(rbias)


def find_best_scale(y_true: np.ndarray, y_pred: np.ndarray):
    best_score = np.inf
    best_scale = 1.0
    for scale in np.linspace(0.90, 1.10, 81):
        score, _, _ = metric(y_true, y_pred * scale)
        if score < best_score:
            best_score = score
            best_scale = float(scale)
    return best_scale, float(best_score)


def build_office_scale_map(
    calib_scale_df: pd.DataFrame,
    global_scale: float,
    office_strength_mult: float,
    scale_clip=SCALE_CLIP,
    min_pred_sum=MIN_PRED_SUM,
):
    office_stats = calib_scale_df.groupby("office_from_id", observed=True).agg(
        y_sum=("y_true", "sum"),
        pred_sum=("y_pred_raw", "sum"),
    )

    office_prior = max(float(office_stats["y_sum"].median()) * office_strength_mult, 1.0)
    office_raw = (office_stats["y_sum"] / office_stats["pred_sum"].clip(lower=min_pred_sum)).clip(
        *scale_clip
    )
    office_alpha = office_stats["y_sum"] / (office_stats["y_sum"] + office_prior)
    office_scale = office_alpha * office_raw + (1.0 - office_alpha) * global_scale
    return office_scale.astype("float32")


def fit_lgbm(
    train_df: pd.DataFrame,
    valid_df: pd.DataFrame,
    feature_cols,
    label_col: str,
    categorical_cols,
):
    model = lgb.LGBMRegressor(**LGB_PARAMS)
    model.fit(
        train_df[feature_cols],
        train_df[label_col],
        eval_set=[(valid_df[feature_cols], valid_df[label_col])],
        eval_metric="l1",
        categorical_feature=categorical_cols,
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )
    return model


def refit_full_lgbm(
    full_df: pd.DataFrame,
    feature_cols,
    label_col: str,
    categorical_cols,
    best_iteration: int,
):
    full_params = dict(LGB_PARAMS)
    full_params["n_estimators"] = max(int(best_iteration or LGB_PARAMS["n_estimators"]), 50)
    model = lgb.LGBMRegressor(**full_params)
    model.fit(
        full_df[feature_cols],
        full_df[label_col],
        categorical_feature=categorical_cols,
    )
    return model


def safe_smape(y_true: np.ndarray, y_pred: np.ndarray):
    denom = np.abs(y_true) + np.abs(y_pred) + 1e-6
    return float(np.mean(2.0 * np.abs(y_pred - y_true) / denom))


def evaluation_report(y_true: np.ndarray, y_pred: np.ndarray):
    score, wape, rbias = metric(y_true, y_pred)
    err = y_pred - y_true
    abs_err = np.abs(err)
    return {
        "score": float(score),
        "wape": float(wape),
        "rbias": float(rbias),
        "mae": float(abs_err.mean()),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "smape": safe_smape(y_true, y_pred),
        "mean_error": float(err.mean()),
        "p90_abs_error": float(np.quantile(abs_err, 0.90)),
        "under_capacity_units": float(np.clip(y_true - y_pred, 0, None).sum()),
        "over_capacity_units": float(np.clip(y_pred - y_true, 0, None).sum()),
        "sum_true": float(np.sum(y_true)),
        "sum_pred": float(np.sum(y_pred)),
    }


def to_serializable(obj):
    if isinstance(obj, dict):
        return {str(k): to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_serializable(v) for v in obj]
    if isinstance(obj, tuple):
        return [to_serializable(v) for v in obj]
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    return obj


def save_json(path: Path, payload):
    path.write_text(
        json.dumps(to_serializable(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main():
    project_root = Path(__file__).resolve().parent
    data_dir = Path(os.environ.get("DATA_DIR", project_root))
    workdir = Path(os.environ.get("ARTIFACTS_DIR", project_root / "model_artifacts"))
    workdir.mkdir(parents=True, exist_ok=True)

    search_roots = [data_dir, project_root]
    train_path = autodiscover_input("train_team_track.parquet", search_roots)
    test_path = autodiscover_input("test_team_track.parquet", search_roots)

    submission_path = workdir / "submission.csv"
    variant_submission_path = workdir / f"submission_{VARIANT_NAME}.csv"
    artifact_root = workdir / f"{VARIANT_NAME}_artifacts"
    model_dir = artifact_root / "models"
    report_dir = artifact_root / "reports"
    calib_dir = artifact_root / "calibration"

    for path in [artifact_root, model_dir, report_dir, calib_dir]:
        path.mkdir(parents=True, exist_ok=True)

    train = pd.read_parquet(train_path)
    test = pd.read_parquet(test_path)

    train["timestamp"] = pd.to_datetime(train["timestamp"])
    test["timestamp"] = pd.to_datetime(test["timestamp"])

    train = train.sort_values(["route_id", "timestamp"]).reset_index(drop=True)
    test = test.sort_values(["route_id", "timestamp"]).reset_index(drop=True)

    status_cols = [c for c in train.columns if c.startswith("status_")]
    raw_input_cols = ["route_id", "office_from_id", "timestamp"] + status_cols + ["target_2h"]

    print(f"Running variant: {VARIANT_NAME}")
    feat = train.copy()
    feat["hour"] = feat["timestamp"].dt.hour.astype("int8")
    feat["minute"] = feat["timestamp"].dt.minute.astype("int8")
    feat["dow"] = feat["timestamp"].dt.dayofweek.astype("int8")

    feat["status_sum"] = feat[status_cols].sum(axis=1).astype("float32")
    feat["status_max"] = feat[status_cols].max(axis=1).astype("float32")

    route_group = feat.groupby("route_id", sort=False)

    status_feature_cols = []
    if ENABLE_STATUS_SEMANTICS:
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
                route_group[col_name].rolling(4).mean().reset_index(level=0, drop=True).astype("float32")
            )

        status_feature_cols = [
            "status_early_sum",
            "status_mid_sum",
            "status_late_sum",
            "status_ready_sum",
            "status_pre_ship_sum",
            "status_early_share",
            "status_mid_share",
            "status_late_share",
            "status_ready_share",
            "status_pre_ship_share",
            "status_weighted_stage",
            "status_late_minus_early",
            "status_ship_pressure",
            "status_late_sum_lag_1",
            "status_late_sum_delta_1",
            "status_late_sum_roll_mean_4",
            "status_ready_sum_lag_1",
            "status_ready_sum_delta_1",
            "status_ready_sum_roll_mean_4",
            "status_weighted_stage_lag_1",
            "status_weighted_stage_delta_1",
            "status_weighted_stage_roll_mean_4",
            "status_ship_pressure_lag_1",
            "status_ship_pressure_delta_1",
            "status_ship_pressure_roll_mean_4",
        ]

    for lag in TARGET_LAGS:
        feat[f"target_lag_{lag}"] = route_group["target_2h"].shift(lag).astype("float32")

    for window in TARGET_ROLL_WINDOWS:
        feat[f"target_roll_mean_{window}"] = (
            route_group["target_2h"].rolling(window).mean().reset_index(level=0, drop=True).astype("float32")
        )
        feat[f"target_roll_std_{window}"] = (
            route_group["target_2h"].rolling(window).std().reset_index(level=0, drop=True).astype("float32")
        )

    lag_7d_cols = [f"target_lag_{lag}" for lag in [48, 96, 144, 192, 240, 288, 336]]
    feat["same_slot_mean_7d"] = feat[lag_7d_cols].mean(axis=1).astype("float32")
    feat["base_blend_roll48_same7d"] = (
        feat[["target_roll_mean_48", "same_slot_mean_7d"]].mean(axis=1).astype("float32")
    )

    base_col = "base_blend_roll48_same7d"
    base_feature_cols = [
        "route_id",
        "office_from_id",
        "hour",
        "minute",
        "dow",
        "target_2h",
        "status_sum",
        "status_max",
    ] + status_feature_cols + status_cols + [
        c
        for c in feat.columns
        if c.startswith("target_lag_") or c.startswith("target_roll_") or c == "same_slot_mean_7d"
    ]

    cat_cols = ["route_id", "office_from_id", "hour", "minute", "dow"]

    train_end = feat["timestamp"].max()
    calib_start = train_end - pd.Timedelta(days=CALIB_DAYS) + pd.Timedelta(minutes=30)

    test_source = feat[feat["timestamp"] == train_end].copy()
    assert len(test_source) == test["route_id"].nunique()

    route_target = feat.groupby("route_id", sort=False)["target_2h"]

    history_tail = train.groupby("route_id", sort=False).tail(HISTORY_TAIL_ROWS).copy()
    history_tail[raw_input_cols].to_parquet(artifact_root / "history_tail.parquet", index=False)
    train.loc[train["timestamp"] == train_end, raw_input_cols].to_parquet(
        artifact_root / "latest_snapshot.parquet",
        index=False,
    )

    category_levels = {
        "route_id": sorted(int(v) for v in feat["route_id"].dropna().unique()),
        "office_from_id": sorted(int(v) for v in feat["office_from_id"].dropna().unique()),
        "hour": sorted(int(v) for v in feat["hour"].dropna().unique()),
        "minute": sorted(int(v) for v in feat["minute"].dropna().unique()),
        "dow": sorted(int(v) for v in feat["dow"].dropna().unique()),
    }

    pred_frames = []
    route_calib_frames = []
    metric_by_h = {}
    calib_mode_by_h = {}
    chosen_base_by_h = {}
    horizon_reports = []
    calibration_records = []

    for horizon in FORECAST_HORIZONS:
        print(f"\n===== horizon={horizon} =====")

        train_window_days = get_train_window_days(horizon)
        fit_start = train_end - pd.Timedelta(days=train_window_days)

        target_col = f"y_h{horizon}"
        resid_col = f"resid_h{horizon}"
        future_hour_col = f"future_hour_{horizon}"
        future_minute_col = f"future_minute_{horizon}"
        future_dow_col = f"future_dow_{horizon}"

        y_future = route_target.shift(-horizon).astype("float32")

        future_ts = feat["timestamp"] + pd.Timedelta(minutes=30 * horizon)
        feat[future_hour_col] = future_ts.dt.hour.astype("int8")
        feat[future_minute_col] = future_ts.dt.minute.astype("int8")
        feat[future_dow_col] = future_ts.dt.dayofweek.astype("int8")

        model_features = base_feature_cols + [future_hour_col, future_minute_col, future_dow_col]
        horizon_cat_cols = cat_cols + [future_hour_col, future_minute_col, future_dow_col]

        base_pred = feat[base_col].astype("float32")
        model_mask = (feat["timestamp"] >= fit_start) & y_future.notna() & base_pred.notna()

        model_df = feat.loc[model_mask, model_features + ["timestamp"]].copy()
        model_df["base_pred"] = base_pred.loc[model_mask].to_numpy()
        model_df[target_col] = y_future.loc[model_mask].to_numpy()
        model_df[resid_col] = (model_df[target_col] - model_df["base_pred"]).astype("float32")
        model_df = model_df.dropna().copy()

        fit_df = model_df[model_df["timestamp"] < calib_start].copy()
        calib_df = model_df[model_df["timestamp"] >= calib_start].copy()

        for c in horizon_cat_cols:
            fit_df[c] = fit_df[c].astype("category")
            calib_df[c] = calib_df[c].astype("category")

        model = fit_lgbm(
            train_df=fit_df,
            valid_df=calib_df,
            feature_cols=model_features,
            label_col=resid_col,
            categorical_cols=horizon_cat_cols,
        )

        best_iteration = int(model.best_iteration_ or LGB_PARAMS["n_estimators"])
        raw_valid_resid_pred = model.predict(
            calib_df[model_features],
            num_iteration=model.best_iteration_,
        )
        raw_valid_pred = np.clip(calib_df["base_pred"].to_numpy() + raw_valid_resid_pred, 0, None)
        y_valid = calib_df[target_col].to_numpy()

        global_scale, global_score = find_best_scale(y_valid, raw_valid_pred)
        best_score = float(global_score)
        best_post_scale = float(global_scale)
        best_mode = "global"
        best_office_scale_map = None
        best_office_strength = None

        calib_scale_df = calib_df[["office_from_id"]].copy()
        calib_scale_df["y_true"] = y_valid
        calib_scale_df["y_pred_raw"] = raw_valid_pred

        for office_strength in OFFICE_STRENGTH_GRID:
            office_scale_map = build_office_scale_map(
                calib_scale_df=calib_scale_df,
                global_scale=global_scale,
                office_strength_mult=office_strength,
            )

            row_scale = (
                calib_df["office_from_id"].map(office_scale_map).fillna(global_scale).astype("float32").to_numpy()
            )
            post_scale, shrunk_score = find_best_scale(y_valid, raw_valid_pred * row_scale)

            if shrunk_score < best_score:
                best_score = float(shrunk_score)
                best_post_scale = float(post_scale)
                best_mode = "office_shrink"
                best_office_scale_map = office_scale_map.copy()
                best_office_strength = float(office_strength)

        if best_mode == "office_shrink":
            calib_row_scale = (
                calib_df["office_from_id"].map(best_office_scale_map).fillna(global_scale).astype("float32").to_numpy()
            )
            calib_pred_final = raw_valid_pred * calib_row_scale * best_post_scale
        else:
            calib_pred_final = raw_valid_pred * best_post_scale

        calib_pred_final = np.clip(calib_pred_final, 0, None)

        # Сбор честных калибровочных предсказаний для route-level WAPE.
        calib_res = pd.DataFrame(
            {
                "route_id": calib_df["route_id"].values,
                "y_true": y_valid,
                "y_pred": calib_pred_final,
            }
        )
        route_calib_frames.append(calib_res)

        report = evaluation_report(y_valid, calib_pred_final)
        report.update(
            {
                "variant": VARIANT_NAME,
                "horizon": int(horizon),
                "train_window_days": int(train_window_days),
                "base_name": FIXED_BASE_NAME,
                "calib_mode": best_mode,
                "best_iteration": best_iteration,
                "global_scale": float(global_scale),
                "post_scale": float(best_post_scale),
                "office_strength": best_office_strength,
            }
        )
        horizon_reports.append(report)

        metric_by_h[horizon] = best_score
        calib_mode_by_h[horizon] = best_mode
        chosen_base_by_h[horizon] = FIXED_BASE_NAME

        print(
            f"  -> best base={FIXED_BASE_NAME}, "
            f"calib_metric={best_score:.5f}, "
            f"mode={best_mode}, "
            f"best_iter={best_iteration}"
        )

        for c in horizon_cat_cols:
            model_df[c] = model_df[c].astype("category")

        if USE_REFIT_FULL:
            final_model = refit_full_lgbm(
                full_df=model_df,
                feature_cols=model_features,
                label_col=resid_col,
                categorical_cols=horizon_cat_cols,
                best_iteration=best_iteration,
            )
        else:
            final_model = fit_lgbm(
                train_df=fit_df,
                valid_df=calib_df,
                feature_cols=model_features,
                label_col=resid_col,
                categorical_cols=horizon_cat_cols,
            )

        model_path = model_dir / f"h{horizon:02d}.txt"
        final_model.booster_.save_model(str(model_path))

        if best_office_scale_map is not None:
            office_scale_df = best_office_scale_map.rename("scale").rename_axis("office_from_id").reset_index()
            office_scale_df["horizon"] = int(horizon)
            calibration_records.append(office_scale_df[["horizon", "office_from_id", "scale"]].copy())

        test_h = test_source[base_feature_cols].copy()
        test_future_ts = test_source["timestamp"] + pd.Timedelta(minutes=30 * horizon)
        test_h[future_hour_col] = test_future_ts.dt.hour.astype("int8")
        test_h[future_minute_col] = test_future_ts.dt.minute.astype("int8")
        test_h[future_dow_col] = test_future_ts.dt.dayofweek.astype("int8")

        for c in horizon_cat_cols:
            test_h[c] = test_h[c].astype("category")

        test_base_pred = test_source[base_col].to_numpy()
        test_resid_pred = final_model.predict(
            test_h[model_features],
            num_iteration=getattr(final_model, "best_iteration_", None),
        )

        test_pred = np.clip(test_base_pred + test_resid_pred, 0, None)

        if best_mode == "office_shrink":
            test_row_scale = (
                test_h["office_from_id"].map(best_office_scale_map).fillna(global_scale).astype("float32").to_numpy()
            )
            test_pred = test_pred * test_row_scale * best_post_scale
        else:
            test_pred = test_pred * best_post_scale

        test_pred = np.clip(test_pred, 0, None)

        pred_frames.append(
            pd.DataFrame(
                {
                    "route_id": test_source["route_id"].to_numpy(),
                    "timestamp": test_source["timestamp"] + pd.Timedelta(minutes=30 * horizon),
                    "y_pred": test_pred,
                }
            )
        )

        save_json(
            calib_dir / f"h{horizon:02d}_meta.json",
            {
                "variant": VARIANT_NAME,
                "horizon": int(horizon),
                "base_name": FIXED_BASE_NAME,
                "base_col": base_col,
                "model_path": model_path.name,
                "feature_cols": model_features,
                "categorical_cols": horizon_cat_cols,
                "calib_mode": best_mode,
                "global_scale": float(global_scale),
                "post_scale": float(best_post_scale),
                "office_strength": best_office_strength,
                "best_iteration": best_iteration,
                "train_window_days": int(train_window_days),
                "future_feature_names": [future_hour_col, future_minute_col, future_dow_col],
            },
        )

        del model, final_model, fit_df, calib_df, model_df, test_h, calib_scale_df
        gc.collect()

    forecast_df = pd.concat(pred_frames, ignore_index=True)

    submission = test.merge(
        forecast_df,
        on=["route_id", "timestamp"],
        how="left",
    )[["id", "y_pred"]]

    assert submission["y_pred"].notna().all(), "Some test rows did not receive predictions"

    submission["y_pred"] = submission["y_pred"].clip(lower=0)
    if ROUND_PREDICTIONS:
        submission["y_pred"] = np.round(submission["y_pred"])

    submission.to_csv(submission_path, index=False)
    submission.to_csv(variant_submission_path, index=False)

    metrics_df = pd.DataFrame(horizon_reports).sort_values("horizon").reset_index(drop=True)
    metrics_df.to_csv(report_dir / "metrics_by_horizon.csv", index=False)

    summary_payload = {
        "variant": VARIANT_NAME,
        "mean_calib_metric": float(metrics_df["score"].mean()),
        "mean_wape": float(metrics_df["wape"].mean()),
        "mean_rbias": float(metrics_df["rbias"].mean()),
        "mean_mae": float(metrics_df["mae"].mean()),
        "mean_rmse": float(metrics_df["rmse"].mean()),
        "mean_smape": float(metrics_df["smape"].mean()),
        "total_under_capacity_units": float(metrics_df["under_capacity_units"].sum()),
        "total_over_capacity_units": float(metrics_df["over_capacity_units"].sum()),
        "chosen_base_by_horizon": chosen_base_by_h,
        "calibration_mode_by_horizon": calib_mode_by_h,
    }
    save_json(report_dir / "metrics_summary.json", summary_payload)

    if route_calib_frames:
        all_calib_df = pd.concat(route_calib_frames, ignore_index=True)
        route_wape_series = all_calib_df.groupby("route_id").apply(
            lambda x: float(np.abs(x["y_pred"] - x["y_true"]).sum() / (x["y_true"].sum() + 1e-6))
        )
        save_json(artifact_root / "route_wape_7d.json", route_wape_series.to_dict())
        save_json(report_dir / "route_wape_7d.json", route_wape_series.to_dict())

    if calibration_records:
        office_scales_df = pd.concat(calibration_records, ignore_index=True)
    else:
        office_scales_df = pd.DataFrame(columns=["horizon", "office_from_id", "scale"])
    office_scales_df.to_parquet(calib_dir / "office_scales.parquet", index=False)

    feature_schema = {
        "variant": VARIANT_NAME,
        "enable_status_semantics": ENABLE_STATUS_SEMANTICS,
        "raw_input_cols": raw_input_cols,
        "status_cols": status_cols,
        "base_feature_cols": base_feature_cols,
        "status_feature_cols": status_feature_cols,
        "categorical_cols": cat_cols,
        "category_levels": category_levels,
        "target_lags": TARGET_LAGS,
        "target_roll_windows": TARGET_ROLL_WINDOWS,
        "history_tail_rows": HISTORY_TAIL_ROWS,
        "fixed_base_name": FIXED_BASE_NAME,
        "base_col": base_col,
    }
    save_json(artifact_root / "feature_schema.json", feature_schema)

    save_json(
        artifact_root / "manifest.json",
        {
            "variant": VARIANT_NAME,
            "train_path": train_path,
            "test_path": test_path,
            "submission_path": submission_path,
            "variant_submission_path": variant_submission_path,
            "artifact_root": artifact_root,
            "models_dir": model_dir,
            "reports_dir": report_dir,
            "calibration_dir": calib_dir,
            "history_tail_file": artifact_root / "history_tail.parquet",
            "latest_snapshot_file": artifact_root / "latest_snapshot.parquet",
            "feature_schema_file": artifact_root / "feature_schema.json",
            "metrics_by_horizon_file": report_dir / "metrics_by_horizon.csv",
            "metrics_summary_file": report_dir / "metrics_summary.json",
            "route_wape_7d_file": artifact_root / "route_wape_7d.json",
            "office_scales_file": calib_dir / "office_scales.parquet",
            "notes": [
                "history_tail.parquet stores the last 672 rows per route for feature recomputation in inference.",
                "Each horizon model is saved as LightGBM text in models/.",
                "Per-horizon calibration metadata is stored in calibration/hXX_meta.json.",
            ],
        },
    )

    print("\nVariant:", VARIANT_NAME)
    print("Chosen bases by horizon:")
    print(chosen_base_by_h)
    print("\nCalibration mode by horizon:")
    print(calib_mode_by_h)
    print("\nMean per-horizon calib metric:", round(float(np.mean(list(metric_by_h.values()))), 5))
    print("\nSaved submission to:", submission_path)
    print("Saved variant submission to:", variant_submission_path)
    print("Saved inference artifacts to:", artifact_root)
    print(submission.head())


if __name__ == "__main__":
    main()
