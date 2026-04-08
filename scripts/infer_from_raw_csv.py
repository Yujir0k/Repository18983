import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import lightgbm as lgb
import numpy as np
import pandas as pd

STATUS_COLS = [f"status_{idx}" for idx in range(1, 9)]
REQUIRED_RAW_COLUMNS = ["route_id", "office_from_id", "timestamp", *STATUS_COLS]


def _fail(message: str) -> None:
    raise ValueError(message)


def _to_utc_iso(value: pd.Timestamp) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.isoformat().replace("+00:00", "Z")


def _read_csv_auto(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep=None, engine="python")
    except Exception:
        return pd.read_csv(path)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    normalized.columns = [str(column).strip().lower() for column in normalized.columns]
    return normalized


def _infer_frequency_minutes(history_df: pd.DataFrame) -> int:
    route0 = history_df[history_df["route_id"] == history_df["route_id"].iloc[0]].copy()
    route0 = route0.sort_values("timestamp")
    diffs = route0["timestamp"].diff().dropna()
    if diffs.empty:
        return 30
    minutes = (diffs.dt.total_seconds() / 60.0).round().astype(int)
    mode_values = minutes.mode()
    if mode_values.empty:
        return 30
    freq = int(mode_values.iloc[0])
    return max(1, freq)


def _prepare_raw_events(raw_df: pd.DataFrame, known_route_ids: np.ndarray) -> pd.DataFrame:
    df = _normalize_columns(raw_df)

    missing_columns = [column for column in REQUIRED_RAW_COLUMNS if column not in df.columns]
    if missing_columns:
        _fail(
            "В CSV отсутствуют обязательные колонки: "
            + ", ".join(missing_columns)
        )

    keep_columns = [
        "route_id",
        "office_from_id",
        "timestamp",
        *STATUS_COLS,
    ]
    if "target_2h" in df.columns:
        keep_columns.append("target_2h")

    df = df[keep_columns].copy()
    df["route_id"] = pd.to_numeric(df["route_id"], errors="coerce")
    df["office_from_id"] = pd.to_numeric(df["office_from_id"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_convert(None)

    for status_col in STATUS_COLS:
        df[status_col] = pd.to_numeric(df[status_col], errors="coerce").fillna(0.0)
        df[status_col] = np.maximum(df[status_col], 0.0)

    if "target_2h" in df.columns:
        df["target_2h"] = pd.to_numeric(df["target_2h"], errors="coerce")
    else:
        df["target_2h"] = np.nan

    df = df.dropna(subset=["route_id", "office_from_id", "timestamp"])
    if df.empty:
        _fail("CSV не содержит валидных строк после очистки.")

    df["route_id"] = df["route_id"].round().astype(int)
    df["office_from_id"] = df["office_from_id"].round().astype(int)

    known_routes_set = set(int(route_id) for route_id in known_route_ids.tolist())
    unknown_routes = sorted(set(df["route_id"].tolist()) - known_routes_set)
    if unknown_routes:
        sample = ", ".join(str(value) for value in unknown_routes[:10])
        _fail(
            "В CSV есть route_id, которых нет в текущей модели/срезе: "
            + sample
        )

    df = df.sort_values(["route_id", "timestamp"]).drop_duplicates(
        subset=["route_id", "timestamp"], keep="last"
    )
    return df.reset_index(drop=True)


def _merge_history(
    history_df: pd.DataFrame,
    events_df: pd.DataFrame,
    history_tail_rows: int,
) -> Dict[int, pd.DataFrame]:
    merged = pd.concat([history_df, events_df], ignore_index=True)
    merged = merged.sort_values(["route_id", "timestamp"])
    merged = merged.drop_duplicates(subset=["route_id", "timestamp"], keep="last")

    merged["target_2h"] = pd.to_numeric(merged["target_2h"], errors="coerce")
    merged["target_2h"] = merged.groupby("route_id")["target_2h"].ffill()
    status_sum = merged[STATUS_COLS].sum(axis=1)
    merged["target_2h"] = merged["target_2h"].fillna(status_sum)

    route_history: Dict[int, pd.DataFrame] = {}
    for route_id, route_df in merged.groupby("route_id", sort=True):
        tail = route_df.tail(history_tail_rows).copy().reset_index(drop=True)
        route_history[int(route_id)] = tail

    return route_history


def _build_base_features(
    route_ids: np.ndarray,
    route_history: Dict[int, pd.DataFrame],
    target_lags: List[int],
    target_roll_windows: List[int],
    freq_minutes: int,
) -> pd.DataFrame:
    slots_per_day = max(1, (24 * 60) // max(1, freq_minutes))
    same_slot_lags = [slots_per_day * day for day in range(1, 8)]

    rows: List[Dict[str, float]] = []
    for route_id in route_ids.tolist():
        route_key = int(route_id)
        if route_key not in route_history:
            continue

        history_df = route_history[route_key]
        history_targets = history_df["target_2h"].to_numpy(dtype=float)
        current = history_df.iloc[-1]
        ts = pd.Timestamp(current["timestamp"])

        row: Dict[str, float] = {
            "route_id": float(route_key),
            "office_from_id": float(int(current["office_from_id"])),
            "hour": float(ts.hour),
            "minute": float(ts.minute),
            "dow": float(ts.dayofweek),
            "target_2h": float(current["target_2h"]),
        }

        status_values = []
        for status_col in STATUS_COLS:
            status_value = float(current[status_col])
            row[status_col] = status_value
            status_values.append(status_value)

        row["status_sum"] = float(np.sum(status_values))
        row["status_max"] = float(np.max(status_values)) if status_values else 0.0

        current_index = len(history_targets) - 1
        for lag in target_lags:
            lookup_index = current_index - lag
            lag_value = history_targets[lookup_index] if lookup_index >= 0 else np.nan
            row[f"target_lag_{lag}"] = float(lag_value) if np.isfinite(lag_value) else np.nan

        for window in target_roll_windows:
            if len(history_targets) == 0:
                row[f"target_roll_mean_{window}"] = np.nan
                row[f"target_roll_std_{window}"] = 0.0
                continue
            window_slice = history_targets[-window:]
            row[f"target_roll_mean_{window}"] = float(np.nanmean(window_slice))
            row[f"target_roll_std_{window}"] = (
                float(np.nanstd(window_slice, ddof=0)) if len(window_slice) > 1 else 0.0
            )

        same_slot_values = []
        for lag in same_slot_lags:
            lookup_index = current_index - lag
            if lookup_index >= 0:
                same_slot_values.append(history_targets[lookup_index])
        row["same_slot_mean_7d"] = (
            float(np.nanmean(same_slot_values)) if same_slot_values else np.nan
        )

        row["timestamp"] = ts
        rows.append(row)

    if not rows:
        _fail("После объединения истории не осталось маршрутов для инференса.")

    return pd.DataFrame(rows).sort_values("route_id").reset_index(drop=True)


def _load_office_scales(model_dir: Path) -> Dict[int, Dict[int, float]]:
    office_scales_path = model_dir / "office_scales.parquet"
    if not office_scales_path.exists():
        return {}

    office_scales_df = pd.read_parquet(office_scales_path)
    horizon_map: Dict[int, Dict[int, float]] = {}
    for horizon, horizon_df in office_scales_df.groupby("horizon", sort=True):
        horizon_map[int(horizon)] = {
            int(office_id): float(scale)
            for office_id, scale in zip(
                horizon_df["office_from_id"].tolist(),
                horizon_df["scale"].tolist(),
            )
        }
    return horizon_map


def _apply_calibration(
    predictions: np.ndarray,
    office_ids: np.ndarray,
    horizon: int,
    meta: Dict,
    office_scales_by_horizon: Dict[int, Dict[int, float]],
) -> np.ndarray:
    calibrated = np.nan_to_num(predictions, nan=0.0, posinf=0.0, neginf=0.0)
    calibrated = np.maximum(calibrated, 0.0)

    global_scale = float(meta.get("global_scale", 1.0))
    post_scale = float(meta.get("post_scale", 1.0))
    calibrated *= global_scale

    if str(meta.get("calib_mode", "")).lower() == "office_shrink":
        horizon_scales = office_scales_by_horizon.get(horizon, {})
        office_scale_values = np.array(
            [horizon_scales.get(int(office_id), 1.0) for office_id in office_ids],
            dtype=float,
        )
        office_strength = max(0.0, float(meta.get("office_strength", 1.0)))
        shrink_factor = office_strength / (office_strength + 1.0)
        office_adjustment = 1.0 + (office_scale_values - 1.0) * shrink_factor
        calibrated *= office_adjustment

    calibrated *= post_scale
    return np.maximum(calibrated, 0.0)


def _predict_all_horizons(
    model_dir: Path,
    base_features_df: pd.DataFrame,
    freq_minutes: int,
    category_levels: Dict[str, List[int]],
) -> Tuple[np.ndarray, List[int]]:
    office_scales_by_horizon = _load_office_scales(model_dir)

    route_ids = base_features_df["route_id"].astype(int).to_numpy()
    office_ids = base_features_df["office_from_id"].astype(int).to_numpy()
    base_timestamps = pd.to_datetime(base_features_df["timestamp"], errors="coerce")

    horizon_predictions: List[np.ndarray] = []
    horizons: List[int] = []

    for horizon in range(1, 11):
        meta_path = model_dir / f"h{horizon:02d}_meta.json"
        model_path = model_dir / f"h{horizon:02d}.txt"
        if not meta_path.exists() or not model_path.exists():
            _fail(f"Отсутствуют артефакты для горизонта h{horizon:02d}.")

        with meta_path.open("r", encoding="utf-8") as meta_file:
            meta = json.load(meta_file)

        feature_cols = list(meta["feature_cols"])
        future_feature_names = list(meta.get("future_feature_names", []))

        frame = base_features_df.copy()
        if len(future_feature_names) != 3:
            _fail(f"Некорректные future_feature_names в h{horizon:02d}_meta.json")

        future_ts = base_timestamps + pd.to_timedelta(freq_minutes * horizon, unit="m")
        frame[future_feature_names[0]] = future_ts.dt.hour.astype(float)
        frame[future_feature_names[1]] = future_ts.dt.minute.astype(float)
        frame[future_feature_names[2]] = future_ts.dt.dayofweek.astype(float)

        for feature_col in feature_cols:
            if feature_col not in frame.columns:
                frame[feature_col] = 0.0

        model_input = frame[feature_cols].copy()
        categorical_cols = set(meta.get("categorical_cols", []))

        for feature_col in feature_cols:
            if feature_col in categorical_cols:
                levels = category_levels.get(feature_col)
                if levels is None and feature_col.startswith("future_hour_"):
                    levels = category_levels.get("hour")
                if levels is None and feature_col.startswith("future_minute_"):
                    levels = category_levels.get("minute")
                if levels is None and feature_col.startswith("future_dow_"):
                    levels = category_levels.get("dow")
                if levels is None:
                    levels = sorted(
                        {
                            int(value)
                            for value in pd.to_numeric(
                                model_input[feature_col], errors="coerce"
                            )
                            .dropna()
                            .astype(int)
                            .tolist()
                        }
                    )
                series = pd.to_numeric(model_input[feature_col], errors="coerce")
                series = series.fillna(levels[0] if levels else 0).astype(int)
                model_input[feature_col] = pd.Categorical(
                    series, categories=[int(level) for level in levels]
                )
            else:
                model_input[feature_col] = (
                    pd.to_numeric(model_input[feature_col], errors="coerce")
                    .replace([np.inf, -np.inf], np.nan)
                    .fillna(0.0)
                    .astype(float)
                )

        booster = lgb.Booster(model_file=str(model_path))
        best_iteration = int(meta.get("best_iteration", 0))
        raw_prediction = booster.predict(
            model_input,
            num_iteration=best_iteration if best_iteration > 0 else None,
        )
        calibrated_prediction = _apply_calibration(
            np.asarray(raw_prediction, dtype=float),
            office_ids=office_ids,
            horizon=horizon,
            meta=meta,
            office_scales_by_horizon=office_scales_by_horizon,
        )
        horizon_predictions.append(calibrated_prediction)
        horizons.append(horizon)

    prediction_matrix = np.vstack(horizon_predictions)
    return prediction_matrix, route_ids.tolist()


def _build_submission(prediction_matrix: np.ndarray, route_ids: List[int]) -> pd.DataFrame:
    submission_rows: List[Dict[str, float]] = []
    route_ids_np = np.asarray(route_ids, dtype=int)

    for route_index, route_id in enumerate(route_ids_np):
        for horizon_idx in range(prediction_matrix.shape[0]):
            submission_rows.append(
                {
                    "id": int(route_id * 10 + horizon_idx),
                    "yPred": float(prediction_matrix[horizon_idx, route_index]),
                }
            )

    submission_df = pd.DataFrame(submission_rows).sort_values("id").reset_index(drop=True)
    return submission_df


def run_inference(model_dir: Path, input_csv: Path, output_csv: Path, output_meta: Path) -> None:
    feature_schema_path = model_dir / "feature_schema.json"
    history_tail_path = model_dir / "history_tail.parquet"
    latest_snapshot_path = model_dir / "latest_snapshot.parquet"

    if not feature_schema_path.exists():
        _fail("Не найден feature_schema.json в model.")
    if not history_tail_path.exists():
        _fail("Не найден history_tail.parquet в model.")
    if not latest_snapshot_path.exists():
        _fail("Не найден latest_snapshot.parquet в model.")
    if not input_csv.exists():
        _fail("Не найден входной CSV с сырыми данными.")

    with feature_schema_path.open("r", encoding="utf-8") as schema_file:
        feature_schema = json.load(schema_file)

    history_tail_rows = int(feature_schema.get("history_tail_rows", 672))
    target_lags = [int(value) for value in feature_schema.get("target_lags", [])]
    target_roll_windows = [int(value) for value in feature_schema.get("target_roll_windows", [])]
    category_levels = {
        str(key): [int(value) for value in values]
        for key, values in feature_schema.get("category_levels", {}).items()
    }

    history_df = pd.read_parquet(history_tail_path).copy()
    latest_df = pd.read_parquet(latest_snapshot_path).copy()

    history_df = _normalize_columns(history_df)
    latest_df = _normalize_columns(latest_df)
    history_df["timestamp"] = pd.to_datetime(history_df["timestamp"], errors="coerce")
    latest_df["timestamp"] = pd.to_datetime(latest_df["timestamp"], errors="coerce")

    known_route_ids = (
        latest_df["route_id"].dropna().astype(int).sort_values().unique()
    )
    raw_events_df = _read_csv_auto(input_csv)
    events_df = _prepare_raw_events(raw_events_df, known_route_ids=known_route_ids)

    route_history = _merge_history(
        history_df=history_df,
        events_df=events_df,
        history_tail_rows=history_tail_rows,
    )

    freq_minutes = _infer_frequency_minutes(history_df)
    base_features_df = _build_base_features(
        route_ids=known_route_ids,
        route_history=route_history,
        target_lags=target_lags,
        target_roll_windows=target_roll_windows,
        freq_minutes=freq_minutes,
    )

    prediction_matrix, route_ids = _predict_all_horizons(
        model_dir=model_dir,
        base_features_df=base_features_df,
        freq_minutes=freq_minutes,
        category_levels=category_levels,
    )
    submission_df = _build_submission(prediction_matrix, route_ids)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_meta.parent.mkdir(parents=True, exist_ok=True)
    submission_df.to_csv(output_csv, index=False)

    input_ts_min = pd.to_datetime(events_df["timestamp"], errors="coerce").min()
    input_ts_max = pd.to_datetime(events_df["timestamp"], errors="coerce").max()
    anchor_ts_min = pd.to_datetime(base_features_df["timestamp"], errors="coerce").min()
    anchor_ts_max = pd.to_datetime(base_features_df["timestamp"], errors="coerce").max()
    horizon_1_delta = pd.to_timedelta(freq_minutes, unit="m")
    horizon_4_delta = pd.to_timedelta(freq_minutes * 4, unit="m")
    horizon_8_delta = pd.to_timedelta(freq_minutes * 8, unit="m")
    horizon_10_delta = pd.to_timedelta(freq_minutes * 10, unit="m")

    horizon_1_min = anchor_ts_min + horizon_1_delta
    horizon_1_max = anchor_ts_max + horizon_1_delta
    horizon_4_min = anchor_ts_min + horizon_4_delta
    horizon_4_max = anchor_ts_max + horizon_4_delta
    horizon_8_min = anchor_ts_min + horizon_8_delta
    horizon_8_max = anchor_ts_max + horizon_8_delta
    horizon_10_min = anchor_ts_min + horizon_10_delta
    horizon_10_max = anchor_ts_max + horizon_10_delta

    meta = {
        "input_rows": int(len(events_df)),
        "input_routes": int(events_df["route_id"].nunique()),
        "predicted_routes": int(len(route_ids)),
        "generated_rows": int(len(submission_df)),
        "freq_minutes": int(freq_minutes),
        "generated_at_utc": _to_utc_iso(pd.Timestamp.utcnow()),
        "timestamp_min_utc": _to_utc_iso(input_ts_min),
        "timestamp_max_utc": _to_utc_iso(input_ts_max),
        "anchor_timestamp_min_utc": _to_utc_iso(anchor_ts_min),
        "anchor_timestamp_max_utc": _to_utc_iso(anchor_ts_max),
        "horizon_1_min_utc": _to_utc_iso(horizon_1_min),
        "horizon_1_max_utc": _to_utc_iso(horizon_1_max),
        "horizon_4_min_utc": _to_utc_iso(horizon_4_min),
        "horizon_4_max_utc": _to_utc_iso(horizon_4_max),
        "horizon_8_min_utc": _to_utc_iso(horizon_8_min),
        "horizon_8_max_utc": _to_utc_iso(horizon_8_max),
        "horizon_10_min_utc": _to_utc_iso(horizon_10_min),
        "horizon_10_max_utc": _to_utc_iso(horizon_10_max),
    }
    with output_meta.open("w", encoding="utf-8") as meta_file:
        json.dump(meta, meta_file, ensure_ascii=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run E2E inference from raw CSV and build submission predictions."
    )
    parser.add_argument("--model-dir", required=True, help="Path to model artifacts folder.")
    parser.add_argument("--input-csv", required=True, help="Path to raw CSV file.")
    parser.add_argument("--output-csv", required=True, help="Path to output submission CSV.")
    parser.add_argument("--output-meta", required=True, help="Path to output metadata JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_inference(
        model_dir=Path(args.model_dir),
        input_csv=Path(args.input_csv),
        output_csv=Path(args.output_csv),
        output_meta=Path(args.output_meta),
    )


if __name__ == "__main__":
    main()
