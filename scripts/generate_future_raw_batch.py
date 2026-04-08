import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

STATUS_COLS = [f"status_{idx}" for idx in range(1, 9)]
TRAIN_COLS = ["office_from_id", "route_id", "timestamp", *STATUS_COLS, "target_2h"]


def _fit_log_linear_target_model(sample_df: pd.DataFrame) -> Tuple[float, np.ndarray]:
    x = np.log1p(sample_df[STATUS_COLS].to_numpy(dtype=float))
    y = np.log1p(sample_df["target_2h"].to_numpy(dtype=float))
    x_aug = np.concatenate([np.ones((x.shape[0], 1)), x], axis=1)
    beta, *_ = np.linalg.lstsq(x_aug, y, rcond=None)
    intercept = float(beta[0])
    weights = beta[1:].astype(float)
    return intercept, weights


def _build_route_histories(
    train_df: pd.DataFrame,
    route_ids: np.ndarray,
    history_len: int,
) -> Tuple[np.ndarray, np.ndarray]:
    n_routes = len(route_ids)
    hist_status = np.zeros((n_routes, history_len, len(STATUS_COLS)), dtype=np.float64)
    hist_target = np.zeros((n_routes, history_len), dtype=np.float64)

    grouped = train_df.groupby("route_id", sort=True)
    for route_pos, route_id in enumerate(route_ids):
        route_df = grouped.get_group(int(route_id)).sort_values("timestamp").tail(history_len)
        status_values = route_df[STATUS_COLS].to_numpy(dtype=np.float64)
        target_values = route_df["target_2h"].to_numpy(dtype=np.float64)

        if len(route_df) < history_len:
            pad_len = history_len - len(route_df)
            if len(route_df) == 0:
                continue
            status_pad = np.repeat(status_values[:1], pad_len, axis=0)
            target_pad = np.repeat(target_values[:1], pad_len, axis=0)
            status_values = np.concatenate([status_pad, status_values], axis=0)
            target_values = np.concatenate([target_pad, target_values], axis=0)

        hist_status[route_pos] = status_values[-history_len:]
        hist_target[route_pos] = target_values[-history_len:]

    return hist_status, hist_target


def generate_future_batch(
    train_path: Path,
    output_csv_path: Path,
    steps: int,
    seed: int,
    history_len: int,
) -> Dict[str, object]:
    rng = np.random.default_rng(seed)

    train_df = pd.read_parquet(train_path, columns=TRAIN_COLS).copy()
    train_df["timestamp"] = pd.to_datetime(train_df["timestamp"], errors="coerce")
    train_df = train_df.dropna(subset=["timestamp"]).sort_values(["route_id", "timestamp"])

    route_office_counts = train_df.groupby("route_id")["office_from_id"].nunique()
    if not (route_office_counts == 1).all():
        raise ValueError("Обнаружены route_id с несколькими office_from_id, генерация остановлена.")

    route_office_map = (
        train_df.groupby("route_id", sort=True)["office_from_id"].last().astype(int).to_dict()
    )
    route_ids = np.array(sorted(route_office_map.keys()), dtype=int)
    office_ids = np.array([route_office_map[int(route_id)] for route_id in route_ids], dtype=int)
    n_routes = len(route_ids)

    max_ts = train_df["timestamp"].max()
    min_ts = train_df["timestamp"].min()
    freq_minutes = int(
        (
            train_df[train_df["route_id"] == route_ids[0]]["timestamp"]
            .diff()
            .dropna()
            .dt.total_seconds()
            .div(60)
            .mode()
            .iloc[0]
        )
    )

    # Глобальные факторы сезонности по времени слота и дню недели.
    seasonal_df = train_df.copy()
    seasonal_df["slot"] = (
        seasonal_df["timestamp"].dt.hour * (60 // freq_minutes)
        + seasonal_df["timestamp"].dt.minute // freq_minutes
    ).astype(int)
    seasonal_df["dow"] = seasonal_df["timestamp"].dt.dayofweek.astype(int)

    slot_factors = np.ones((len(STATUS_COLS), int((24 * 60) // freq_minutes)), dtype=np.float64)
    dow_factors = np.ones((len(STATUS_COLS), 7), dtype=np.float64)
    expected_means_by_dow_slot = np.zeros(
        (len(STATUS_COLS), 7, int((24 * 60) // freq_minutes)), dtype=np.float64
    )
    zero_rate_by_dow_slot = np.zeros(
        (len(STATUS_COLS), 7, int((24 * 60) // freq_minutes)), dtype=np.float64
    )
    status_caps = np.zeros(len(STATUS_COLS), dtype=np.float64)
    status_q30 = np.zeros(len(STATUS_COLS), dtype=np.float64)
    status_zero_rate = np.zeros(len(STATUS_COLS), dtype=np.float64)
    status_noise_sigma = np.zeros(len(STATUS_COLS), dtype=np.float64)

    for status_idx, status_col in enumerate(STATUS_COLS):
        overall_mean = float(train_df[status_col].mean()) + 1e-6

        by_slot = seasonal_df.groupby("slot")[status_col].mean() / overall_mean
        by_dow = seasonal_df.groupby("dow")[status_col].mean() / overall_mean
        by_dow_slot = (
            seasonal_df.groupby(["dow", "slot"], as_index=False)[status_col]
            .mean()
            .rename(columns={status_col: "mean_value"})
        )
        zero_by_dow_slot = (
            seasonal_df.assign(_is_zero=(seasonal_df[status_col] == 0).astype(float))
            .groupby(["dow", "slot"], as_index=False)["_is_zero"]
            .mean()
            .rename(columns={"_is_zero": "zero_rate"})
        )

        slot_factors[status_idx, by_slot.index.to_numpy(dtype=int)] = by_slot.to_numpy(dtype=float)
        dow_factors[status_idx, by_dow.index.to_numpy(dtype=int)] = by_dow.to_numpy(dtype=float)
        expected_means_by_dow_slot[status_idx, :, :] = overall_mean
        zero_rate_by_dow_slot[status_idx, :, :] = float((train_df[status_col] == 0).mean())
        for _, row in by_dow_slot.iterrows():
            expected_means_by_dow_slot[
                status_idx, int(row["dow"]), int(row["slot"])
            ] = float(row["mean_value"])
        for _, row in zero_by_dow_slot.iterrows():
            zero_rate_by_dow_slot[
                status_idx, int(row["dow"]), int(row["slot"])
            ] = float(row["zero_rate"])

        slot_factors[status_idx] = np.clip(slot_factors[status_idx], 0.70, 1.35)
        dow_factors[status_idx] = np.clip(dow_factors[status_idx], 0.82, 1.20)

        q50 = float(train_df[status_col].quantile(0.5))
        q30 = float(train_df[status_col].quantile(0.3))
        q95 = float(train_df[status_col].quantile(0.95))
        q999 = float(train_df[status_col].quantile(0.999))
        status_q30[status_idx] = q30
        status_zero_rate[status_idx] = float((train_df[status_col] == 0).mean())
        status_caps[status_idx] = q999

        # Больше волатильности у более "рваных" статусов.
        spread = (q95 - q50) / (q50 + 1.0)
        status_noise_sigma[status_idx] = float(np.clip(0.08 + 0.03 * spread, 0.09, 0.26))

    # Модель для target_2h и маршрутная поправка.
    sample_size = min(300_000, len(train_df))
    sample_df = train_df.sample(n=sample_size, random_state=seed) if len(train_df) > sample_size else train_df
    intercept, target_weights = _fit_log_linear_target_model(sample_df)

    recent_df = train_df.groupby("route_id", sort=True).tail(336).copy()
    recent_x = np.log1p(recent_df[STATUS_COLS].to_numpy(dtype=float))
    recent_pred = np.expm1(intercept + recent_x @ target_weights)
    recent_ratio = (recent_df["target_2h"].to_numpy(dtype=float) + 1.0) / (recent_pred + 1.0)
    route_scales = (
        recent_df.assign(_ratio=recent_ratio)
        .groupby("route_id")["_ratio"]
        .median()
        .clip(0.60, 1.65)
        .to_dict()
    )
    route_target_scale = np.array(
        [float(route_scales.get(int(route_id), 1.0)) for route_id in route_ids],
        dtype=np.float64,
    )

    # Риск-индекс маршрута для редких всплесков.
    status_sum = train_df[STATUS_COLS].sum(axis=1)
    spike_threshold = float(status_sum.quantile(0.975))
    route_spike_rate = (
        train_df.assign(_sum=status_sum)
        .assign(_is_spike=lambda d: (d["_sum"] >= spike_threshold).astype(int))
        .groupby("route_id")["_is_spike"]
        .mean()
        .clip(0.0, 0.20)
        .to_dict()
    )
    route_spike_prob = np.array(
        [0.010 + 0.08 * float(route_spike_rate.get(int(route_id), 0.0)) for route_id in route_ids],
        dtype=np.float64,
    )

    hist_status, hist_target = _build_route_histories(
        train_df=train_df,
        route_ids=route_ids,
        history_len=history_len,
    )

    rows: List[Dict[str, object]] = []
    current_ts = max_ts
    slots_per_day = int((24 * 60) // freq_minutes)

    for _step_idx in range(steps):
        current_ts = current_ts + pd.Timedelta(minutes=freq_minutes)
        slot_idx = int(current_ts.hour * (60 // freq_minutes) + current_ts.minute // freq_minutes)
        dow_idx = int(current_ts.dayofweek)

        same_week_idx = -336 if history_len >= 336 else -1
        same_slot = hist_status[:, same_week_idx, :]
        last_values = hist_status[:, -1, :]
        short_mean = hist_status[:, -4:, :].mean(axis=1)

        base = 0.55 * same_slot + 0.30 * last_values + 0.15 * short_mean
        seasonal = slot_factors[:, slot_idx][None, :] * dow_factors[:, dow_idx][None, :]
        noise = rng.lognormal(
            mean=0.0,
            sigma=status_noise_sigma[None, :],
            size=(n_routes, len(STATUS_COLS)),
        )
        generated = base * seasonal * noise

        # Плавная связь с прошлой целью, чтобы не терять консистентность.
        target_momentum = np.clip(hist_target[:, -1] / 120.0, 0.75, 1.35)[:, None]
        generated *= 0.90 + 0.10 * target_momentum

        spike_mask = rng.random(n_routes) < route_spike_prob
        if spike_mask.any():
            spike_boost = rng.uniform(1.35, 2.35, size=(int(spike_mask.sum()), 1))
            generated[np.where(spike_mask)[0], 4:8] *= spike_boost

        # Калибруем средний уровень к реальным статистикам train для конкретного dow+slot.
        desired_means = expected_means_by_dow_slot[:, dow_idx, slot_idx]
        current_means = np.maximum(generated.mean(axis=0), 1e-6)
        mean_correction = np.clip(desired_means / current_means, 0.60, 1.20)
        generated *= mean_correction[None, :]

        # Возвращаем реалистичную плотность нулевых значений по статусам.
        for status_idx in range(len(STATUS_COLS)):
            desired_zero_rate = float(zero_rate_by_dow_slot[status_idx, dow_idx, slot_idx])
            desired_zero_rate = float(np.clip(desired_zero_rate, 0.0, 0.95))
            zeros_count = int(round(desired_zero_rate * n_routes))
            if zeros_count <= 0:
                continue
            # Обнуляем в первую очередь самые малые значения, чтобы форма распределения
            # оставалась похожей на train.
            order = np.argsort(generated[:, status_idx], kind="mergesort")
            generated[order[:zeros_count], status_idx] = 0.0

        # Для status_7/status_8 делаем более "редкий пик": много малых значений и редкие всплески.
        for status_idx in (6, 7):
            values = generated[:, status_idx]
            nonzero = values[values > 0]
            if nonzero.size < 10:
                continue
            q80 = float(np.quantile(nonzero, 0.80))
            mid_mask = (values > 0) & (values < q80)
            high_mask = values >= q80
            values[mid_mask] *= 0.42
            values[high_mask] *= 1.18
            generated[:, status_idx] = values

        # Повторно выравниваем средние после shape-коррекции.
        current_means_2 = np.maximum(generated.mean(axis=0), 1e-6)
        mean_correction_2 = np.clip(desired_means / current_means_2, 0.85, 1.25)
        generated *= mean_correction_2[None, :]

        generated = np.clip(generated, 0.0, status_caps[None, :])
        generated_int = np.rint(generated).astype(int)

        x_log = np.log1p(generated_int.astype(np.float64))
        target_pred = np.expm1(intercept + x_log @ target_weights)
        target_noise = rng.lognormal(mean=0.0, sigma=0.10, size=n_routes)
        generated_target = target_pred * route_target_scale * target_noise
        generated_target = np.clip(generated_target, 0.0, 900.0)
        low_activity = generated_int.sum(axis=1) < np.quantile(generated_int.sum(axis=1), 0.08)
        generated_target[low_activity] = 0.0
        generated_target = np.round(generated_target, 1)

        for idx in range(n_routes):
            row = {
                "office_from_id": int(office_ids[idx]),
                "route_id": int(route_ids[idx]),
                "timestamp": current_ts,
                "target_2h": float(generated_target[idx]),
            }
            for status_idx, status_col in enumerate(STATUS_COLS):
                row[status_col] = int(generated_int[idx, status_idx])
            rows.append(row)

        # Обновляем историю скользящим окном.
        hist_status = np.roll(hist_status, shift=-1, axis=1)
        hist_status[:, -1, :] = generated_int
        hist_target = np.roll(hist_target, shift=-1, axis=1)
        hist_target[:, -1] = generated_target

    future_df = pd.DataFrame(rows, columns=TRAIN_COLS)
    future_df = future_df.sort_values(["timestamp", "route_id"]).reset_index(drop=True)

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    future_df.to_csv(output_csv_path, index=False)

    summary = {
        "train_rows": int(len(train_df)),
        "generated_rows": int(len(future_df)),
        "routes": int(n_routes),
        "offices": int(future_df["office_from_id"].nunique()),
        "freq_minutes": int(freq_minutes),
        "history_window_rows_per_route": int(history_len),
        "generated_steps": int(steps),
        "train_time_min": str(min_ts),
        "train_time_max": str(max_ts),
        "generated_time_min": str(future_df["timestamp"].min()),
        "generated_time_max": str(future_df["timestamp"].max()),
        "output_csv": str(output_csv_path),
    }
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate realistic future raw batch in train-like schema."
    )
    parser.add_argument(
        "--train-path",
        type=Path,
        default=Path("train_team_track.parquet"),
        help="Path to train parquet.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path(".cache/demo/future_raw_batch.csv"),
        help="Path to output generated CSV.",
    )
    parser.add_argument(
        "--steps",
        type=int,
        default=24,
        help="Number of future 30-min steps to generate (default: 24 => 12h).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--history-len",
        type=int,
        default=672,
        help="History window per route used for generation.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = generate_future_batch(
        train_path=args.train_path,
        output_csv_path=args.output_csv,
        steps=args.steps,
        seed=args.seed,
        history_len=args.history_len,
    )
    print("Synthetic future batch generated:")
    for key, value in summary.items():
        print(f"  - {key}: {value}")


if __name__ == "__main__":
    main()
