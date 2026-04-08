import datetime
import json
import os
import sys

import numpy as np


def to_int_array(values):
    result = []
    for value in values:
        try:
            result.append(int(value))
        except Exception:
            result.append(value.item() if hasattr(value, "item") else str(value))
    return result


def build_summary(npz_path: str):
    npz_data = np.load(npz_path, allow_pickle=True)

    route_ids = to_int_array(npz_data["route_ids"])
    office_ids = to_int_array(npz_data["office_ids"])
    timestamps_ns = npz_data["timestamps_ns"].astype(np.int64)
    target_matrix = npz_data["target_matrix"].astype(np.float32)
    freq_minutes = int(npz_data["freq_minutes"][0])

    slots_per_day = (24 * 60) // freq_minutes
    slots_per_week = slots_per_day * 7

    slot_indices = np.empty(len(timestamps_ns), dtype=np.int32)
    for i, timestamp_ns in enumerate(timestamps_ns):
        dt_utc = datetime.datetime.utcfromtimestamp(int(timestamp_ns) / 1_000_000_000)
        weekday = dt_utc.weekday()  # Monday=0
        minute_of_day = dt_utc.hour * 60 + dt_utc.minute
        slot_of_day = minute_of_day // freq_minutes
        slot_indices[i] = weekday * slots_per_day + slot_of_day

    route_global_mean = np.mean(target_matrix, axis=1)
    baseline_matrix = np.empty((target_matrix.shape[0], slots_per_week), dtype=np.float32)

    for slot in range(slots_per_week):
        slot_columns = np.where(slot_indices == slot)[0]
        if slot_columns.size > 0:
            baseline_matrix[:, slot] = np.mean(target_matrix[:, slot_columns], axis=1)
        else:
            baseline_matrix[:, slot] = route_global_mean

    history_len = target_matrix.shape[1]
    same_slot_lags = [slots_per_week * week for week in range(1, 5)]
    same_slot_samples = []
    for lag in same_slot_lags:
        column_index = history_len - 1 - lag
        if column_index >= 0:
            same_slot_samples.append(target_matrix[:, column_index])

    if same_slot_samples:
        same_slot_stack = np.stack(same_slot_samples, axis=1)
        same_slot_mean_4w = np.nanmean(same_slot_stack, axis=1).astype(np.float32)
    else:
        same_slot_mean_4w = route_global_mean.astype(np.float32)

    roll_window = min(48, history_len)
    if roll_window > 0:
        roll_mean_48 = np.nanmean(target_matrix[:, -roll_window:], axis=1).astype(np.float32)
    else:
        roll_mean_48 = route_global_mean.astype(np.float32)

    baseline_blend = (0.5 * same_slot_mean_4w + 0.5 * roll_mean_48).astype(np.float32)
    baseline_rule_based = target_matrix[:, -1].astype(np.float32)

    same_slot_mean_4w = np.where(np.isfinite(same_slot_mean_4w), same_slot_mean_4w, route_global_mean)
    roll_mean_48 = np.where(np.isfinite(roll_mean_48), roll_mean_48, route_global_mean)
    baseline_blend = np.where(np.isfinite(baseline_blend), baseline_blend, route_global_mean)
    baseline_rule_based = np.where(
        np.isfinite(baseline_rule_based),
        baseline_rule_based,
        route_global_mean,
    )

    summary = {
        "generated_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "source_npz": os.path.basename(npz_path),
        "freq_minutes": freq_minutes,
        "slots_per_day": slots_per_day,
        "slots_per_week": slots_per_week,
        "route_ids": route_ids,
        "office_ids": office_ids,
        "baseline_by_route_and_slot": np.round(baseline_matrix, 6).tolist(),
        "baseline_same_4w_by_route": np.round(same_slot_mean_4w, 6).tolist(),
        "baseline_blend_by_route": np.round(baseline_blend, 6).tolist(),
        "baseline_rule_based_by_route": np.round(baseline_rule_based, 6).tolist(),
    }
    return summary


def persist_enterprise_baselines(npz_path: str, summary: dict):
    with np.load(npz_path, allow_pickle=True) as npz_data:
        payload = {key: npz_data[key] for key in npz_data.files}

    payload.pop("baseline_same_7d_by_route", None)

    payload["baseline_same_4w_by_route"] = np.asarray(
        summary.get("baseline_same_4w_by_route", []), dtype=np.float32
    )
    payload["baseline_blend_by_route"] = np.asarray(
        summary.get("baseline_blend_by_route", []), dtype=np.float32
    )
    payload["baseline_rule_based_by_route"] = np.asarray(
        summary.get("baseline_rule_based_by_route", []), dtype=np.float32
    )

    temp_path = f"{npz_path}.tmp.npz"
    np.savez_compressed(temp_path, **payload)
    os.replace(temp_path, npz_path)


def main():
    if len(sys.argv) != 3:
        print("Usage: build_inference_summary.py <inference_state.npz> <output.json>")
        sys.exit(1)

    npz_path = sys.argv[1]
    out_path = sys.argv[2]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    summary = build_summary(npz_path)
    persist_enterprise_baselines(npz_path, summary)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False)

    print(f"Summary written to: {out_path}")


if __name__ == "__main__":
    main()
