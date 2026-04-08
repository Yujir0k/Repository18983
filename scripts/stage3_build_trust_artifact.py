import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


FORECAST_HORIZONS = list(range(1, 11))
HISTORY_TAIL_EXPECTED_ROWS = 672

COMPONENT_WEIGHTS = {
    "horizon": 0.20,
    "stability": 0.16,
    "agreement_with_blend": 0.18,
    "route_error_history": 0.18,
    "office_error_history": 0.14,
    "history_completeness": 0.08,
    "freshness": 0.06,
}

AGREEMENT_MODE_NEUTRAL = "neutral"
AGREEMENT_MODE_LEGACY = "legacy"
AGREEMENT_MODE_GUARDRAIL = "guardrail"
DEFAULT_AGREEMENT_MODE = AGREEMENT_MODE_NEUTRAL

POLICY_CALIBRATION_NONE = "none"
POLICY_CALIBRATION_UPPER_TAIL_V1 = "upper_tail_v1"
DEFAULT_POLICY_CALIBRATION = POLICY_CALIBRATION_NONE


COMPONENT_LABELS_RU = {
    "horizon": "близкий горизонт",
    "stability": "стабильность маршрута",
    "agreement_with_blend": "согласие с blend baseline",
    "route_error_history": "история ошибок маршрута",
    "office_error_history": "история ошибок офиса",
    "history_completeness": "полнота истории",
    "freshness": "свежесть данных",
    "empirical_win_rate": "исторический win-rate офиса на горизонте",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return float(max(low, min(high, value)))


def parse_anchor_timestamp(anchor_raw: str, local_tz: str) -> pd.Timestamp:
    ts = pd.Timestamp(anchor_raw)
    if ts.tz is None:
        ts = ts.tz_localize(local_tz)
    return ts.tz_convert("UTC")


def find_first_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists() and path.is_file():
            return path
    raise FileNotFoundError(f"None of the candidate files exists: {[str(p) for p in paths]}")


def discover_artifact_roots() -> list[Path]:
    preferred = [
        Path("model_artifacts/status_semantics_export_bundle_artifacts"),
        Path("model_artifacts/stable_export_bundle_artifacts"),
    ]
    discovered = list(sorted(Path("model_artifacts").glob("*_artifacts")))

    roots: list[Path] = []
    for candidate in preferred + discovered:
        if candidate.exists() and candidate.is_dir() and candidate not in roots:
            roots.append(candidate)
    return roots


def parse_submission_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    columns = {col.lower().strip(): col for col in df.columns}

    id_col = columns.get("id")
    pred_col = None
    for key in ["y_pred", "ypred", "prediction", "pred", "forecast", "value"]:
        if key in columns:
            pred_col = columns[key]
            break

    if id_col is None or pred_col is None:
        raise ValueError(f"Submission CSV must include id and prediction columns: {csv_path}")

    out = pd.DataFrame(
        {
            "id": pd.to_numeric(df[id_col], errors="coerce"),
            "ml_prediction": pd.to_numeric(df[pred_col], errors="coerce"),
        }
    ).dropna()

    out["id"] = out["id"].astype(np.int64)
    out["route_id"] = (out["id"] // 10).astype(np.int64)
    out["horizon"] = ((out["id"] % 10) + 1).astype(np.int64)
    out["horizon_key"] = out["horizon"].map(lambda h: f"h{int(h)}")
    return out[["id", "route_id", "horizon", "horizon_key", "ml_prediction"]]


def load_inference_summary(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def to_utc_slot_index(timestamp_utc: pd.Timestamp, slots_per_day: int, freq_minutes: int) -> int:
    weekday_monday_zero = int(timestamp_utc.weekday())
    minute_of_day = int(timestamp_utc.hour * 60 + timestamp_utc.minute)
    slot_of_day = minute_of_day // freq_minutes
    return weekday_monday_zero * slots_per_day + slot_of_day


def baseline_value_at_timestamp(
    baseline_by_slot: np.ndarray,
    timestamp_utc: pd.Timestamp,
    slots_per_day: int,
    freq_minutes: int,
) -> float:
    if baseline_by_slot.size == 0:
        return 0.0
    slots_per_week = baseline_by_slot.size
    slot_index = to_utc_slot_index(timestamp_utc, slots_per_day, freq_minutes) % slots_per_week
    return float(baseline_by_slot[slot_index])


def baseline_value_at_timestamp_shifted(
    baseline_by_slot: np.ndarray,
    timestamp_utc: pd.Timestamp,
    slots_per_day: int,
    freq_minutes: int,
    shift_slots: int,
) -> float:
    if baseline_by_slot.size == 0:
        return 0.0
    slots_per_week = baseline_by_slot.size
    slot_index = to_utc_slot_index(timestamp_utc, slots_per_day, freq_minutes) % slots_per_week
    shifted_index = (slot_index - shift_slots + slots_per_week) % slots_per_week
    return float(baseline_by_slot[shifted_index])


def load_history_stats(history_tail_path: Path) -> pd.DataFrame:
    history = pd.read_parquet(history_tail_path, columns=["route_id", "timestamp", "target_2h"])
    history["timestamp"] = pd.to_datetime(history["timestamp"], utc=True)
    history = history.sort_values(["route_id", "timestamp"]).reset_index(drop=True)

    rows = []
    for route_id, group in history.groupby("route_id", sort=False):
        values = group["target_2h"].astype(float).to_numpy()
        values = values[np.isfinite(values)]
        if values.size == 0:
            mean_96 = 0.0
            std_96 = 0.0
            cv_96 = 2.0
        else:
            tail = values[-96:] if values.size >= 96 else values
            mean_96 = float(np.mean(tail))
            std_96 = float(np.std(tail))
            cv_96 = float(std_96 / (mean_96 + 1.0))

        stability_signal = 1.0 / (1.0 + cv_96)
        completeness_signal = clamp(float(len(group)) / float(HISTORY_TAIL_EXPECTED_ROWS))

        last_ts = group["timestamp"].iloc[-1]
        rows.append(
            {
                "route_id": int(route_id),
                "history_rows": int(len(group)),
                "history_last_timestamp_utc": last_ts.isoformat(),
                "stability_signal": clamp(stability_signal),
                "history_completeness_signal": completeness_signal,
                "cv_96": cv_96,
                "mean_96": mean_96,
                "std_96": std_96,
            }
        )

    return pd.DataFrame(rows)


def load_route_wape(path: Path) -> dict[int, float]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    lookup: dict[int, float] = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            try:
                route_id = int(float(key))
                route_wape = float(value)
            except Exception:
                continue
            if math.isfinite(route_wape):
                lookup[route_id] = max(0.0, route_wape)
    return lookup


def load_office_proxy(stage2_office_proxy_path: Path, stage2_by_horizon_path: Path) -> tuple[dict, dict, float]:
    office_df = pd.read_csv(stage2_office_proxy_path)
    by_h_df = pd.read_csv(stage2_by_horizon_path)

    office_lookup: dict[tuple[int, int], dict[str, float]] = {}
    if not office_df.empty:
        for row in office_df.itertuples(index=False):
            key = (int(row.office_from_id), int(row.horizon))
            office_lookup[key] = {
                "win_rate": float(row.win_rate),
                "expected_loss_wape_like": float(row.expected_loss_wape_like),
                "rows": float(row.rows),
            }

    horizon_lookup: dict[int, dict[str, float]] = {}
    for row in by_h_df.itertuples(index=False):
        horizon_lookup[int(row.horizon)] = {
            "proxy_win_rate": float(getattr(row, "proxy_win_rate", np.nan)),
            "proxy_expected_loss_wape_like": float(
                getattr(row, "proxy_expected_loss_wape_like", np.nan)
            ),
            "proxy_rows": float(getattr(row, "proxy_rows", 0.0)),
        }

    global_win = float(np.nanmean(by_h_df.get("proxy_win_rate", pd.Series([0.5]))))
    if not math.isfinite(global_win):
        global_win = 0.5

    return office_lookup, horizon_lookup, global_win


def horizon_signal(horizon: int) -> float:
    return clamp(1.0 - (float(horizon - 1) / 9.0))


def agreement_signal_legacy(ml_pred: float, strong_baseline: float) -> float:
    denom = max(1.0, abs(strong_baseline) + 25.0)
    delta = abs(ml_pred - strong_baseline) / denom
    return clamp(1.0 - delta)


def agreement_signal_guardrail(ml_pred: float, strong_baseline: float) -> float:
    # Neutral by default; penalize only very large disagreement with baseline.
    denom = max(1.0, abs(strong_baseline) + 25.0)
    delta = abs(ml_pred - strong_baseline) / denom
    if delta <= 0.35:
        return 0.5

    excess = delta - 0.35
    penalty = min(0.5, (excess / 1.0) * 0.5)
    return clamp(0.5 - penalty)


def agreement_signal(ml_pred: float, strong_baseline: float, mode: str) -> float:
    if mode == AGREEMENT_MODE_LEGACY:
        return agreement_signal_legacy(ml_pred, strong_baseline)
    if mode == AGREEMENT_MODE_GUARDRAIL:
        return agreement_signal_guardrail(ml_pred, strong_baseline)
    return 0.5


def route_error_signal(route_wape: float) -> float:
    return clamp(1.0 - (route_wape / 0.60))


def office_error_signal(expected_loss_wape_like: float) -> float:
    return clamp(1.0 - (expected_loss_wape_like / 0.12))


def freshness_signal(freshness_minutes: float) -> float:
    freshness_minutes = max(0.0, freshness_minutes)
    return clamp(math.exp(-freshness_minutes / 180.0))


@dataclass
class TrustResult:
    trust_score: float
    trust_score_pct: float
    reason_short: str
    reason_full: str
    empirical_win_rate: float
    empirical_weight: float
    base_components_score: float


def compose_reason(contrib: dict[str, float]) -> tuple[str, str]:
    positives = [(k, v) for k, v in contrib.items() if v > 0.01]
    negatives = [(k, v) for k, v in contrib.items() if v < -0.01]
    positives.sort(key=lambda item: item[1], reverse=True)
    negatives.sort(key=lambda item: item[1])

    positive_labels = [COMPONENT_LABELS_RU.get(k, k) for k, _ in positives[:2]]
    negative_labels = [COMPONENT_LABELS_RU.get(k, k) for k, _ in negatives[:2]]

    if positive_labels and negative_labels:
        short = f"Плюсы: {', '.join(positive_labels)}; риски: {', '.join(negative_labels)}."
    elif positive_labels:
        short = f"Плюсы: {', '.join(positive_labels)}."
    elif negative_labels:
        short = f"Риски: {', '.join(negative_labels)}."
    else:
        short = "Нейтральный профиль факторов: выраженных усилителей или рисков нет."

    long_parts = []
    for key, value in sorted(contrib.items(), key=lambda item: abs(item[1]), reverse=True):
        long_parts.append(f"{COMPONENT_LABELS_RU.get(key, key)}: {value:+.3f}")
    full = "; ".join(long_parts)
    return short, full


def compute_trust(
    component_scores: dict[str, float],
    empirical_win_rate_value: float,
    empirical_rows: float,
    horizon_global_win_rate: float,
) -> TrustResult:
    base_components_score = float(
        sum(COMPONENT_WEIGHTS[name] * component_scores[name] for name in COMPONENT_WEIGHTS)
    )

    empirical_rows = max(0.0, empirical_rows)
    empirical_weight = clamp(empirical_rows / 2000.0)
    empirical_blended = (
        empirical_weight * empirical_win_rate_value
        + (1.0 - empirical_weight) * horizon_global_win_rate
    )

    final_score = clamp(0.55 * base_components_score + 0.45 * empirical_blended)
    final_pct = round(final_score * 100.0, 1)

    contrib: dict[str, float] = {}
    for name, weight in COMPONENT_WEIGHTS.items():
        contrib[name] = 0.55 * weight * (component_scores[name] - 0.5)
    contrib["empirical_win_rate"] = 0.45 * (empirical_blended - 0.5)

    reason_short, reason_full = compose_reason(contrib)
    return TrustResult(
        trust_score=final_score,
        trust_score_pct=final_pct,
        reason_short=reason_short,
        reason_full=reason_full,
        empirical_win_rate=empirical_blended,
        empirical_weight=empirical_weight,
        base_components_score=base_components_score,
    )


def build_score_stats(scores_pct: pd.Series) -> dict[str, float]:
    if scores_pct.empty:
        return {
            "min": 0.0,
            "p10": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "max": 0.0,
            "share_ge_70": 0.0,
            "share_ge_85": 0.0,
        }

    return {
        "min": float(scores_pct.min()),
        "p10": float(scores_pct.quantile(0.10)),
        "p50": float(scores_pct.quantile(0.50)),
        "p90": float(scores_pct.quantile(0.90)),
        "max": float(scores_pct.max()),
        "share_ge_70": float((scores_pct >= 70.0).mean()),
        "share_ge_85": float((scores_pct >= 85.0).mean()),
    }


def apply_policy_calibration(
    trust_df: pd.DataFrame, mode: str
) -> tuple[pd.DataFrame, dict]:
    calibrated = trust_df.copy()
    calibrated["trust_score_raw"] = calibrated["trust_score"].astype(float)
    calibrated["trust_score_pct_raw"] = calibrated["trust_score_pct"].astype(float)

    before_scores = calibrated["trust_score_pct_raw"]
    after_scores = calibrated["trust_score_pct"].astype(float)

    report: dict = {
        "mode": mode,
        "applied": False,
        "note": "No policy calibration applied.",
        "before_global": build_score_stats(before_scores),
        "after_global": build_score_stats(after_scores),
        "per_horizon": {},
    }

    if mode == POLICY_CALIBRATION_NONE:
        return calibrated, report

    if mode != POLICY_CALIBRATION_UPPER_TAIL_V1:
        report["note"] = f"Unknown calibration mode '{mode}'. Fallback to none."
        return calibrated, report

    # Upper-tail calibration:
    # - Keep scores below 70 unchanged (preserve low-confidence behavior).
    # - Stretch only the >=70 segment to [70..90] per horizon when there is enough tail.
    adjusted_horizons: list[int] = []
    for horizon, group in calibrated.groupby("horizon"):
        raw_pct = group["trust_score_pct_raw"].astype(float)
        if raw_pct.empty:
            continue

        p90 = float(raw_pct.quantile(0.90))
        max_pct = float(raw_pct.max())
        min_pct = float(raw_pct.min())

        if p90 < 70.0 or max_pct <= 72.0:
            report["per_horizon"][int(horizon)] = {
                "applied": False,
                "note": "Tail is too weak for safe upper-tail stretching.",
                "before": build_score_stats(raw_pct),
                "after": build_score_stats(group["trust_score_pct"].astype(float)),
            }
            continue

        denom = max_pct - 70.0
        if denom <= 1e-9:
            report["per_horizon"][int(horizon)] = {
                "applied": False,
                "note": "Degenerate horizon tail.",
                "before": build_score_stats(raw_pct),
                "after": build_score_stats(group["trust_score_pct"].astype(float)),
            }
            continue

        horizon_mask = calibrated["horizon"] == horizon
        high_mask = horizon_mask & (calibrated["trust_score_pct_raw"] >= 70.0)
        high_raw = calibrated.loc[high_mask, "trust_score_pct_raw"].astype(float)
        if high_raw.empty:
            report["per_horizon"][int(horizon)] = {
                "applied": False,
                "note": "No scores >=70 on this horizon.",
                "before": build_score_stats(raw_pct),
                "after": build_score_stats(group["trust_score_pct"].astype(float)),
            }
            continue

        z = (high_raw - 70.0) / denom
        high_calibrated = 70.0 + z * 20.0
        high_calibrated = high_calibrated.clip(lower=min_pct, upper=95.0)

        calibrated.loc[high_mask, "trust_score_pct"] = high_calibrated
        calibrated.loc[high_mask, "trust_score"] = high_calibrated / 100.0
        adjusted_horizons.append(int(horizon))

        after_h = calibrated.loc[horizon_mask, "trust_score_pct"].astype(float)
        report["per_horizon"][int(horizon)] = {
            "applied": True,
            "note": "Upper tail >=70 stretched into [70..90].",
            "before": build_score_stats(raw_pct),
            "after": build_score_stats(after_h),
        }

    calibrated["trust_score_pct"] = calibrated["trust_score_pct"].astype(float).round(1)
    calibrated["trust_score"] = (calibrated["trust_score_pct"] / 100.0).astype(float)

    report["applied"] = len(adjusted_horizons) > 0
    report["adjusted_horizons"] = adjusted_horizons
    report["note"] = (
        "Upper-tail calibration applied to horizons with non-trivial >=70 tail. "
        "Scores <70 are unchanged."
    )
    report["after_global"] = build_score_stats(calibrated["trust_score_pct"].astype(float))
    return calibrated, report


def refresh_lookup_payload_from_df(
    trust_df: pd.DataFrame,
    lookup_payload: dict[str, dict[str, dict]],
    agreement_mode: str,
) -> None:
    for row in trust_df.itertuples(index=False):
        route_key = str(int(row.route_id))
        horizon_key = f"h{int(row.horizon)}"

        route_bucket = lookup_payload.setdefault(route_key, {})
        cell = route_bucket.get(horizon_key, {})
        cell.update(
            {
                "trust_score": float(row.trust_score),
                "trust_score_pct": float(row.trust_score_pct),
                "reason_short": row.reason_short,
                "reason_full": row.reason_full,
                "horizon": int(row.horizon),
                "office_from_id": int(row.office_from_id),
                "agreement_mode": agreement_mode,
                "source": "stage3_artifact",
            }
        )

        if hasattr(row, "trust_score_raw"):
            cell["trust_score_raw"] = float(row.trust_score_raw)
        if hasattr(row, "trust_score_pct_raw"):
            cell["trust_score_pct_raw"] = float(row.trust_score_pct_raw)

        route_bucket[horizon_key] = cell


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build stage3 trust score artifact for route x horizon."
    )
    parser.add_argument(
        "--submission-path",
        type=Path,
        default=None,
        help="Path to submission CSV. If omitted, auto-detects in model dir.",
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("model"),
        help="Model directory with route_wape/history files.",
    )
    parser.add_argument(
        "--inference-summary-path",
        type=Path,
        default=Path(".cache/inference-summary.json"),
        help="Path to inference summary JSON.",
    )
    parser.add_argument(
        "--history-tail-path",
        type=Path,
        default=None,
        help="Path to history_tail parquet. Defaults to model/history_tail.parquet",
    )
    parser.add_argument(
        "--latest-snapshot-path",
        type=Path,
        default=None,
        help="Path to latest_snapshot parquet. Defaults to model/latest_snapshot.parquet",
    )
    parser.add_argument(
        "--route-wape-path",
        type=Path,
        default=None,
        help="Path to route_wape_7d.json. Defaults to model/route_wape_7d.json",
    )
    parser.add_argument(
        "--stage2-office-proxy-path",
        type=Path,
        default=Path("model/analytics_stage2_office_horizon_proxy.csv"),
        help="Path to stage2 office x horizon proxy csv.",
    )
    parser.add_argument(
        "--stage2-by-horizon-path",
        type=Path,
        default=Path("model/analytics_stage2_by_horizon.csv"),
        help="Path to stage2 by horizon csv.",
    )
    parser.add_argument(
        "--anchor-timestamp",
        type=str,
        default="2025-05-30T10:30:00",
        help="Anchor timestamp used by dashboard (local time if timezone missing).",
    )
    parser.add_argument(
        "--local-timezone",
        type=str,
        default="Europe/Moscow",
        help="Timezone used when anchor timestamp has no timezone.",
    )
    parser.add_argument(
        "--agreement-mode",
        type=str,
        choices=[
            AGREEMENT_MODE_NEUTRAL,
            AGREEMENT_MODE_GUARDRAIL,
            AGREEMENT_MODE_LEGACY,
        ],
        default=DEFAULT_AGREEMENT_MODE,
        help=(
            "How to use agreement with blend baseline inside trust score. "
            "neutral=disabled (component fixed at 0.5), "
            "guardrail=penalize only large disagreement, "
            "legacy=old direct-agreement signal."
        ),
    )
    parser.add_argument(
        "--policy-calibration",
        type=str,
        choices=[
            POLICY_CALIBRATION_NONE,
            POLICY_CALIBRATION_UPPER_TAIL_V1,
        ],
        default=DEFAULT_POLICY_CALIBRATION,
        help=(
            "Post-calibration for trust score used by decision thresholds. "
            "none=raw trust, upper_tail_v1=stretch only >=70 tail per horizon."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("model"),
        help="Output dir for trust artifacts.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    artifact_roots = discover_artifact_roots()

    submission_path = (
        args.submission_path
        if args.submission_path is not None
        else find_first_existing(
            [
                args.model_dir / "submission_stable_export_bundle.csv",
                args.model_dir / "submission.csv",
                args.model_dir / "submission (61).csv",
            ]
        )
    )

    history_candidates = [args.model_dir / "history_tail.parquet"] + [
        root / "history_tail.parquet" for root in artifact_roots
    ]
    latest_snapshot_candidates = [args.model_dir / "latest_snapshot.parquet"] + [
        root / "latest_snapshot.parquet" for root in artifact_roots
    ]
    route_wape_candidates = [args.model_dir / "route_wape_7d.json"] + [
        root / "route_wape_7d.json" for root in artifact_roots
    ] + [
        root / "reports" / "route_wape_7d.json" for root in artifact_roots
    ]

    history_tail_path = (
        args.history_tail_path
        if args.history_tail_path is not None
        else find_first_existing(history_candidates)
    )
    latest_snapshot_path = (
        args.latest_snapshot_path
        if args.latest_snapshot_path is not None
        else find_first_existing(latest_snapshot_candidates)
    )
    route_wape_path = (
        args.route_wape_path
        if args.route_wape_path is not None
        else find_first_existing(route_wape_candidates)
    )

    anchor_ts_utc = parse_anchor_timestamp(args.anchor_timestamp, args.local_timezone)

    submission = parse_submission_csv(submission_path)
    summary = load_inference_summary(args.inference_summary_path)
    history_stats = load_history_stats(history_tail_path)
    route_wape_lookup = load_route_wape(route_wape_path)
    office_lookup, horizon_lookup, global_win_rate = load_office_proxy(
        args.stage2_office_proxy_path,
        args.stage2_by_horizon_path,
    )

    latest_snapshot = pd.read_parquet(
        latest_snapshot_path,
        columns=["route_id", "timestamp", "office_from_id"],
    )
    latest_snapshot["timestamp"] = pd.to_datetime(latest_snapshot["timestamp"], utc=True)
    latest_by_route = latest_snapshot.sort_values(["route_id", "timestamp"]).groupby("route_id", as_index=False).tail(1)
    route_latest_ts = {
        int(row.route_id): row.timestamp for row in latest_by_route.itertuples(index=False)
    }

    route_ids = summary.get("route_ids", [])
    office_ids = summary.get("office_ids", [])
    baseline_matrix = summary.get("baseline_by_route_and_slot", [])
    freq_minutes = int(summary.get("freq_minutes", 30))
    slots_per_day = int(summary.get("slots_per_day", 48))

    lag48_shift_slots = max(1, int(round((48 * 60) / freq_minutes)))

    submission_map = {
        (int(row.route_id), int(row.horizon)): float(row.ml_prediction)
        for row in submission.itertuples(index=False)
    }

    history_map = {
        int(row.route_id): row for row in history_stats.itertuples(index=False)
    }

    global_route_wape = (
        float(np.mean(list(route_wape_lookup.values())))
        if route_wape_lookup
        else 0.2485
    )

    rows = []
    lookup_payload: dict[str, dict[str, dict]] = {}

    for route_index, route_id_raw in enumerate(route_ids):
        route_id = int(route_id_raw)
        office_id = int(office_ids[route_index]) if route_index < len(office_ids) else -1
        baseline_series = (
            np.asarray(baseline_matrix[route_index], dtype="float64")
            if route_index < len(baseline_matrix)
            else np.asarray([], dtype="float64")
        )
        route_history = history_map.get(route_id)
        route_wape = route_wape_lookup.get(route_id, global_route_wape)
        route_error_hist_signal = route_error_signal(route_wape)

        if route_history is not None:
            stability_val = float(route_history.stability_signal)
            completeness_val = float(route_history.history_completeness_signal)
            route_last_ts = pd.Timestamp(route_history.history_last_timestamp_utc)
            if route_last_ts.tz is None:
                route_last_ts = route_last_ts.tz_localize("UTC")
        else:
            stability_val = 0.5
            completeness_val = 0.5
            route_last_ts = route_latest_ts.get(route_id, anchor_ts_utc)

        freshness_minutes = max(
            0.0, float((anchor_ts_utc - route_last_ts).total_seconds() / 60.0)
        )
        freshness_val = freshness_signal(freshness_minutes)

        route_key = str(route_id)
        lookup_payload[route_key] = {}

        for horizon in FORECAST_HORIZONS:
            horizon_ts = anchor_ts_utc + pd.Timedelta(minutes=freq_minutes * horizon)
            same4w = baseline_value_at_timestamp(
                baseline_series, horizon_ts, slots_per_day, freq_minutes
            )
            lag48 = baseline_value_at_timestamp_shifted(
                baseline_series,
                horizon_ts,
                slots_per_day,
                freq_minutes,
                lag48_shift_slots,
            )
            strong_blend = (same4w + lag48) / 2.0

            ml_pred = submission_map.get((route_id, horizon), 0.0)
            agreement_val = agreement_signal(
                ml_pred,
                strong_blend,
                mode=args.agreement_mode,
            )
            horizon_val = horizon_signal(horizon)

            office_row = office_lookup.get((office_id, horizon))
            horizon_row = horizon_lookup.get(horizon, {})
            if office_row is not None:
                raw_win_rate = float(office_row["win_rate"])
                raw_expected_loss = float(office_row["expected_loss_wape_like"])
                raw_rows = float(office_row["rows"])
            else:
                raw_win_rate = float(horizon_row.get("proxy_win_rate", global_win_rate))
                raw_expected_loss = float(
                    horizon_row.get("proxy_expected_loss_wape_like", 0.05)
                )
                raw_rows = 0.0

            office_error_hist_val = office_error_signal(raw_expected_loss)

            component_scores = {
                "horizon": horizon_val,
                "stability": stability_val,
                "agreement_with_blend": agreement_val,
                "route_error_history": route_error_hist_signal,
                "office_error_history": office_error_hist_val,
                "history_completeness": completeness_val,
                "freshness": freshness_val,
            }

            trust = compute_trust(
                component_scores=component_scores,
                empirical_win_rate_value=raw_win_rate,
                empirical_rows=raw_rows,
                horizon_global_win_rate=float(horizon_row.get("proxy_win_rate", global_win_rate)),
            )

            row = {
                "route_id": route_id,
                "office_from_id": office_id,
                "horizon": horizon,
                "horizon_key": f"h{horizon}",
                "agreement_mode": args.agreement_mode,
                "trust_score": trust.trust_score,
                "trust_score_pct": trust.trust_score_pct,
                "reason_short": trust.reason_short,
                "reason_full": trust.reason_full,
                "empirical_win_rate": trust.empirical_win_rate,
                "empirical_weight": trust.empirical_weight,
                "base_components_score": trust.base_components_score,
                "component_horizon": component_scores["horizon"],
                "component_stability": component_scores["stability"],
                "component_agreement_with_blend": component_scores["agreement_with_blend"],
                "component_route_error_history": component_scores["route_error_history"],
                "component_office_error_history": component_scores["office_error_history"],
                "component_history_completeness": component_scores["history_completeness"],
                "component_freshness": component_scores["freshness"],
                "ml_prediction": ml_pred,
                "strong_baseline_blend": strong_blend,
                "abs_delta_ml_vs_blend": abs(ml_pred - strong_blend),
                "route_wape_7d": route_wape,
                "office_expected_loss_wape_like": raw_expected_loss,
                "office_proxy_rows": raw_rows,
                "history_rows": int(route_history.history_rows) if route_history is not None else 0,
                "freshness_minutes": freshness_minutes,
                "history_last_timestamp_utc": route_last_ts.isoformat(),
            }
            rows.append(row)

            lookup_payload[route_key][f"h{horizon}"] = {
                "trust_score": float(trust.trust_score),
                "trust_score_pct": float(trust.trust_score_pct),
                "reason_short": trust.reason_short,
                "reason_full": trust.reason_full,
                "horizon": int(horizon),
                "office_from_id": int(office_id),
                "agreement_mode": args.agreement_mode,
                "source": "stage3_artifact",
            }

    trust_df_raw = pd.DataFrame(rows).sort_values(["route_id", "horizon"]).reset_index(drop=True)
    trust_df, policy_calibration_report = apply_policy_calibration(
        trust_df_raw,
        mode=args.policy_calibration,
    )
    refresh_lookup_payload_from_df(
        trust_df=trust_df,
        lookup_payload=lookup_payload,
        agreement_mode=args.agreement_mode,
    )

    trust_csv_path = args.output_dir / "trust_stage3_route_horizon.csv"
    trust_json_path = args.output_dir / "trust_stage3_route_horizon.json"
    trust_lookup_path = args.output_dir / "trust_stage3_lookup.json"
    trust_summary_path = args.output_dir / "trust_stage3_summary.json"

    trust_df.to_csv(trust_csv_path, index=False)
    trust_json_path.write_text(
        json.dumps(
            {
                "generated_at_utc": now_utc_iso(),
                "rows": trust_df.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    trust_lookup_path.write_text(
        json.dumps(
            {
                "generated_at_utc": now_utc_iso(),
                "anchor_timestamp_utc": anchor_ts_utc.isoformat(),
                "agreement_mode": args.agreement_mode,
                "policy_calibration_mode": args.policy_calibration,
                "weights": COMPONENT_WEIGHTS,
                "route_horizon": lookup_payload,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    if args.agreement_mode == AGREEMENT_MODE_LEGACY:
        agreement_mode_note = (
            "legacy: direct agreement with blend baseline (can over-penalize useful ML deviations)."
        )
    elif args.agreement_mode == AGREEMENT_MODE_GUARDRAIL:
        agreement_mode_note = (
            "guardrail: neutral in normal zone, penalizes only large disagreement with blend baseline."
        )
    else:
        agreement_mode_note = (
            "neutral: agreement component fixed at 0.5 per ablation result; signal does not affect trust score."
        )

    summary_payload = {
        "generated_at_utc": now_utc_iso(),
        "stage": "stage3_trust_score",
        "anchor_timestamp_input": args.anchor_timestamp,
        "anchor_timestamp_utc": anchor_ts_utc.isoformat(),
        "metric_role": "policy_confidence_proxy",
        "official_metric_guardrail": "Official competition metric remains WAPE + |Relative Bias|.",
        "agreement_mode": args.agreement_mode,
        "agreement_mode_note": agreement_mode_note,
        "policy_calibration": policy_calibration_report,
        "weights": COMPONENT_WEIGHTS,
        "signals": {
            "horizon": "Higher trust for closer horizons.",
            "stability": "Route-level variability over recent history (lower CV -> higher trust).",
            "agreement_with_blend": (
                "Agreement with strong baseline blend_roll48_same7d at horizon slot "
                f"(mode={args.agreement_mode})."
            ),
            "route_error_history": "Route historical WAPE profile (lower error -> higher trust).",
            "office_error_history": "Office x horizon expected loss vs strong baseline from stage2 proxy.",
            "history_completeness": "Coverage of route history in history_tail.",
            "freshness": "Staleness of latest route data relative to anchor timestamp.",
            "empirical_win_rate": "Historical office x horizon win-rate of ML vs strong baseline.",
        },
        "global_stats": {
            "rows": int(len(trust_df)),
            "routes": int(trust_df["route_id"].nunique()),
            "horizons": int(trust_df["horizon"].nunique()),
            "trust_score_pct_mean": float(trust_df["trust_score_pct"].mean()),
            "trust_score_pct_p10": float(trust_df["trust_score_pct"].quantile(0.10)),
            "trust_score_pct_p50": float(trust_df["trust_score_pct"].quantile(0.50)),
            "trust_score_pct_p90": float(trust_df["trust_score_pct"].quantile(0.90)),
        },
        "inputs": {
            "submission_path": str(submission_path),
            "inference_summary_path": str(args.inference_summary_path),
            "history_tail_path": str(history_tail_path),
            "latest_snapshot_path": str(latest_snapshot_path),
            "route_wape_path": str(route_wape_path),
            "stage2_office_proxy_path": str(args.stage2_office_proxy_path),
            "stage2_by_horizon_path": str(args.stage2_by_horizon_path),
        },
        "output_files": {
            "trust_route_horizon_csv": str(trust_csv_path),
            "trust_route_horizon_json": str(trust_json_path),
            "trust_lookup_json": str(trust_lookup_path),
            "trust_summary_json": str(trust_summary_path),
        },
    }
    trust_summary_path.write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("Stage 3 trust artifact completed.")
    print(f"trust lookup: {trust_lookup_path.resolve()}")
    print(f"trust summary: {trust_summary_path.resolve()}")


if __name__ == "__main__":
    main()
