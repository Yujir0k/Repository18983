import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd


CALIB_DAYS = 7
HORIZONS = list(range(1, 11))
EPS = 1e-6
HIST_EXPECTED_ROWS = 672
TARGET_LAGS = [1, 2, 3, 4, 6, 8, 12, 24, 48, 96, 144, 192, 240, 288, 336, 672, 1008, 1344]
TARGET_ROLL_WINDOWS = [2, 4, 8, 16, 48, 96]

W = {
    "horizon": 0.20,
    "stability": 0.16,
    "agreement": 0.18,
    "route_error": 0.18,
    "office_error": 0.14,
    "completeness": 0.08,
    "freshness": 0.06,
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return float(max(lo, min(hi, v)))


def safe_div(a: float, b: float) -> float:
    return float(a / b) if b > 0 else float("nan")


def get_train_window_days(h: int) -> int:
    if h <= 3:
        return 28
    if h <= 7:
        return 42
    return 56


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


def ensure_cols(df: pd.DataFrame, cols: list[str], name: str) -> None:
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise ValueError(f"Missing columns in {name}: {miss}")


def prep_features(train: pd.DataFrame, enable_status_semantics: bool = False) -> pd.DataFrame:
    feat = train.copy()
    feat["timestamp"] = pd.to_datetime(feat["timestamp"])
    feat = feat.sort_values(["route_id", "timestamp"]).reset_index(drop=True)
    status_cols = [c for c in feat.columns if c.startswith("status_")]
    if not status_cols:
        raise ValueError("No status_* columns found.")

    feat["hour"] = feat["timestamp"].dt.hour.astype("int8")
    feat["minute"] = feat["timestamp"].dt.minute.astype("int8")
    feat["dow"] = feat["timestamp"].dt.dayofweek.astype("int8")
    feat["status_sum"] = feat[status_cols].sum(axis=1).astype("float32")
    feat["status_max"] = feat[status_cols].max(axis=1).astype("float32")

    grp = feat.groupby("route_id", sort=False)

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
            feat[f"{col_name}_lag_1"] = grp[col_name].shift(1).astype("float32")
            feat[f"{col_name}_delta_1"] = (feat[col_name] - feat[f"{col_name}_lag_1"]).astype(
                "float32"
            )
            feat[f"{col_name}_roll_mean_4"] = (
                grp[col_name].rolling(4).mean().reset_index(level=0, drop=True).astype("float32")
            )
    for lag in TARGET_LAGS:
        feat[f"target_lag_{lag}"] = grp["target_2h"].shift(lag).astype("float32")
    for win in TARGET_ROLL_WINDOWS:
        feat[f"target_roll_mean_{win}"] = (
            grp["target_2h"].rolling(win).mean().reset_index(level=0, drop=True).astype("float32")
        )
        feat[f"target_roll_std_{win}"] = (
            grp["target_2h"].rolling(win).std().reset_index(level=0, drop=True).astype("float32")
        )

    lag7 = [f"target_lag_{lag}" for lag in [48, 96, 144, 192, 240, 288, 336]]
    feat["same_slot_mean_7d"] = feat[lag7].mean(axis=1).astype("float32")
    feat["base_blend_roll48_same7d"] = (
        feat[["target_roll_mean_48", "same_slot_mean_7d"]].mean(axis=1).astype("float32")
    )
    feat["baseline_same_4w"] = (
        feat[[f"target_lag_{lag}" for lag in [336, 672, 1008, 1344]]].mean(axis=1).astype("float32")
    )

    prev_ts = grp["timestamp"].shift(1)
    gap = (feat["timestamp"] - prev_ts).dt.total_seconds() / 60.0
    feat["freshness_minutes_proxy"] = np.clip(gap.fillna(30.0) - 30.0, 0.0, None).astype("float32")
    return feat


def load_route_stats(history_tail_path: Path) -> tuple[dict[int, float], dict[int, float]]:
    h = pd.read_parquet(history_tail_path, columns=["route_id", "timestamp", "target_2h"])
    h = h.sort_values(["route_id", "timestamp"]).reset_index(drop=True)
    st: dict[int, float] = {}
    cp: dict[int, float] = {}
    for rid, g in h.groupby("route_id", sort=False):
        v = g["target_2h"].astype(float).to_numpy()
        v = v[np.isfinite(v)]
        if v.size == 0:
            cv = 2.0
        else:
            tail = v[-96:] if v.size >= 96 else v
            cv = float(np.std(tail) / (np.mean(tail) + 1.0))
        st[int(rid)] = clamp(1.0 / (1.0 + cv))
        cp[int(rid)] = clamp(float(len(g)) / float(HIST_EXPECTED_ROWS))
    return st, cp


def load_route_wape(path: Path) -> dict[int, float]:
    raw = read_json(path)
    out: dict[int, float] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                out[int(float(k))] = max(0.0, float(v))
            except Exception:
                continue
    return out


def load_office_proxy(office_csv: Path, by_h_csv: Path) -> dict:
    off = pd.read_csv(office_csv)
    byh = pd.read_csv(by_h_csv)

    win_map: dict[int, dict[int, float]] = {h: {} for h in HORIZONS}
    loss_map: dict[int, dict[int, float]] = {h: {} for h in HORIZONS}
    rows_map: dict[int, dict[int, float]] = {h: {} for h in HORIZONS}
    if not off.empty:
        off["horizon"] = off["horizon"].astype(int)
        off["office_from_id"] = off["office_from_id"].astype(int)
        for r in off.itertuples(index=False):
            h = int(r.horizon)
            o = int(r.office_from_id)
            win_map[h][o] = float(r.win_rate)
            loss_map[h][o] = float(r.expected_loss_wape_like)
            rows_map[h][o] = float(r.rows)

    h_win, h_loss = {}, {}
    for r in byh.itertuples(index=False):
        h = int(r.horizon)
        h_win[h] = float(getattr(r, "proxy_win_rate", np.nan))
        h_loss[h] = float(getattr(r, "proxy_expected_loss_wape_like", np.nan))
    g_win = float(np.nanmean(np.array(list(h_win.values()), dtype="float64")))
    g_loss = float(np.nanmean(np.array(list(h_loss.values()), dtype="float64")))
    if not math.isfinite(g_win):
        g_win = 0.5
    if not math.isfinite(g_loss):
        g_loss = 0.05
    for h in HORIZONS:
        if h not in h_win or not math.isfinite(h_win[h]):
            h_win[h] = g_win
        if h not in h_loss or not math.isfinite(h_loss[h]):
            h_loss[h] = g_loss
    return {
        "win_map": win_map,
        "loss_map": loss_map,
        "rows_map": rows_map,
        "h_win": h_win,
        "h_loss": h_loss,
        "g_win": g_win,
        "g_loss": g_loss,
    }


def map_vals(keys: np.ndarray, m: dict[int, float], d: float) -> np.ndarray:
    if not m:
        return np.full(keys.shape[0], d, dtype="float32")
    return pd.Series(keys, copy=False).map(m).fillna(d).to_numpy(dtype="float32")


def auc_binned(pos: np.ndarray, neg: np.ndarray) -> float:
    p = float(np.sum(pos))
    n = float(np.sum(neg))
    if p <= 0 or n <= 0:
        return float("nan")
    neg_before, acc = 0.0, 0.0
    for i in range(pos.shape[0]):
        acc += float(pos[i]) * neg_before + 0.5 * float(pos[i]) * float(neg[i])
        neg_before += float(neg[i])
    return float(acc / (p * n))


class Agg:
    def __init__(self, name: str, thr: float, hi: float, lo: float, bins: int):
        self.name = name
        self.thr = thr
        self.hi = hi
        self.lo = lo
        self.bins = bins
        self.rows = 0
        self.score_sum = 0.0
        self.win_sum = 0.0
        self.brier_sum = 0.0
        self.logloss_sum = 0.0
        self.y_sum = 0.0
        self.loss_sum = 0.0
        self.auto_rows = self.man_rows = 0
        self.auto_win = self.man_win = 0.0
        self.auto_y = self.man_y = 0.0
        self.auto_loss = self.man_loss = 0.0
        self.pos = np.zeros(bins, dtype="float64")
        self.neg = np.zeros(bins, dtype="float64")

    def update(self, s: np.ndarray, w: np.ndarray, loss: np.ndarray, y: np.ndarray) -> None:
        if s.size == 0:
            return
        s = np.clip(s.astype("float64"), 0.0, 1.0)
        w = w.astype("float64")
        loss = loss.astype("float64")
        y = y.astype("float64")
        n = int(s.shape[0])
        self.rows += n
        self.score_sum += float(np.sum(s))
        self.win_sum += float(np.sum(w))
        self.brier_sum += float(np.sum((s - w) ** 2))
        s_clip = np.clip(s, EPS, 1.0 - EPS)
        self.logloss_sum += float(-np.sum(w * np.log(s_clip) + (1.0 - w) * np.log1p(-s_clip)))
        self.y_sum += float(np.sum(y))
        self.loss_sum += float(np.sum(loss))

        am = s >= self.thr
        mm = ~am
        if np.any(am):
            self.auto_rows += int(np.sum(am))
            self.auto_win += float(np.sum(w[am]))
            self.auto_y += float(np.sum(y[am]))
            self.auto_loss += float(np.sum(loss[am]))
        if np.any(mm):
            self.man_rows += int(np.sum(mm))
            self.man_win += float(np.sum(w[mm]))
            self.man_y += float(np.sum(y[mm]))
            self.man_loss += float(np.sum(loss[mm]))

        b = np.clip((s * float(self.bins - 1)).astype(np.int32), 0, self.bins - 1)
        self.pos += np.bincount(b, weights=w, minlength=self.bins)
        self.neg += np.bincount(b, weights=(1.0 - w), minlength=self.bins)

    def row(self) -> dict:
        auto_wr = safe_div(self.auto_win, float(self.auto_rows))
        man_wr = safe_div(self.man_win, float(self.man_rows))
        return {
            "variant": self.name,
            "rows": int(self.rows),
            "mean_score": safe_div(self.score_sum, float(self.rows)),
            "win_rate": safe_div(self.win_sum, float(self.rows)),
            "brier": safe_div(self.brier_sum, float(self.rows)),
            "logloss": safe_div(self.logloss_sum, float(self.rows)),
            "auc_binned": auc_binned(self.pos, self.neg),
            "expected_loss_wape_like": safe_div(self.loss_sum, self.y_sum),
            "auto_threshold": float(self.thr),
            "auto_rows": int(self.auto_rows),
            "auto_share": safe_div(float(self.auto_rows), float(self.rows)),
            "auto_win_rate": auto_wr,
            "auto_expected_loss_wape_like": safe_div(self.auto_loss, self.auto_y),
            "manual_rows": int(self.man_rows),
            "manual_share": safe_div(float(self.man_rows), float(self.rows)),
            "manual_win_rate": man_wr,
            "manual_expected_loss_wape_like": safe_div(self.man_loss, self.man_y),
            "auto_vs_manual_win_lift": auto_wr - man_wr if math.isfinite(auto_wr) and math.isfinite(man_wr) else float("nan"),
        }


def main() -> None:
    p = argparse.ArgumentParser(description="Stage3 trust ablation report.")
    p.add_argument("--train-path", type=Path, default=Path("train_team_track.parquet"))
    p.add_argument("--artifact-root", type=Path, default=None)
    p.add_argument("--stage2-office-proxy-path", type=Path, default=Path("model/analytics_stage2_office_horizon_proxy.csv"))
    p.add_argument("--stage2-by-horizon-path", type=Path, default=Path("model/analytics_stage2_by_horizon.csv"))
    p.add_argument("--route-wape-path", type=Path, default=None)
    p.add_argument("--history-tail-path", type=Path, default=None)
    p.add_argument("--output-dir", type=Path, default=Path("model"))
    p.add_argument("--auto-threshold", type=float, default=0.65)
    p.add_argument("--auc-bins", type=int, default=200)
    args = p.parse_args()

    if args.artifact_root is None:
        args.artifact_root = discover_artifact_root()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    train = pd.read_parquet(args.train_path)
    ensure_cols(train, ["route_id", "office_from_id", "timestamp", "target_2h"], str(args.train_path))
    feat_schema = read_json(args.artifact_root / "feature_schema.json")
    feat = prep_features(
        train,
        enable_status_semantics=bool(feat_schema.get("enable_status_semantics", False)),
    )

    base_col = str(feat_schema.get("base_col", "base_blend_roll48_same7d"))
    if base_col not in feat.columns:
        raise ValueError(f"Base column '{base_col}' not found.")

    route_wape_path = args.route_wape_path or (
        args.artifact_root / "route_wape_7d.json" if (args.artifact_root / "route_wape_7d.json").exists() else Path("model/route_wape_7d.json")
    )
    history_tail_path = args.history_tail_path or (
        args.artifact_root / "history_tail.parquet" if (args.artifact_root / "history_tail.parquet").exists() else Path("model/history_tail.parquet")
    )
    route_wape = load_route_wape(route_wape_path)
    g_route_wape = float(np.mean(list(route_wape.values()))) if route_wape else 0.2485
    route_err = {k: float(np.clip(1.0 - (v / 0.60), 0.0, 1.0)) for k, v in route_wape.items()}
    d_route_err = float(np.clip(1.0 - (g_route_wape / 0.60), 0.0, 1.0))
    stability, completeness = load_route_stats(history_tail_path)
    office_proxy = load_office_proxy(args.stage2_office_proxy_path, args.stage2_by_horizon_path)

    off_scales = {}
    sc_path = args.artifact_root / "calibration" / "office_scales.parquet"
    if sc_path.exists():
        sc = pd.read_parquet(sc_path)
        if not sc.empty:
            sc["horizon"] = sc["horizon"].astype(int)
            sc["office_from_id"] = sc["office_from_id"].astype(int)
            for h in sorted(sc["horizon"].unique()):
                off_scales[int(h)] = sc[sc["horizon"] == h].set_index("office_from_id")["scale"].astype(float)

    variants = ["full", "no_agreement_with_blend", "no_horizon", "no_stability", "no_route_error_history", "no_office_error_history", "no_history_completeness", "no_freshness", "no_empirical_win_rate"]
    ab_map = {
        "no_agreement_with_blend": ("agreement", 0.18),
        "no_horizon": ("horizon", 0.20),
        "no_stability": ("stability", 0.16),
        "no_route_error_history": ("route_error", 0.18),
        "no_office_error_history": ("office_error", 0.14),
        "no_history_completeness": ("completeness", 0.08),
        "no_freshness": ("freshness", 0.06),
    }
    agg = {v: Agg(v, float(args.auto_threshold), 0.75, 0.45, int(args.auc_bins)) for v in variants}
    agg_h = {(v, h): Agg(v, float(args.auto_threshold), 0.75, 0.45, int(args.auc_bins)) for v in ["full", "no_agreement_with_blend"] for h in HORIZONS}

    train_end = feat["timestamp"].max()
    calib_start = train_end - pd.Timedelta(days=CALIB_DAYS) + pd.Timedelta(minutes=30)
    route_target = feat.groupby("route_id", sort=False)["target_2h"]
    model_dir = args.artifact_root / "models"
    calib_dir = args.artifact_root / "calibration"
    total_rows = 0

    for h in HORIZONS:
        meta = read_json(calib_dir / f"h{h:02d}_meta.json")
        feats = list(meta["feature_cols"])
        cats = list(meta.get("categorical_cols", []))
        fut = list(meta.get("future_feature_names", [f"future_hour_{h}", f"future_minute_{h}", f"future_dow_{h}"]))
        fut_ts = feat["timestamp"] + pd.Timedelta(minutes=30 * h)
        feat[fut[0]] = fut_ts.dt.hour.astype("int8")
        feat[fut[1]] = fut_ts.dt.minute.astype("int8")
        feat[fut[2]] = fut_ts.dt.dayofweek.astype("int8")

        y_f = route_target.shift(-h).astype("float32")
        fit_start = train_end - pd.Timedelta(days=get_train_window_days(h))
        m = (feat["timestamp"] >= fit_start) & (feat["timestamp"] >= calib_start) & y_f.notna() & feat[base_col].notna() & feat["baseline_same_4w"].notna()
        if not bool(m.any()):
            continue

        need = sorted(set(feats + ["office_from_id", "route_id", "freshness_minutes_proxy"]))
        d = feat.loc[m, need].copy()
        y = y_f.loc[m].to_numpy(dtype="float32")
        strong = feat.loc[m, base_col].to_numpy(dtype="float32")
        for c in cats:
            if c in d.columns:
                d[c] = d[c].astype("category")

        booster = lgb.Booster(model_file=str(model_dir / str(meta["model_path"])))
        resid = booster.predict(d[feats])
        raw = np.clip(strong + resid, 0, None)
        post_scale = float(meta.get("post_scale", 1.0))
        glob_scale = float(meta.get("global_scale", 1.0))
        mode = str(meta.get("calib_mode", "global"))
        if mode == "office_shrink":
            om = off_scales.get(h)
            if om is None:
                rs = np.full(raw.shape[0], glob_scale, dtype="float32")
            else:
                rs = d["office_from_id"].astype("int64").map(om).fillna(glob_scale).to_numpy(dtype="float32")
            ml = raw.astype("float32") * rs * np.float32(post_scale)
        else:
            ml = raw.astype("float32") * np.float32(post_scale)
        ml = np.clip(ml, 0, None).astype("float32")

        abs_ml = np.abs(ml - y).astype("float32")
        abs_str = np.abs(strong - y).astype("float32")
        loss = np.clip(abs_ml - abs_str, 0, None).astype("float32")
        win = (abs_ml <= abs_str).astype("float32")
        rid = d["route_id"].to_numpy(dtype="int64")
        oid = d["office_from_id"].to_numpy(dtype="int64")

        c_h = np.full(ml.shape[0], clamp(1.0 - (float(h - 1) / 9.0)), dtype="float32")
        c_s = map_vals(rid, stability, 0.5)
        c_c = map_vals(rid, completeness, 0.5)
        c_r = map_vals(rid, route_err, d_route_err)
        den = np.maximum(1.0, np.abs(strong) + 25.0)
        c_a = np.clip(1.0 - (np.abs(ml - strong) / den), 0.0, 1.0).astype("float32")
        c_f = np.clip(np.exp(-np.maximum(d["freshness_minutes_proxy"].to_numpy(dtype="float32"), 0.0) / 180.0), 0.0, 1.0).astype("float32")
        ow = map_vals(oid, office_proxy["win_map"].get(h, {}), office_proxy["h_win"][h])
        ol = map_vals(oid, office_proxy["loss_map"].get(h, {}), office_proxy["h_loss"][h])
        orows = map_vals(oid, office_proxy["rows_map"].get(h, {}), 0.0)
        c_o = np.clip(1.0 - (ol / 0.12), 0.0, 1.0).astype("float32")
        ew = np.clip(orows / 2000.0, 0.0, 1.0).astype("float32")
        emp = (ew * ow + (1.0 - ew) * np.float32(office_proxy["h_win"][h])).astype("float32")

        base = (np.float32(W["horizon"]) * c_h + np.float32(W["stability"]) * c_s + np.float32(W["agreement"]) * c_a + np.float32(W["route_error"]) * c_r + np.float32(W["office_error"]) * c_o + np.float32(W["completeness"]) * c_c + np.float32(W["freshness"]) * c_f)
        full = np.clip(0.55 * base + 0.45 * emp, 0.0, 1.0).astype("float32")

        scores = {"full": full}
        comp = {"horizon": c_h, "stability": c_s, "agreement": c_a, "route_error": c_r, "office_error": c_o, "completeness": c_c, "freshness": c_f}
        for vn, (cn, ww) in ab_map.items():
            scores[vn] = np.clip(full - (0.55 * np.float32(ww) * (comp[cn] - 0.5)), 0.0, 1.0).astype("float32")
        scores["no_empirical_win_rate"] = np.clip(full - (0.45 * (emp - 0.5)), 0.0, 1.0).astype("float32")

        for vn in variants:
            agg[vn].update(scores[vn], win, loss, y)
        agg_h[("full", h)].update(scores["full"], win, loss, y)
        agg_h[("no_agreement_with_blend", h)].update(scores["no_agreement_with_blend"], win, loss, y)
        total_rows += int(y.shape[0])
        print(f"h{h}: rows={y.shape[0]}")

    df = pd.DataFrame([agg[v].row() for v in variants])
    full_row = df[df["variant"] == "full"].iloc[0]
    for m in ["brier", "logloss", "auc_binned", "auto_win_rate", "auto_expected_loss_wape_like", "auto_vs_manual_win_lift"]:
        df[f"delta_{m}_vs_full"] = df[m].astype(float) - float(full_row[m])
    df = df.sort_values(["delta_auto_expected_loss_wape_like_vs_full", "delta_brier_vs_full"], ascending=[False, False]).reset_index(drop=True)

    by_h = []
    for h in HORIZONS:
        for vn in ["full", "no_agreement_with_blend"]:
            r = agg_h[(vn, h)].row()
            r["horizon"] = int(h)
            by_h.append(r)
    by_h_df = pd.DataFrame(by_h).sort_values(["horizon", "variant"]).reset_index(drop=True)

    out_v = args.output_dir / "trust_stage3_ablation_variants.csv"
    out_h = args.output_dir / "trust_stage3_ablation_by_horizon.csv"
    out_s = args.output_dir / "trust_stage3_ablation_summary.json"
    df.to_csv(out_v, index=False)
    by_h_df.to_csv(out_h, index=False)

    def pick(vn: str, m: str) -> float:
        p2 = df[df["variant"] == vn]
        return float(p2.iloc[0][m]) if not p2.empty else float("nan")

    summary = {
        "generated_at_utc": now_utc_iso(),
        "stage": "stage3_trust_ablation",
        "scope": "Calibration-window backtest rows with real target_2h.",
        "official_metric_guardrail": "Official KPI remains WAPE + |Relative Bias| from stage1.",
        "inputs": {
            "train_path": str(args.train_path),
            "artifact_root": str(args.artifact_root),
            "stage2_office_proxy_path": str(args.stage2_office_proxy_path),
            "stage2_by_horizon_path": str(args.stage2_by_horizon_path),
            "route_wape_path": str(route_wape_path),
            "history_tail_path": str(history_tail_path),
            "base_col": base_col,
            "rows_evaluated": int(total_rows),
        },
        "formula": {
            "final": "clip(0.55 * weighted_components + 0.45 * empirical_blended, 0, 1)",
            "weights": W,
        },
        "agreement_ablation_result": {
            "full_auto_expected_loss_wape_like": pick("full", "auto_expected_loss_wape_like"),
            "no_agreement_auto_expected_loss_wape_like": pick("no_agreement_with_blend", "auto_expected_loss_wape_like"),
            "delta_auto_expected_loss_wape_like": pick("no_agreement_with_blend", "auto_expected_loss_wape_like") - pick("full", "auto_expected_loss_wape_like"),
            "full_auc_binned": pick("full", "auc_binned"),
            "no_agreement_auc_binned": pick("no_agreement_with_blend", "auc_binned"),
            "delta_auc_binned": pick("no_agreement_with_blend", "auc_binned") - pick("full", "auc_binned"),
            "full_auto_vs_manual_win_lift": pick("full", "auto_vs_manual_win_lift"),
            "no_agreement_auto_vs_manual_win_lift": pick("no_agreement_with_blend", "auto_vs_manual_win_lift"),
            "delta_auto_vs_manual_win_lift": pick("no_agreement_with_blend", "auto_vs_manual_win_lift") - pick("full", "auto_vs_manual_win_lift"),
        },
        "output_files": {
            "variants_csv": str(out_v),
            "by_horizon_csv": str(out_h),
            "summary_json": str(out_s),
        },
    }
    out_s.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Stage 3 ablation report completed.")
    print(f"variants: {out_v.resolve()}")
    print(f"by_horizon: {out_h.resolve()}")
    print(f"summary: {out_s.resolve()}")


if __name__ == "__main__":
    main()
