import { performance } from "node:perf_hooks"
import fs from "node:fs"
import path from "node:path"

import { NextResponse } from "next/server"

import { dashboardConfig } from "@/lib/dashboard-config"
import { calculateDispatchRegistry } from "@/lib/metrics-calculator"
import { confidenceFromWape, getVolumeDeltaStatus } from "@/lib/utils/dispatch-status"
import type { DispatchRegistryRow, SimulationStrategy, WaveSelection } from "@/types/dashboard-metrics"

type Stage1MacroRow = {
  variant: string
  mean_score?: number
  mean_wape?: number
  mean_rbias?: number
}

type Stage1SummaryFile = {
  generated_at_utc?: string
  averages_macro?: Stage1MacroRow[]
}

type Stage2ByHorizonRow = {
  horizon?: number
  eval_rows?: number
  ml_score?: number
  ml_wape?: number
  ml_rbias?: number
  strong_score?: number
  strong_wape?: number
  strong_rbias?: number
  primitive_score?: number
  primitive_wape?: number
  primitive_rbias?: number
  proxy_win_rate?: number
  proxy_tie_rate?: number
  proxy_expected_loss_abs_mean?: number
  proxy_expected_loss_wape_like?: number
  proxy_delta_abs_err_mean?: number
}

type Stage2ByHorizonFile = {
  generated_at_utc?: string
  rows?: Stage2ByHorizonRow[]
}

type Stage2OfficeCell = {
  rows?: number
  win_rate?: number
  tie_rate?: number
  delta_abs_err_mean?: number
  expected_loss_abs_mean?: number
  expected_loss_wape_like?: number
}

type Stage2OfficeRow = {
  office_from_id?: number | string
  rows_total?: number
  cells?: Record<string, Stage2OfficeCell>
}

type Stage2OfficeFile = {
  matrix?: {
    horizons?: number[]
    rows?: Stage2OfficeRow[]
  }
}

type TrustStage3SummaryFile = {
  generated_at_utc?: string
  anchor_timestamp_utc?: string
  metric_role?: string
  official_metric_guardrail?: string
  agreement_mode?: string
  agreement_mode_note?: string
  global_stats?: {
    rows?: number
    routes?: number
    horizons?: number
    trust_score_pct_mean?: number
    trust_score_pct_p10?: number
    trust_score_pct_p50?: number
    trust_score_pct_p90?: number
  }
  policy_calibration?: {
    mode?: string
    applied?: boolean
    note?: string
    before_global?: {
      min?: number
      p10?: number
      p50?: number
      p90?: number
      max?: number
      share_ge_70?: number
      share_ge_85?: number
    }
    after_global?: {
      min?: number
      p10?: number
      p50?: number
      p90?: number
      max?: number
      share_ge_70?: number
      share_ge_85?: number
    }
  }
  weights?: Record<string, number>
}

type TrustStage3RouteRow = {
  freshness_minutes?: number
}

type TrustStage3RouteFile = {
  generated_at_utc?: string
  rows?: TrustStage3RouteRow[]
}

type TrustStage3AblationSummaryFile = {
  generated_at_utc?: string
  agreement_ablation_result?: {
    full_auto_expected_loss_wape_like?: number
    no_agreement_auto_expected_loss_wape_like?: number
    delta_auto_expected_loss_wape_like?: number
    full_auc_binned?: number
    no_agreement_auc_binned?: number
    delta_auc_binned?: number
    full_auto_vs_manual_win_lift?: number
    no_agreement_auto_vs_manual_win_lift?: number
    delta_auto_vs_manual_win_lift?: number
  }
}

type LegacyMetricsSummaryFile = {
  variant?: string
  mean_calib_metric?: number
  mean_wape?: number
  mean_rbias?: number
  mean_mae?: number
  mean_rmse?: number
  mean_smape?: number
}

type ParsedCsvRow = Record<string, string>

type OfficialVariantSummary = {
  score: number
  wape: number
  rbias: number
}

type StatusKey = "ok" | "review" | "drop" | "critical"

const MODEL_DIR_CANDIDATES = [
  process.env.MODEL_DIR?.trim(),
  path.resolve(process.cwd(), "model"),
  path.resolve(process.cwd(), "../model"),
].filter((candidate): candidate is string => Boolean(candidate && candidate.length > 0))

const WAVE_OPTIONS: WaveSelection[] = [1, 2]
const STRATEGY_OPTIONS: SimulationStrategy[] = ["economy", "balance", "reliable"]

const STRONG_VARIANT_NAME = "strong_baseline_blend_roll48_same7d"
const PRIMITIVE_VARIANT_NAME = "primitive_baseline_same_4w"

const LATENCY_WINDOW = 200
const inferenceLatencySamplesMs: number[] = []

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value))
}

function safeRatio(numerator: number, denominator: number): number {
  return denominator > 0 ? numerator / denominator : 0
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const rank = clamp((p / 100) * (sorted.length - 1), 0, sorted.length - 1)
  const low = Math.floor(rank)
  const high = Math.ceil(rank)
  if (low === high) return sorted[low]
  const weight = rank - low
  return sorted[low] * (1 - weight) + sorted[high] * weight
}

function mean(values: number[]): number {
  if (values.length === 0) return 0
  return values.reduce((acc, value) => acc + value, 0) / values.length
}

function trackInferenceLatency(sampleMs: number) {
  inferenceLatencySamplesMs.push(sampleMs)
  if (inferenceLatencySamplesMs.length > LATENCY_WINDOW) {
    inferenceLatencySamplesMs.splice(0, inferenceLatencySamplesMs.length - LATENCY_WINDOW)
  }
}

function getLatencyStats() {
  return {
    p50: Number(percentile(inferenceLatencySamplesMs, 50).toFixed(2)),
    p95: Number(percentile(inferenceLatencySamplesMs, 95).toFixed(2)),
    mean: Number(mean(inferenceLatencySamplesMs).toFixed(2)),
    min: Number((inferenceLatencySamplesMs.length > 0 ? Math.min(...inferenceLatencySamplesMs) : 0).toFixed(2)),
    max: Number((inferenceLatencySamplesMs.length > 0 ? Math.max(...inferenceLatencySamplesMs) : 0).toFixed(2)),
    samples: inferenceLatencySamplesMs.length,
    windowSize: LATENCY_WINDOW,
  }
}

function resolveModelDir(): string {
  for (const candidate of MODEL_DIR_CANDIDATES) {
    if (!fs.existsSync(candidate)) continue
    const stat = fs.statSync(candidate)
    if (stat.isDirectory()) return candidate
  }
  throw new Error("Не удалось найти папку model для загрузки аналитических артефактов.")
}

function readJsonFileSafe<T>(filePath: string): T | null {
  if (!fs.existsSync(filePath)) return null
  try {
    const raw = fs.readFileSync(filePath, "utf8")
    return JSON.parse(raw) as T
  } catch {
    try {
      const raw = fs.readFileSync(filePath, "utf8")
      // Some Python-exported artifacts may contain NaN/Infinity tokens that are invalid for strict JSON.parse.
      // Replace them with null to keep the payload parseable and avoid dropping the whole artifact.
      const sanitized = raw
        .replace(/\b-?Infinity\b/g, "null")
        .replace(/\bNaN\b/g, "null")
      return JSON.parse(sanitized) as T
    } catch {
      return null
    }
  }
}

function parseCsvLine(line: string): string[] {
  const cells: string[] = []
  let current = ""
  let inQuotes = false

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i]

    if (char === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"'
        i += 1
        continue
      }
      inQuotes = !inQuotes
      continue
    }

    if (char === "," && !inQuotes) {
      cells.push(current)
      current = ""
      continue
    }

    current += char
  }

  cells.push(current)
  return cells
}

function parseCsvText(csvText: string): ParsedCsvRow[] {
  const lines = csvText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

  if (lines.length <= 1) return []

  const headers = parseCsvLine(lines[0]).map((header) => header.trim())
  const rows: ParsedCsvRow[] = []

  for (let i = 1; i < lines.length; i += 1) {
    const cells = parseCsvLine(lines[i])
    const row: ParsedCsvRow = {}
    for (let c = 0; c < headers.length; c += 1) {
      row[headers[c]] = cells[c] ?? ""
    }
    rows.push(row)
  }

  return rows
}

function readCsvFileSafe(filePath: string): ParsedCsvRow[] {
  if (!fs.existsSync(filePath)) return []
  try {
    return parseCsvText(fs.readFileSync(filePath, "utf8"))
  } catch {
    return []
  }
}

function pickMacroVariant(
  variants: Stage1MacroRow[] | undefined,
  variantName: string
): OfficialVariantSummary {
  const row = (variants ?? []).find((item) => item.variant === variantName)
  return {
    score: toNumber(row?.mean_score),
    wape: toNumber(row?.mean_wape),
    rbias: toNumber(row?.mean_rbias),
  }
}

function buildFeatureImportance(modelDir: string): Array<{ feature: string; value: number }> {
  const featurePath = path.join(modelDir, "feature_importance_baseline_feature_importance.csv")
  const rows = readCsvFileSafe(featurePath)

  const gainByFeature = new Map<string, number>()
  for (const row of rows) {
    const feature = (row.feature ?? "").trim()
    if (!feature) continue
    const gain = toNumber(row.gain)
    if (!Number.isFinite(gain) || gain <= 0) continue
    gainByFeature.set(feature, (gainByFeature.get(feature) ?? 0) + gain)
  }

  return Array.from(gainByFeature.entries())
    .map(([feature, value]) => ({ feature, value }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 30)
}

function buildLegacyHorizonDecay(
  stage2Rows: Stage2ByHorizonRow[],
  modelDir: string
): Array<{ horizon: string; error: number }> {
  if (stage2Rows.length > 0) {
    return stage2Rows
      .map((row) => ({
        horizon: `h${Math.round(toNumber(row.horizon, 0))}`,
        error: toNumber(row.ml_score, toNumber(row.ml_wape)),
      }))
      .filter((row) => row.horizon !== "h0")
  }

  const legacyRows = readCsvFileSafe(path.join(modelDir, "metrics_by_horizon.csv"))
  return legacyRows
    .map((row) => {
      const horizon = Math.round(toNumber(row.horizon))
      return {
        horizon: `h${horizon}`,
        error: toNumber(row.score, toNumber(row.wape)),
      }
    })
    .filter((row) => row.horizon !== "h0")
}

function buildOfficeHeatmapFromCsv(modelDir: string): {
  horizons: number[]
  rows: Stage2OfficeRow[]
} {
  const csvRows = readCsvFileSafe(path.join(modelDir, "analytics_stage2_office_horizon_proxy.csv"))
  const horizons = new Set<number>()
  const officeMap = new Map<
    string,
    {
      rowsTotal: number
      cells: Record<string, Stage2OfficeCell>
    }
  >()

  for (const row of csvRows) {
    const officeId = String(row.office_from_id ?? "").trim()
    const horizon = Math.round(toNumber(row.horizon, Number.NaN))
    if (!officeId || !Number.isFinite(horizon) || horizon <= 0) continue

    horizons.add(horizon)
    const key = `h${horizon}`
    const rowCount = Math.round(toNumber(row.rows))

    const office = officeMap.get(officeId) ?? { rowsTotal: 0, cells: {} }
    office.rowsTotal += rowCount
    office.cells[key] = {
      rows: rowCount,
      win_rate: toNumber(row.win_rate),
      tie_rate: toNumber(row.tie_rate),
      delta_abs_err_mean: toNumber(row.delta_abs_err_mean),
      expected_loss_abs_mean: toNumber(row.expected_loss_abs_mean),
      expected_loss_wape_like: toNumber(row.expected_loss_wape_like),
    }
    officeMap.set(officeId, office)
  }

  return {
    horizons: Array.from(horizons).sort((a, b) => a - b),
    rows: Array.from(officeMap.entries()).map(([officeId, value]) => ({
      office_from_id: officeId,
      rows_total: value.rowsTotal,
      cells: value.cells,
    })),
  }
}

function readStatusBaseline(row: DispatchRegistryRow): number {
  return toNumber(
    row.baseline_blend ??
      row.baseline_same_4w ??
      row.baseline_same_7d ??
      row.baselineSameSlot ??
      (row.baseline ? row.baseline / 4 : 0)
  )
}

function calcOperationsBlock(rows: DispatchRegistryRow[]) {
  const counts: Record<StatusKey, number> = {
    ok: 0,
    review: 0,
    drop: 0,
    critical: 0,
  }

  for (const row of rows) {
    const confidence =
      typeof row.trustScore === "number" && Number.isFinite(row.trustScore)
        ? clamp(Math.round(row.trustScore), 0, 100)
        : confidenceFromWape(row.wape, 75)

    const status = getVolumeDeltaStatus({
      forecast: row.forecast,
      baseline: readStatusBaseline(row),
      confidence,
    }).status

    counts[status] += 1
  }

  const total = rows.length
  const autoCount = counts.ok
  const manualReviewCount = counts.review
  const operatorAttentionCount = counts.review + counts.drop + counts.critical

  const pieStatuses = [
    { status: "ok", label: "В норме", count: counts.ok },
    { status: "review", label: "Требует проверки", count: counts.review },
    { status: "drop", label: "Аномальное снижение", count: counts.drop },
    { status: "critical", label: "Критическая нагрузка", count: counts.critical },
  ].map((item) => ({
    ...item,
    share: safeRatio(item.count, total),
  }))

  return {
    totalRoutes: total,
    autoDecision: {
      count: autoCount,
      share: safeRatio(autoCount, total),
    },
    manualReview: {
      count: manualReviewCount,
      share: safeRatio(manualReviewCount, total),
    },
    operatorAttention: {
      count: operatorAttentionCount,
      share: safeRatio(operatorAttentionCount, total),
    },
    pieStatuses,
  }
}

function buildFreshnessBlock(
  trustSummary: TrustStage3SummaryFile | null,
  trustRowsFile: TrustStage3RouteFile | null
) {
  const freshnessValues = (trustRowsFile?.rows ?? [])
    .map((row) => toNumber(row.freshness_minutes, Number.NaN))
    .filter((value) => Number.isFinite(value))

  return {
    source: "trust_stage3_route_horizon.json",
    rows: freshnessValues.length,
    p50Minutes: Number(percentile(freshnessValues, 50).toFixed(2)),
    p95Minutes: Number(percentile(freshnessValues, 95).toFixed(2)),
    maxMinutes: Number((freshnessValues.length > 0 ? Math.max(...freshnessValues) : 0).toFixed(2)),
    anchorTimestampUtc: trustSummary?.anchor_timestamp_utc ?? null,
    generatedAtUtc: trustRowsFile?.generated_at_utc ?? trustSummary?.generated_at_utc ?? null,
  }
}

function buildTrustWaveDistribution(rows: DispatchRegistryRow[]) {
  const scores = rows
    .map((row) =>
      typeof row.trustScore === "number" && Number.isFinite(row.trustScore)
        ? clamp(Number(row.trustScore), 0, 100)
        : confidenceFromWape(row.wape, 75)
    )
    .filter((score) => Number.isFinite(score))

  const total = scores.length
  const binSize = 5
  const bins: Array<{
    from: number
    to: number
    center: number
    label: string
    count: number
    share: number
  }> = []

  for (let from = 0; from < 100; from += binSize) {
    const to = from + binSize
    let count = 0
    for (const score of scores) {
      const inRange = score >= from && (score < to || (to >= 100 && score <= 100))
      if (inRange) count += 1
    }
    bins.push({
      from,
      to,
      center: from + binSize / 2,
      label: `${from}-${to}`,
      count,
      share: safeRatio(count, total),
    })
  }

  return {
    routes: total,
    bins,
    stats: {
      min: Number((scores.length > 0 ? Math.min(...scores) : 0).toFixed(1)),
      p10: Number(percentile(scores, 10).toFixed(1)),
      p50: Number(percentile(scores, 50).toFixed(1)),
      p90: Number(percentile(scores, 90).toFixed(1)),
      max: Number((scores.length > 0 ? Math.max(...scores) : 0).toFixed(1)),
      mean: Number(mean(scores).toFixed(2)),
      shareGe70: safeRatio(scores.filter((score) => score >= 70).length, total),
      shareGe85: safeRatio(scores.filter((score) => score >= 85).length, total),
    },
  }
}

function buildTrustBlock(
  trustSummary: TrustStage3SummaryFile | null,
  trustAblation: TrustStage3AblationSummaryFile | null,
  dispatchRows: DispatchRegistryRow[]
) {
  const calibration = trustSummary?.policy_calibration ?? {}
  const before = calibration.before_global ?? {}
  const after = calibration.after_global ?? {}

  return {
    metricRole: trustSummary?.metric_role ?? "policy_confidence_proxy",
    officialMetricGuardrail:
      trustSummary?.official_metric_guardrail ?? "Official metric remains WAPE + |Relative Bias|.",
    agreementMode: trustSummary?.agreement_mode ?? "unknown",
    agreementModeNote: trustSummary?.agreement_mode_note ?? null,
    calibration: {
      mode: calibration.mode ?? "none",
      applied: Boolean(calibration.applied),
      note: calibration.note ?? null,
      beforeGlobal: {
        min: toNumber(before.min),
        p10: toNumber(before.p10),
        p50: toNumber(before.p50),
        p90: toNumber(before.p90),
        max: toNumber(before.max),
        shareGe70: toNumber(before.share_ge_70),
        shareGe85: toNumber(before.share_ge_85),
      },
      afterGlobal: {
        min: toNumber(after.min, toNumber(before.min)),
        p10: toNumber(after.p10, toNumber(before.p10)),
        p50: toNumber(after.p50, toNumber(before.p50)),
        p90: toNumber(after.p90, toNumber(before.p90)),
        max: toNumber(after.max, toNumber(before.max)),
        shareGe70: toNumber(after.share_ge_70, toNumber(before.share_ge_70)),
        shareGe85: toNumber(after.share_ge_85, toNumber(before.share_ge_85)),
      },
    },
    globalStats: {
      rows: Math.round(toNumber(trustSummary?.global_stats?.rows)),
      routes: Math.round(toNumber(trustSummary?.global_stats?.routes)),
      horizons: Math.round(toNumber(trustSummary?.global_stats?.horizons)),
      meanPct: toNumber(
        trustSummary?.global_stats?.trust_score_pct_mean,
        toNumber(calibration.after_global?.p50, toNumber(calibration.before_global?.p50))
      ),
      p10Pct: toNumber(
        trustSummary?.global_stats?.trust_score_pct_p10,
        toNumber(calibration.after_global?.p10, toNumber(calibration.before_global?.p10))
      ),
      p50Pct: toNumber(
        trustSummary?.global_stats?.trust_score_pct_p50,
        toNumber(calibration.after_global?.p50, toNumber(calibration.before_global?.p50))
      ),
      p90Pct: toNumber(
        trustSummary?.global_stats?.trust_score_pct_p90,
        toNumber(calibration.after_global?.p90, toNumber(calibration.before_global?.p90))
      ),
    },
    weights: Object.entries(trustSummary?.weights ?? {})
      .map(([name, value]) => ({
        signal: name,
        weight: toNumber(value),
      }))
      .filter((item) => item.weight > 0)
      .sort((a, b) => b.weight - a.weight),
    ablation: trustAblation?.agreement_ablation_result
      ? {
          deltaExpectedLossWapeLike: toNumber(
            trustAblation.agreement_ablation_result.delta_auto_expected_loss_wape_like
          ),
          deltaAucBinned: toNumber(trustAblation.agreement_ablation_result.delta_auc_binned),
          deltaAutoVsManualWinLift: toNumber(
            trustAblation.agreement_ablation_result.delta_auto_vs_manual_win_lift
          ),
        }
      : null,
    waveDistribution: buildTrustWaveDistribution(dispatchRows),
    generatedAtUtc: trustSummary?.generated_at_utc ?? null,
    anchorTimestampUtc: trustSummary?.anchor_timestamp_utc ?? null,
    ablationGeneratedAtUtc: trustAblation?.generated_at_utc ?? null,
  }
}

export async function GET(request: Request) {
  try {
    const modelDir = resolveModelDir()

    const stage1Summary = readJsonFileSafe<Stage1SummaryFile>(
      path.join(modelDir, "offline_eval_stage1_summary.json")
    )
    const stage2ByHorizon = readJsonFileSafe<Stage2ByHorizonFile>(
      path.join(modelDir, "analytics_stage2_by_horizon.json")
    )
    const stage2Office = readJsonFileSafe<Stage2OfficeFile>(
      path.join(modelDir, "analytics_stage2_office_horizon_proxy.json")
    )
    const trustSummary = readJsonFileSafe<TrustStage3SummaryFile>(
      path.join(modelDir, "trust_stage3_summary.json")
    )
    const trustAblation = readJsonFileSafe<TrustStage3AblationSummaryFile>(
      path.join(modelDir, "trust_stage3_ablation_summary.json")
    )
    const trustRowsFile = readJsonFileSafe<TrustStage3RouteFile>(
      path.join(modelDir, "trust_stage3_route_horizon.json")
    )
    const legacySummary = readJsonFileSafe<LegacyMetricsSummaryFile>(
      path.join(modelDir, "metrics_summary.json")
    )

    const stage2Rows = (stage2ByHorizon?.rows ?? [])
      .slice()
      .sort((a, b) => toNumber(a.horizon) - toNumber(b.horizon))

    const mlOfficial = pickMacroVariant(stage1Summary?.averages_macro, "ml")
    const strongOfficial = pickMacroVariant(stage1Summary?.averages_macro, STRONG_VARIANT_NAME)
    const primitiveOfficial = pickMacroVariant(stage1Summary?.averages_macro, PRIMITIVE_VARIANT_NAME)

    const summary = {
      mean_wape: mlOfficial.wape || toNumber(legacySummary?.mean_wape),
      mean_rbias: mlOfficial.rbias || toNumber(legacySummary?.mean_rbias),
      mean_mae: toNumber(legacySummary?.mean_mae),
      mean_rmse: toNumber(legacySummary?.mean_rmse),
      mean_smape: toNumber(legacySummary?.mean_smape),
      mean_score: mlOfficial.score || toNumber(legacySummary?.mean_calib_metric),
      generated_at_utc: stage1Summary?.generated_at_utc ?? null,
      variant: legacySummary?.variant ?? "ml",
    }

    const horizonDecay = buildLegacyHorizonDecay(stage2Rows, modelDir)
    const featureImportance = buildFeatureImportance(modelDir)

    const { searchParams } = new URL(request.url)
    const selectedTimestamp = searchParams.get("timestamp") ?? dashboardConfig.CURRENT_TIME
    const waveRaw = Number(searchParams.get("wave") ?? 1)
    const cargoCapacityRaw = Number(searchParams.get("cargoCapacity") ?? dashboardConfig.CARGO_CAPACITY)
    const utilizationRaw = Number(searchParams.get("utilization") ?? 0.85)
    const reserveTrucksRaw = Number(searchParams.get("reserveTrucks") ?? 0)
    const strategyRaw = (searchParams.get("strategy") ?? "balance") as SimulationStrategy

    const selectedWave = WAVE_OPTIONS.includes(waveRaw as WaveSelection)
      ? (waveRaw as WaveSelection)
      : 1
    const strategy = STRATEGY_OPTIONS.includes(strategyRaw) ? strategyRaw : "balance"
    const cargoCapacity =
      Number.isFinite(cargoCapacityRaw) && cargoCapacityRaw > 0
        ? cargoCapacityRaw
        : dashboardConfig.CARGO_CAPACITY
    const utilization = Number.isFinite(utilizationRaw)
      ? clamp(utilizationRaw, 0.5, 1)
      : 0.85
    const reserveTrucks = Number.isFinite(reserveTrucksRaw)
      ? Math.max(0, Math.floor(reserveTrucksRaw))
      : 0

    const inferenceStart = performance.now()
    const dispatchRegistry = calculateDispatchRegistry({
      selectedTimestampIso: selectedTimestamp,
      selectedWave,
      cargoCapacity,
      utilization,
      reserveTrucks,
      strategy,
    })
    const inferenceLatencyMs = performance.now() - inferenceStart
    trackInferenceLatency(inferenceLatencyMs)

    const operations = calcOperationsBlock(dispatchRegistry.rows)

    const officialByHorizon = stage2Rows.map((row) => ({
      horizon: `h${Math.round(toNumber(row.horizon))}`,
      evalRows: Math.round(toNumber(row.eval_rows)),
      mlScore: toNumber(row.ml_score),
      mlWape: toNumber(row.ml_wape),
      mlRBias: toNumber(row.ml_rbias),
      strongScore: toNumber(row.strong_score),
      strongWape: toNumber(row.strong_wape),
      strongRBias: toNumber(row.strong_rbias),
      primitiveScore: toNumber(row.primitive_score),
      primitiveWape: toNumber(row.primitive_wape),
      primitiveRBias: toNumber(row.primitive_rbias),
    }))

    const proxyByHorizon = stage2Rows.map((row) => ({
      horizon: `h${Math.round(toNumber(row.horizon))}`,
      rows: Math.round(toNumber(row.eval_rows)),
      winRate: toNumber(row.proxy_win_rate),
      tieRate: toNumber(row.proxy_tie_rate),
      expectedLossAbsMean: toNumber(row.proxy_expected_loss_abs_mean),
      expectedLossWapeLike: toNumber(row.proxy_expected_loss_wape_like),
      deltaAbsErrMean: toNumber(row.proxy_delta_abs_err_mean),
    }))

    const csvHeatmap = buildOfficeHeatmapFromCsv(modelDir)
    const jsonHeatmapHorizons = (stage2Office?.matrix?.horizons ?? [])
      .map((h) => Math.round(toNumber(h)))
      .filter((h) => h > 0)
    const jsonHeatmapRows = stage2Office?.matrix?.rows ?? []
    const hasJsonHeatmap = jsonHeatmapHorizons.length > 0 && jsonHeatmapRows.length > 0

    const heatmapHorizons = hasJsonHeatmap ? jsonHeatmapHorizons : csvHeatmap.horizons
    const heatmapInputRows = hasJsonHeatmap ? jsonHeatmapRows : csvHeatmap.rows

    const heatmapRows = heatmapInputRows.map((row) => {
      const cells = heatmapHorizons.map((h) => {
        const key = `h${h}`
        const cell = row.cells?.[key]
        return {
          horizon: key,
          rows: Math.round(toNumber(cell?.rows)),
          winRate: toNumber(cell?.win_rate),
          tieRate: toNumber(cell?.tie_rate),
          expectedLossAbsMean: toNumber(cell?.expected_loss_abs_mean),
          expectedLossWapeLike: toNumber(cell?.expected_loss_wape_like),
          deltaAbsErrMean: toNumber(cell?.delta_abs_err_mean),
        }
      })

      return {
        officeFromId: String(row.office_from_id ?? "N/A"),
        rowsTotal: Math.round(toNumber(row.rows_total)),
        cells,
      }
    })

    const responsePayload = {
      summary,
      horizonDecay,
      featureImportance,
      analyticsBackend: {
        generatedAtUtc: new Date().toISOString(),
        officialMetrics: {
          metricName: "WAPE + |Relative Bias|",
          variants: {
            ml: mlOfficial,
            strongBaselineBlendRoll48Same7d: strongOfficial,
            primitiveBaselineSame4w: primitiveOfficial,
          },
          deltas: {
            mlVsStrongScore: mlOfficial.score - strongOfficial.score,
            mlVsPrimitiveScore: mlOfficial.score - primitiveOfficial.score,
          },
        },
        horizonChart: {
          official: officialByHorizon,
          proxy: proxyByHorizon,
        },
        officeHorizonHeatmap: {
          horizons: heatmapHorizons.map((h) => `h${h}`),
          rows: heatmapRows,
          coverage: {
            offices: heatmapRows.length,
            rows: heatmapRows.reduce((acc, row) => acc + row.rowsTotal, 0),
          },
        },
        operations: {
          ...operations,
          selectedWave,
          selectedTimestamp,
          strategy,
        },
        system: {
          dataFreshness: buildFreshnessBlock(trustSummary, trustRowsFile),
          inferenceLatencyMs: getLatencyStats(),
        },
        trust: buildTrustBlock(trustSummary, trustAblation, dispatchRegistry.rows),
        sources: {
          stage1Summary: "offline_eval_stage1_summary.json",
          stage2ByHorizon: "analytics_stage2_by_horizon.json",
          stage2OfficeHorizonProxy: "analytics_stage2_office_horizon_proxy.json|csv",
          trustSummary: "trust_stage3_summary.json",
          trustRouteHorizon: "trust_stage3_route_horizon.json",
        },
      },
    }

    return NextResponse.json(responsePayload, {
      headers: {
        "Cache-Control": "no-store",
      },
    })
  } catch (error) {
    const message = error instanceof Error ? error.message : "Не удалось собрать аналитические артефакты."
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
