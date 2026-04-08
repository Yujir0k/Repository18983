import "server-only"

import { execFileSync } from "node:child_process"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"

import { dashboardConfig } from "@/lib/dashboard-config"
import type {
  DispatchRegistryResponse,
  DashboardMetricsResponse,
  HorizonHours,
  MetricTone,
  SimulationStrategy,
  WaveSelection,
} from "@/types/dashboard-metrics"

type SubmissionCsvRow = {
  id: number
  yPred: number
  routeId: number
  horizonIndex: number
}

type SubmissionSnapshot = {
  routeCount: number
  horizonCount: number
  forecastByRoute: number[][]
}

type HorizonMetrics = {
  samples: number
  model: {
    wape: number
    relative_bias: number
    mae?: number
    rmse?: number
    smape?: number
  }
  baseline: {
    wape: number
    relative_bias: number
    mae?: number
    rmse?: number
    smape?: number
  }
}

type MetricsFile = {
  per_horizon: Record<string, HorizonMetrics>
  model: {
    wape: number
    relative_bias: number
  }
  baseline: {
    wape: number
    relative_bias: number
  }
}

type InferenceSummary = {
  generated_at_utc: string
  source_npz: string
  freq_minutes: number
  slots_per_day: number
  slots_per_week: number
  route_ids: Array<number | string>
  office_ids: Array<number | string>
  baseline_by_route_and_slot: number[][]
  baseline_same_4w_by_route?: number[]
  baseline_same_7d_by_route?: number[]
  baseline_blend_by_route?: number[]
  baseline_rule_based_by_route?: number[]
}

type TrustLookupCell = {
  trust_score?: number
  trust_score_pct?: number
  reason_short?: string
  reason_full?: string
  horizon?: number
  office_from_id?: number
  source?: string
}

type TrustLookupFile = {
  generated_at_utc?: string
  anchor_timestamp_utc?: string
  weights?: Record<string, number>
  route_horizon?: Record<string, Record<string, TrustLookupCell>>
}

type DemoInferenceMeta = {
  input_rows?: number
  input_routes?: number
  predicted_routes?: number
  generated_rows?: number
  freq_minutes?: number
  timestamp_min_utc?: string | null
  timestamp_max_utc?: string | null
  anchor_timestamp_min_utc?: string | null
  anchor_timestamp_max_utc?: string | null
  horizon_1_min_utc?: string | null
  horizon_1_max_utc?: string | null
  horizon_4_min_utc?: string | null
  horizon_4_max_utc?: string | null
  horizon_8_min_utc?: string | null
  horizon_8_max_utc?: string | null
  horizon_10_min_utc?: string | null
  horizon_10_max_utc?: string | null
  generated_at_utc?: string | null
}

type RuntimeCache = {
  submission?: SubmissionSnapshot
  baselineSubmissionFileName?: string
  metricsFile?: MetricsFile
  inferenceSummary?: InferenceSummary
  routeWapeLookup?: Record<string, number>
  trustLookupFile?: TrustLookupFile | null
  trustLookupCacheKey?: string
}

type SubmissionTotals = {
  wave1: number
  wave2: number
  total20h: number
}

export type DemoPredictionDiff = {
  comparedAtUtc: string
  changedCells: number
  changedRoutes: number
  totalsBefore: SubmissionTotals
  totalsAfter: SubmissionTotals
  totalsDelta: SubmissionTotals
}

type DemoRuntimeState = {
  stagedRawCsvPath?: string
  stagedFileName?: string
  stagedRows?: number
  stagedAtUtc?: string
  stagedRouteCount?: number
  stagedTimestampMinUtc?: string
  stagedTimestampMaxUtc?: string
  activeSubmission?: SubmissionSnapshot
  activeFileName?: string
  activeAtUtc?: string
  activeInferenceMeta?: DemoInferenceMeta
  lastDiff?: DemoPredictionDiff
}

type StagedUploadMeta = {
  stagedRows: number
  routeCount: number
  fileName: string
  stagedAtUtc: string
  timestampMinUtc: string | null
  timestampMaxUtc: string | null
}

type ActiveDemoMeta = {
  activeFileName: string
  activeAtUtc: string
  activeInferenceMeta: DemoInferenceMeta | null
  lastDiff: DemoPredictionDiff | null
}

type CalculatorInput = {
  selectedTimestampIso: string
  selectedHorizonHours: HorizonHours
  isCumulative: boolean
}

type DispatchRegistryInput = {
  selectedTimestampIso: string
  selectedWave: WaveSelection
  cargoCapacity: number
  utilization: number
  reserveTrucks: number
  strategy: SimulationStrategy
}

const runtimeCache: RuntimeCache = {}
const demoRuntimeState: DemoRuntimeState = {}
let hasLoggedRouteAlignmentDebug = false

const HOURS_PER_STEP = 2
const STEP_DURATION_MS = HOURS_PER_STEP * 60 * 60 * 1000
const STRATEGY_RESERVE_FACTORS: Record<SimulationStrategy, number> = {
  economy: 0,
  balance: 0.5,
  reliable: 1,
}
const STRATEGY_RISK_FACTORS: Record<SimulationStrategy, number> = {
  economy: 0,
  balance: 0.8,
  reliable: 1.6,
}

const HORIZON_STEP_BY_HOURS: Record<HorizonHours, number> = {
  2: 1,
  4: 2,
  6: 3,
  8: 4,
  10: 5,
  12: 6,
  14: 7,
  16: 8,
  18: 9,
  20: 10,
}

const WAVE_FORECAST_RANGES: Record<WaveSelection, [number, number]> = {
  1: [0, 3],
  2: [4, 7],
}

const RAW_REQUIRED_COLUMNS = [
  "route_id",
  "office_from_id",
  "timestamp",
  "status_1",
  "status_2",
  "status_3",
  "status_4",
  "status_5",
  "status_6",
  "status_7",
  "status_8",
] as const

const DEMO_STAGED_RAW_CSV_FILE = "staged-raw-upload.csv"
const DEMO_STAGED_META_FILE = "staged-upload-meta.json"
const DEMO_ACTIVE_SUBMISSION_FILE = "active-submission.csv"
const DEMO_ACTIVE_META_FILE = "active-meta.json"
const WINDOWS_ASCII_ASSETS_ROOT = "C:\\rwb_assets"
const asciiMirrorCache = new Map<string, string>()
const MODEL_REQUIRED_FILES = [
  "feature_schema.json",
  "history_tail.parquet",
  "latest_snapshot.parquet",
  "office_scales.parquet",
  "h01.txt",
  "h10.txt",
] as const

function hasNonAsciiCharacters(value: string): boolean {
  return /[^\x00-\x7F]/.test(value)
}

function copyDirectoryRecursive(sourceDir: string, targetDir: string) {
  fs.mkdirSync(targetDir, { recursive: true })

  const entries = fs.readdirSync(sourceDir, { withFileTypes: true })
  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name)
    const targetPath = path.join(targetDir, entry.name)

    if (entry.isDirectory()) {
      copyDirectoryRecursive(sourcePath, targetPath)
      continue
    }

    if (!entry.isFile()) {
      continue
    }

    fs.mkdirSync(path.dirname(targetPath), { recursive: true })
    fs.copyFileSync(sourcePath, targetPath)
  }
}

function isModelArtifactsReady(modelDir: string): boolean {
  return MODEL_REQUIRED_FILES.every((fileName) =>
    fs.existsSync(path.join(modelDir, fileName))
  )
}

function canWriteToDirectory(dirPath: string): boolean {
  try {
    fs.mkdirSync(dirPath, { recursive: true })
    const probePath = path.join(
      dirPath,
      `.write-probe-${process.pid}-${Date.now()}.tmp`
    )
    fs.writeFileSync(probePath, "ok", "utf8")
    fs.unlinkSync(probePath)
    return true
  } catch {
    return false
  }
}

function getAsciiMirrorRoots(): string[] {
  const configuredRoot = process.env.RWB_ASSETS_ROOT?.trim()
  const systemDrive = process.env.SystemDrive ?? "C:"
  const publicRoot = path.join(systemDrive, "Users", "Public", "rwb_assets")
  const tempRoot = path.join(os.tmpdir(), "rwb_assets")
  const fallbackDriveRoot = path.join(systemDrive, "rwb_assets")

  const rawCandidates = [
    configuredRoot,
    WINDOWS_ASCII_ASSETS_ROOT,
    publicRoot,
    tempRoot,
    fallbackDriveRoot,
  ].filter((candidate): candidate is string => Boolean(candidate && candidate.length > 0))

  const uniqueRoots = new Set<string>()
  for (const candidate of rawCandidates) {
    const resolved = path.resolve(candidate)
    if (hasNonAsciiCharacters(resolved)) {
      continue
    }
    uniqueRoots.add(resolved)
  }

  return Array.from(uniqueRoots)
}

function ensureAsciiSafeModelDir(sourceDir: string): string {
  if (process.platform !== "win32" || !hasNonAsciiCharacters(sourceDir)) {
    return sourceDir
  }

  const cacheKey = `model:${sourceDir}`
  const cached = asciiMirrorCache.get(cacheKey)
  if (cached && isModelArtifactsReady(cached)) {
    return cached
  }

  const candidateRoots = getAsciiMirrorRoots()
  const syncErrors: string[] = []
  for (const rootDir of candidateRoots) {
    try {
      if (!canWriteToDirectory(rootDir)) {
        syncErrors.push(`${rootDir}: нет прав на запись`)
        continue
      }

      const targetDir = path.join(rootDir, "model")
      copyDirectoryRecursive(sourceDir, targetDir)
      if (!isModelArtifactsReady(targetDir)) {
        syncErrors.push(`${rootDir}: неполная копия model-артефактов`)
        continue
      }

      asciiMirrorCache.set(cacheKey, targetDir)
      return targetDir
    } catch (error) {
      syncErrors.push(
        `${rootDir}: ${error instanceof Error ? error.message : String(error)}`
      )
    }
  }

  throw new Error(
    `Не удалось подготовить ASCII-путь для model. Укажите RWB_ASSETS_ROOT в ASCII-папку с правом записи. Детали: ${syncErrors.join(" | ")}`
  )
}

function resolveWeightsDir(): string {
  const configured = process.env.WEIGHTS_DIR
  const candidates = [
    configured,
    path.resolve(process.cwd(), "../weights"),
    path.resolve(process.cwd(), "weights"),
  ].filter(Boolean) as string[]

  for (const candidate of candidates) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()) {
      return candidate
    }
  }

  throw new Error(
    "Could not find the weights directory. Set the WEIGHTS_DIR environment variable."
  )
}

function getModelDirCandidates(): string[] {
  const configured = process.env.MODEL_DIR?.trim()
  const candidates = [
    configured,
    path.resolve(process.cwd(), "model"),
    path.resolve(process.cwd(), "../model"),
  ].filter((value): value is string => Boolean(value))

  return Array.from(new Set(candidates.map((candidate) => path.resolve(candidate))))
}

function resolveModelDir(): string {
  const candidates = getModelDirCandidates()

  for (const candidate of candidates) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()) {
      return ensureAsciiSafeModelDir(candidate)
    }
  }

  throw new Error(
    "Не удалось найти папку model. Укажите путь через переменную MODEL_DIR."
  )
}

function resolveModelDataDir(): string {
  const candidates = getModelDirCandidates()

  for (const candidate of candidates) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isDirectory()) {
      return candidate
    }
  }

  throw new Error(
    "Не удалось найти папку model. Укажите путь через переменную MODEL_DIR."
  )
}

function findFirstExistingFile(candidates: Array<string | undefined>): string | null {
  for (const candidate of candidates) {
    if (!candidate) {
      continue
    }
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return candidate
    }
  }
  return null
}

function resolveBaselineSubmissionPath(weightsDir: string, modelDir: string): string {
  const configured = process.env.SUBMISSION_CSV?.trim()
  const resolvedConfigured =
    configured && configured.length > 0
      ? path.resolve(configured)
      : undefined

  const resolved = findFirstExistingFile([
    resolvedConfigured,
    path.join(modelDir, "submission_stable_export_bundle.csv"),
    path.join(modelDir, "submission.csv"),
    path.join(modelDir, "submission (61).csv"),
    path.join(weightsDir, "submission.csv"),
  ])

  if (!resolved) {
    throw new Error(
      "Не удалось найти файл submission. Добавьте submission_stable_export_bundle.csv в model или submission.csv в weights."
    )
  }

  return resolved
}

function getDemoCacheDir(): string {
  const demoCacheDir = path.join(process.cwd(), ".cache", "demo")
  fs.mkdirSync(demoCacheDir, { recursive: true })
  return demoCacheDir
}

function cleanupFileIfExists(filePath?: string) {
  if (!filePath) {
    return
  }
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath)
  }
}

function getDemoCachePaths() {
  const demoCacheDir = getDemoCacheDir()
  return {
    stagedRawCsvPath: path.join(demoCacheDir, DEMO_STAGED_RAW_CSV_FILE),
    stagedMetaPath: path.join(demoCacheDir, DEMO_STAGED_META_FILE),
    activeSubmissionPath: path.join(demoCacheDir, DEMO_ACTIVE_SUBMISSION_FILE),
    activeMetaPath: path.join(demoCacheDir, DEMO_ACTIVE_META_FILE),
    generatedSubmissionPath: path.join(demoCacheDir, "generated-submission-from-raw.csv"),
    generatedMetaPath: path.join(demoCacheDir, "generated-submission-meta.json"),
  }
}

function readJsonFileSafe<T>(filePath: string): T | null {
  if (!fs.existsSync(filePath)) {
    return null
  }
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8")) as T
  } catch {
    return null
  }
}

function normalizeRouteWapeLookup(payload: unknown): Record<string, number> {
  const lookup: Record<string, number> = {}

  if (Array.isArray(payload)) {
    payload.forEach((value, index) => {
      const parsed = Number(value)
      if (Number.isFinite(parsed)) {
        lookup[String(index)] = parsed
      }
    })
    return lookup
  }

  if (!payload || typeof payload !== "object") {
    return lookup
  }

  for (const [rawKey, rawValue] of Object.entries(payload as Record<string, unknown>)) {
    const parsedValue = Number(rawValue)
    if (!Number.isFinite(parsedValue)) {
      continue
    }

    lookup[String(rawKey)] = parsedValue

    const numericKey = Number(rawKey)
    if (Number.isFinite(numericKey)) {
      lookup[String(Math.round(numericKey))] = parsedValue
    }
  }

  return lookup
}

function loadRouteWapeLookup(weightsDir: string, modelDir: string): Record<string, number> {
  if (runtimeCache.routeWapeLookup) {
    return runtimeCache.routeWapeLookup
  }

  const configured = process.env.ROUTE_WAPE_JSON?.trim()
  const resolvedConfigured =
    configured && configured.length > 0 ? path.resolve(configured) : undefined

  const routeWapePath = findFirstExistingFile([
    resolvedConfigured,
    path.join(modelDir, "route_wape_7d.json"),
    path.join(modelDir, "reports", "route_wape_7d.json"),
    path.join(weightsDir, "route_wape_7d.json"),
    path.join(process.cwd(), ".cache", "route_wape_7d.json"),
    path.join(process.cwd(), "model_artifacts", "stable_export_bundle_artifacts", "route_wape_7d.json"),
    path.join(
      process.cwd(),
      "model_artifacts",
      "stable_export_bundle_artifacts",
      "reports",
      "route_wape_7d.json"
    ),
    path.join("C:\\rwb_train_artifacts", "stable_export_bundle_artifacts", "route_wape_7d.json"),
    path.join(
      "C:\\rwb_train_artifacts",
      "stable_export_bundle_artifacts",
      "reports",
      "route_wape_7d.json"
    ),
  ])

  if (!routeWapePath) {
    runtimeCache.routeWapeLookup = {}
    return runtimeCache.routeWapeLookup
  }

  const payload = readJsonFileSafe<unknown>(routeWapePath)
  runtimeCache.routeWapeLookup = normalizeRouteWapeLookup(payload)
  return runtimeCache.routeWapeLookup
}

function normalizeTrustLookup(payload: unknown): TrustLookupFile | null {
  if (!payload || typeof payload !== "object") {
    return null
  }

  const data = payload as TrustLookupFile
  const rawRouteHorizon =
    data.route_horizon && typeof data.route_horizon === "object"
      ? data.route_horizon
      : {}

  const route_horizon: Record<string, Record<string, TrustLookupCell>> = {}

  for (const [routeKey, rawHorizonMap] of Object.entries(rawRouteHorizon)) {
    if (!rawHorizonMap || typeof rawHorizonMap !== "object") {
      continue
    }

    const normalizedHorizonMap: Record<string, TrustLookupCell> = {}
    for (const [horizonKey, rawCell] of Object.entries(
      rawHorizonMap as Record<string, unknown>
    )) {
      if (!rawCell || typeof rawCell !== "object") {
        continue
      }
      const cell = rawCell as Record<string, unknown>
      const trustScoreRaw = Number(cell.trust_score)
      const trustScorePctRaw = Number(cell.trust_score_pct)
      const horizonRaw = Number(cell.horizon)
      const officeRaw = Number(cell.office_from_id)

      normalizedHorizonMap[horizonKey] = {
        trust_score: Number.isFinite(trustScoreRaw)
          ? Math.max(0, Math.min(1, trustScoreRaw))
          : undefined,
        trust_score_pct: Number.isFinite(trustScorePctRaw)
          ? Math.max(0, Math.min(100, trustScorePctRaw))
          : undefined,
        reason_short:
          typeof cell.reason_short === "string" ? cell.reason_short : undefined,
        reason_full:
          typeof cell.reason_full === "string" ? cell.reason_full : undefined,
        horizon: Number.isFinite(horizonRaw) ? Math.max(1, Math.round(horizonRaw)) : undefined,
        office_from_id: Number.isFinite(officeRaw) ? Math.round(officeRaw) : undefined,
        source: typeof cell.source === "string" ? cell.source : undefined,
      }
    }

    if (Object.keys(normalizedHorizonMap).length > 0) {
      route_horizon[String(routeKey)] = normalizedHorizonMap
    }
  }

  return {
    generated_at_utc:
      typeof data.generated_at_utc === "string" ? data.generated_at_utc : undefined,
    anchor_timestamp_utc:
      typeof data.anchor_timestamp_utc === "string"
        ? data.anchor_timestamp_utc
        : undefined,
    weights:
      data.weights && typeof data.weights === "object"
        ? (data.weights as Record<string, number>)
        : undefined,
    route_horizon,
  }
}

function loadTrustLookupFile(weightsDir: string, modelDir: string): TrustLookupFile | null {
  const configured = process.env.TRUST_LOOKUP_JSON?.trim()
  const resolvedConfigured =
    configured && configured.length > 0 ? path.resolve(configured) : undefined

  const trustPath = findFirstExistingFile([
    resolvedConfigured,
    path.join(modelDir, "trust_stage3_lookup.json"),
    path.join(weightsDir, "trust_stage3_lookup.json"),
    path.join(process.cwd(), "model", "trust_stage3_lookup.json"),
    path.join(
      process.cwd(),
      "model_artifacts",
      "stable_export_bundle_artifacts",
      "trust_stage3_lookup.json"
    ),
    path.join("C:\\rwb_train_artifacts", "stable_export_bundle_artifacts", "trust_stage3_lookup.json"),
  ])

  if (!trustPath) {
    runtimeCache.trustLookupFile = null
    runtimeCache.trustLookupCacheKey = undefined
    return runtimeCache.trustLookupFile
  }

  let cacheKey = `${trustPath}:na`
  try {
    const stat = fs.statSync(trustPath)
    cacheKey = `${trustPath}:${stat.mtimeMs}:${stat.size}`
  } catch {
    cacheKey = `${trustPath}:na`
  }

  if (
    runtimeCache.trustLookupFile !== undefined &&
    runtimeCache.trustLookupCacheKey === cacheKey
  ) {
    return runtimeCache.trustLookupFile
  }

  const payload = readJsonFileSafe<unknown>(trustPath)
  runtimeCache.trustLookupFile = normalizeTrustLookup(payload)
  runtimeCache.trustLookupCacheKey = cacheKey
  return runtimeCache.trustLookupFile
}

function readRouteTrustCell(
  trustLookupFile: TrustLookupFile | null,
  routeId: number | string,
  routeIndex: number,
  horizon: number
): TrustLookupCell | null {
  if (!trustLookupFile?.route_horizon) {
    return null
  }

  const horizonKey = `h${Math.max(1, Math.round(horizon))}`
  const routeKey = String(routeId)
  const byRouteId = trustLookupFile.route_horizon[routeKey]?.[horizonKey]
  if (byRouteId) {
    return byRouteId
  }

  const byIndex = trustLookupFile.route_horizon[String(routeIndex)]?.[horizonKey]
  if (byIndex) {
    return byIndex
  }

  return null
}

function writeJsonFile(filePath: string, payload: unknown) {
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf8")
}

function hydrateStagedStateFromDisk() {
  const { stagedRawCsvPath, stagedMetaPath } = getDemoCachePaths()
  if (!fs.existsSync(stagedRawCsvPath)) {
    demoRuntimeState.stagedRawCsvPath = undefined
    demoRuntimeState.stagedRows = undefined
    demoRuntimeState.stagedRouteCount = undefined
    demoRuntimeState.stagedFileName = undefined
    demoRuntimeState.stagedAtUtc = undefined
    demoRuntimeState.stagedTimestampMinUtc = undefined
    demoRuntimeState.stagedTimestampMaxUtc = undefined
    return
  }

  const stagedMeta = readJsonFileSafe<StagedUploadMeta>(stagedMetaPath)
  demoRuntimeState.stagedRawCsvPath = stagedRawCsvPath
  demoRuntimeState.stagedRows = stagedMeta?.stagedRows
  demoRuntimeState.stagedRouteCount = stagedMeta?.routeCount
  demoRuntimeState.stagedFileName = stagedMeta?.fileName
  demoRuntimeState.stagedAtUtc = stagedMeta?.stagedAtUtc
  demoRuntimeState.stagedTimestampMinUtc = stagedMeta?.timestampMinUtc ?? undefined
  demoRuntimeState.stagedTimestampMaxUtc = stagedMeta?.timestampMaxUtc ?? undefined
}

function hydrateActiveStateFromDisk() {
  const { activeSubmissionPath, activeMetaPath } = getDemoCachePaths()
  if (!fs.existsSync(activeSubmissionPath)) {
    demoRuntimeState.activeSubmission = undefined
    demoRuntimeState.activeFileName = undefined
    demoRuntimeState.activeAtUtc = undefined
    demoRuntimeState.activeInferenceMeta = undefined
    demoRuntimeState.lastDiff = undefined
    return
  }

  try {
    const activeSubmissionText = fs.readFileSync(activeSubmissionPath, "utf8")
    const activeRows = parseSubmissionCsv(activeSubmissionText)
    if (activeRows.length > 0) {
      demoRuntimeState.activeSubmission = buildSubmissionSnapshot(activeRows)
    }
  } catch {
    demoRuntimeState.activeSubmission = undefined
  }

  const activeMeta = readJsonFileSafe<ActiveDemoMeta>(activeMetaPath)
  if (activeMeta) {
    demoRuntimeState.activeFileName = activeMeta.activeFileName
    demoRuntimeState.activeAtUtc = activeMeta.activeAtUtc
    demoRuntimeState.activeInferenceMeta = activeMeta.activeInferenceMeta ?? undefined
    demoRuntimeState.lastDiff = activeMeta.lastDiff ?? undefined
  }
}

function toIsoStringOrNull(timestampMs: number): string | null {
  if (!Number.isFinite(timestampMs)) {
    return null
  }
  return new Date(timestampMs).toISOString()
}

function parseUtcTimestampMs(rawTimestamp: string): number | null {
  const trimmed = rawTimestamp.trim()
  if (!trimmed) {
    return null
  }

  const isoLike = trimmed.replace(" ", "T")
  const hasTimeZone = /(?:z|[+\-]\d{2}:?\d{2})$/i.test(isoLike)
  const normalized = hasTimeZone ? isoLike : `${isoLike}Z`
  const parsedMs = Date.parse(normalized)

  return Number.isFinite(parsedMs) ? parsedMs : null
}

function parseRawCsvPreview(csvText: string) {
  const cleaned = csvText.replace(/^\uFEFF/, "")
  const lines = cleaned
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

  if (lines.length < 2) {
    throw new Error(
      "CSV должен содержать заголовок и хотя бы одну строку данных."
    )
  }

  const delimiter = lines[0].includes(";") && !lines[0].includes(",") ? ";" : ","
  const headers = lines[0].split(delimiter).map((header) => header.trim().toLowerCase())
  const missingColumns = RAW_REQUIRED_COLUMNS.filter(
    (columnName) => !headers.includes(columnName)
  )

  if (missingColumns.length > 0) {
    throw new Error(
      `В CSV не хватает обязательных колонок: ${missingColumns.join(", ")}.`
    )
  }

  const routeIdIndex = headers.indexOf("route_id")
  const timestampIndex = headers.indexOf("timestamp")
  const uniqueRoutes = new Set<number>()
  let minTimestampMs = Number.POSITIVE_INFINITY
  let maxTimestampMs = Number.NEGATIVE_INFINITY

  for (let i = 1; i < lines.length; i += 1) {
    const parts = lines[i].split(delimiter).map((part) => part.trim())
    const routeId = Number(parts[routeIdIndex])
    if (Number.isFinite(routeId)) {
      uniqueRoutes.add(routeId)
    }

    const timestampRaw = parts[timestampIndex]
    if (timestampRaw) {
      const parsedTimestampMs = parseUtcTimestampMs(timestampRaw)
      if (parsedTimestampMs !== null) {
        minTimestampMs = Math.min(minTimestampMs, parsedTimestampMs)
        maxTimestampMs = Math.max(maxTimestampMs, parsedTimestampMs)
      }
    }
  }

  return {
    rows: lines.length - 1,
    routeCount: uniqueRoutes.size,
    timestampMinUtc: toIsoStringOrNull(minTimestampMs),
    timestampMaxUtc: toIsoStringOrNull(maxTimestampMs),
  }
}

function parseSubmissionCsv(csvText: string): SubmissionCsvRow[] {
  const cleaned = csvText.replace(/^\uFEFF/, "")
  const lines = cleaned.split(/\r?\n/).filter((line) => line.trim().length > 0)
  const rows: SubmissionCsvRow[] = []
  if (lines.length === 0) {
    return rows
  }

  const delimiter =
    lines[0].includes(";") && !lines[0].includes(",") ? ";" : ","
  const splitCsvLine = (line: string) =>
    line.split(delimiter).map((part) => part.trim())
  const normalizeHeader = (value: string) => value.toLowerCase().replace(/[^a-z0-9]+/g, "")
  const firstParts = splitCsvLine(lines[0])
  const firstLooksLikeData =
    firstParts.length >= 2 &&
    Number.isFinite(Number(firstParts[0])) &&
    Number.isFinite(Number(firstParts[1]))

  let idIndex = 0
  let predIndex = 1
  let startIndex = 0

  if (!firstLooksLikeData) {
    const normalizedHeaders = firstParts.map(normalizeHeader)
    const idCandidates = new Set(["id", "rowid"])
    const predCandidates = new Set([
      "ypred",
      "y_pred",
      "prediction",
      "pred",
      "forecast",
      "value",
    ])

    const foundId = normalizedHeaders.findIndex((header) => idCandidates.has(header))
    const foundPred = normalizedHeaders.findIndex((header) => predCandidates.has(header))

    if (foundId >= 0 && foundPred >= 0) {
      idIndex = foundId
      predIndex = foundPred
      startIndex = 1
    } else {
      // Fallback for unknown header names: treat first line as header and use first 2 columns.
      startIndex = 1
    }
  }

  for (let i = startIndex; i < lines.length; i += 1) {
    const parts = splitCsvLine(lines[i])
    const idRaw = parts[idIndex]
    const predRaw = parts[predIndex]
    const id = Number(idRaw)
    const yPred = Number(predRaw)

    if (!Number.isFinite(id) || !Number.isFinite(yPred)) {
      continue
    }

    rows.push({
      id,
      yPred,
      routeId: Math.floor(id / 10),
      horizonIndex: id % 10,
    })
  }

  return rows
}

function buildSubmissionSnapshot(rows: SubmissionCsvRow[]): SubmissionSnapshot {
  const maxRouteId = rows.reduce(
    (acc, row) => (row.routeId > acc ? row.routeId : acc),
    -1
  )
  const maxHorizonIndex = rows.reduce(
    (acc, row) => (row.horizonIndex > acc ? row.horizonIndex : acc),
    -1
  )

  const routeCount = maxRouteId + 1
  const horizonCount = maxHorizonIndex + 1
  const forecastByRoute = Array.from({ length: routeCount }, () =>
    Array.from({ length: horizonCount }, () => 0)
  )

  for (const row of rows) {
    if (
      row.routeId >= 0 &&
      row.routeId < routeCount &&
      row.horizonIndex >= 0 &&
      row.horizonIndex < horizonCount
    ) {
      forecastByRoute[row.routeId][row.horizonIndex] = row.yPred
    }
  }

  return {
    routeCount,
    horizonCount,
    forecastByRoute,
  }
}

function computeSubmissionTotals(snapshot: SubmissionSnapshot): SubmissionTotals {
  const horizonCount = snapshot.horizonCount
  const wave1End = Math.min(3, horizonCount - 1)
  const wave2Start = Math.min(4, horizonCount)
  const wave2End = Math.min(7, horizonCount - 1)
  const totalEnd = Math.min(9, horizonCount - 1)

  let wave1 = 0
  let wave2 = 0
  let total20h = 0

  for (let routeIndex = 0; routeIndex < snapshot.routeCount; routeIndex += 1) {
    const routeForecast = snapshot.forecastByRoute[routeIndex] ?? []
    for (let h = 0; h <= wave1End; h += 1) {
      wave1 += routeForecast[h] ?? 0
    }
    for (let h = wave2Start; h <= wave2End; h += 1) {
      wave2 += routeForecast[h] ?? 0
    }
    for (let h = 0; h <= totalEnd; h += 1) {
      total20h += routeForecast[h] ?? 0
    }
  }

  return { wave1, wave2, total20h }
}

function diffSubmissionSnapshots(
  before: SubmissionSnapshot,
  after: SubmissionSnapshot
): DemoPredictionDiff {
  const maxRoutes = Math.max(before.routeCount, after.routeCount)
  const maxHorizons = Math.max(before.horizonCount, after.horizonCount)
  let changedCells = 0
  let changedRoutes = 0

  for (let routeIndex = 0; routeIndex < maxRoutes; routeIndex += 1) {
    const beforeRoute = before.forecastByRoute[routeIndex] ?? []
    const afterRoute = after.forecastByRoute[routeIndex] ?? []
    let routeChanged = false

    for (let horizonIndex = 0; horizonIndex < maxHorizons; horizonIndex += 1) {
      const beforeValue = beforeRoute[horizonIndex] ?? 0
      const afterValue = afterRoute[horizonIndex] ?? 0
      if (Math.abs(beforeValue - afterValue) > 1e-9) {
        changedCells += 1
        routeChanged = true
      }
    }

    if (routeChanged) {
      changedRoutes += 1
    }
  }

  const totalsBefore = computeSubmissionTotals(before)
  const totalsAfter = computeSubmissionTotals(after)

  return {
    comparedAtUtc: new Date().toISOString(),
    changedCells,
    changedRoutes,
    totalsBefore,
    totalsAfter,
    totalsDelta: {
      wave1: totalsAfter.wave1 - totalsBefore.wave1,
      wave2: totalsAfter.wave2 - totalsBefore.wave2,
      total20h: totalsAfter.total20h - totalsBefore.total20h,
    },
  }
}

function loadSubmissionSnapshot(weightsDir: string, modelDir: string): SubmissionSnapshot {
  hydrateActiveStateFromDisk()

  if (demoRuntimeState.activeSubmission) {
    return demoRuntimeState.activeSubmission
  }

  if (runtimeCache.submission) {
    return runtimeCache.submission
  }

  const submissionPath = resolveBaselineSubmissionPath(weightsDir, modelDir)
  const csvText = fs.readFileSync(submissionPath, "utf8")
  const rows = parseSubmissionCsv(csvText)
  const snapshot = buildSubmissionSnapshot(rows)

  runtimeCache.submission = snapshot
  runtimeCache.baselineSubmissionFileName = path.basename(submissionPath)
  return snapshot
}

export function stageSubmissionCsv(csvText: string, fileName?: string) {
  const preview = parseRawCsvPreview(csvText)
  const { stagedRawCsvPath, stagedMetaPath } = getDemoCachePaths()
  const stagedAtUtc = new Date().toISOString()
  const resolvedFileName = fileName ?? "upload.csv"

  fs.writeFileSync(stagedRawCsvPath, csvText, "utf8")
  writeJsonFile(stagedMetaPath, {
    stagedRows: preview.rows,
    routeCount: preview.routeCount,
    fileName: resolvedFileName,
    stagedAtUtc,
    timestampMinUtc: preview.timestampMinUtc ?? null,
    timestampMaxUtc: preview.timestampMaxUtc ?? null,
  } satisfies StagedUploadMeta)

  demoRuntimeState.stagedRawCsvPath = stagedRawCsvPath
  demoRuntimeState.stagedRows = preview.rows
  demoRuntimeState.stagedRouteCount = preview.routeCount
  demoRuntimeState.stagedFileName = resolvedFileName
  demoRuntimeState.stagedAtUtc = stagedAtUtc
  demoRuntimeState.stagedTimestampMinUtc = preview.timestampMinUtc ?? undefined
  demoRuntimeState.stagedTimestampMaxUtc = preview.timestampMaxUtc ?? undefined

  return {
    stagedRows: preview.rows,
    routeCount: preview.routeCount,
    horizonCount: 10,
    fileName: resolvedFileName,
    stagedAtUtc: demoRuntimeState.stagedAtUtc,
    stagedTimestampMinUtc: demoRuntimeState.stagedTimestampMinUtc ?? null,
    stagedTimestampMaxUtc: demoRuntimeState.stagedTimestampMaxUtc ?? null,
  }
}

export function applyStagedSubmission() {
  hydrateStagedStateFromDisk()
  const {
    stagedRawCsvPath,
    stagedMetaPath,
    generatedSubmissionPath,
    generatedMetaPath,
    activeSubmissionPath,
    activeMetaPath,
  } = getDemoCachePaths()
  const effectiveStagedRawCsvPath =
    demoRuntimeState.stagedRawCsvPath ?? stagedRawCsvPath

  if (!fs.existsSync(effectiveStagedRawCsvPath)) {
    throw new Error("Нет загруженного CSV для обработки.")
  }

  const inferenceScriptPath = path.join(
    process.cwd(),
    "scripts",
    "infer_from_raw_csv.py"
  )

  execFileSync(
    "python",
    [
      inferenceScriptPath,
      "--model-dir",
      resolveModelDir(),
      "--input-csv",
      effectiveStagedRawCsvPath,
      "--output-csv",
      generatedSubmissionPath,
      "--output-meta",
      generatedMetaPath,
    ],
    { stdio: "pipe" }
  )

  const generatedSubmissionText = fs.readFileSync(generatedSubmissionPath, "utf8")
  const rows = parseSubmissionCsv(generatedSubmissionText)
  if (rows.length === 0) {
    throw new Error("Инференс не вернул валидные предсказания.")
  }

  const nextSnapshot = buildSubmissionSnapshot(rows)
  const currentWeightsDir = resolveWeightsDir()
  const currentModelDir = resolveModelDataDir()
  const currentSnapshot = loadSubmissionSnapshot(currentWeightsDir, currentModelDir)
  const diff = diffSubmissionSnapshots(currentSnapshot, nextSnapshot)

  let parsedMeta: DemoInferenceMeta = {}
  if (fs.existsSync(generatedMetaPath)) {
    try {
      const rawMeta = JSON.parse(fs.readFileSync(generatedMetaPath, "utf8")) as DemoInferenceMeta
      if (rawMeta && typeof rawMeta === "object") {
        parsedMeta = rawMeta
      }
    } catch {
      parsedMeta = {}
    }
  }

  const mergedMeta: DemoInferenceMeta = {
    ...parsedMeta,
    timestamp_min_utc:
      parsedMeta.timestamp_min_utc ??
      demoRuntimeState.stagedTimestampMinUtc ??
      null,
    timestamp_max_utc:
      parsedMeta.timestamp_max_utc ??
      demoRuntimeState.stagedTimestampMaxUtc ??
      null,
  }

  demoRuntimeState.activeSubmission = nextSnapshot
  demoRuntimeState.activeFileName =
    demoRuntimeState.stagedFileName ?? "uploaded_raw.csv"
  demoRuntimeState.activeAtUtc = new Date().toISOString()
  demoRuntimeState.activeInferenceMeta = mergedMeta
  demoRuntimeState.lastDiff = diff

  fs.writeFileSync(activeSubmissionPath, generatedSubmissionText, "utf8")
  writeJsonFile(activeMetaPath, {
    activeFileName: demoRuntimeState.activeFileName,
    activeAtUtc: demoRuntimeState.activeAtUtc,
    activeInferenceMeta: demoRuntimeState.activeInferenceMeta ?? null,
    lastDiff: demoRuntimeState.lastDiff ?? null,
  } satisfies ActiveDemoMeta)

  cleanupFileIfExists(effectiveStagedRawCsvPath)
  cleanupFileIfExists(stagedMetaPath)
  demoRuntimeState.stagedRawCsvPath = undefined
  demoRuntimeState.stagedRows = undefined
  demoRuntimeState.stagedRouteCount = undefined
  demoRuntimeState.stagedFileName = undefined
  demoRuntimeState.stagedAtUtc = undefined
  demoRuntimeState.stagedTimestampMinUtc = undefined
  demoRuntimeState.stagedTimestampMaxUtc = undefined

  return {
    activeFileName: demoRuntimeState.activeFileName,
    activeAtUtc: demoRuntimeState.activeAtUtc,
    activeInferenceMeta: demoRuntimeState.activeInferenceMeta ?? null,
    diff,
  }
}

export function resetDemoSubmissionToBaseline() {
  const {
    stagedRawCsvPath,
    stagedMetaPath,
    generatedSubmissionPath,
    generatedMetaPath,
    activeSubmissionPath,
    activeMetaPath,
  } = getDemoCachePaths()

  demoRuntimeState.activeSubmission = undefined
  demoRuntimeState.activeFileName = undefined
  demoRuntimeState.activeAtUtc = undefined
  demoRuntimeState.activeInferenceMeta = undefined
  demoRuntimeState.lastDiff = undefined
  cleanupFileIfExists(stagedRawCsvPath)
  cleanupFileIfExists(stagedMetaPath)
  cleanupFileIfExists(generatedSubmissionPath)
  cleanupFileIfExists(generatedMetaPath)
  cleanupFileIfExists(activeSubmissionPath)
  cleanupFileIfExists(activeMetaPath)
  demoRuntimeState.stagedRawCsvPath = undefined
  demoRuntimeState.stagedRows = undefined
  demoRuntimeState.stagedRouteCount = undefined
  demoRuntimeState.stagedFileName = undefined
  demoRuntimeState.stagedAtUtc = undefined
  demoRuntimeState.stagedTimestampMinUtc = undefined
  demoRuntimeState.stagedTimestampMaxUtc = undefined

  const weightsDir = resolveWeightsDir()
  const modelDir = resolveModelDataDir()
  const baselineSnapshot = loadSubmissionSnapshot(weightsDir, modelDir)
  return {
    activeFileName: runtimeCache.baselineSubmissionFileName ?? "submission.csv",
    routeCount: baselineSnapshot.routeCount,
    horizonCount: baselineSnapshot.horizonCount,
    resetAtUtc: new Date().toISOString(),
  }
}

export function getDemoSubmissionStatus() {
  hydrateStagedStateFromDisk()
  hydrateActiveStateFromDisk()
  let baselineFileName = runtimeCache.baselineSubmissionFileName
  if (!baselineFileName) {
    try {
      const weightsDir = resolveWeightsDir()
      const modelDir = resolveModelDataDir()
      baselineFileName = path.basename(resolveBaselineSubmissionPath(weightsDir, modelDir))
      runtimeCache.baselineSubmissionFileName = baselineFileName
    } catch {
      baselineFileName = "submission.csv"
    }
  }

  return {
    isUsingBaseline: !demoRuntimeState.activeSubmission,
    hasStagedUpload: Boolean(demoRuntimeState.stagedRawCsvPath),
    stagedRows: demoRuntimeState.stagedRows ?? 0,
    stagedFileName: demoRuntimeState.stagedFileName ?? null,
    stagedAtUtc: demoRuntimeState.stagedAtUtc ?? null,
    stagedTimestampMinUtc: demoRuntimeState.stagedTimestampMinUtc ?? null,
    stagedTimestampMaxUtc: demoRuntimeState.stagedTimestampMaxUtc ?? null,
    activeFileName:
      demoRuntimeState.activeFileName ??
      baselineFileName ??
      "submission.csv",
    activeAtUtc: demoRuntimeState.activeAtUtc ?? null,
    activeInferenceMeta: demoRuntimeState.activeInferenceMeta ?? null,
    lastDiff: demoRuntimeState.lastDiff ?? null,
  }
}

function parseModelMetricsByHorizonCsv(csvText: string) {
  const lines = csvText
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

  if (lines.length < 2) {
    return []
  }

  const headers = lines[0].split(",").map((header) => header.trim())
  const horizonIndex = headers.indexOf("horizon")
  const wapeIndex = headers.indexOf("wape")
  const rbiasIndex = headers.indexOf("rbias")
  const sumTrueIndex = headers.indexOf("sum_true")
  const maeIndex = headers.indexOf("mae")
  const rmseIndex = headers.indexOf("rmse")
  const smapeIndex = headers.indexOf("smape")

  if (horizonIndex < 0 || wapeIndex < 0 || rbiasIndex < 0) {
    return []
  }

  const rows: Array<{
    horizon: number
    wape: number
    rbias: number
    samples: number
    mae?: number
    rmse?: number
    smape?: number
  }> = []

  for (let i = 1; i < lines.length; i += 1) {
    const parts = lines[i].split(",").map((part) => part.trim())
    const horizon = Number(parts[horizonIndex])
    const wape = Number(parts[wapeIndex])
    const rbias = Math.abs(Number(parts[rbiasIndex]))
    const samplesRaw =
      sumTrueIndex >= 0 ? Number(parts[sumTrueIndex]) : Number.NaN
    const maeRaw = maeIndex >= 0 ? Number(parts[maeIndex]) : Number.NaN
    const rmseRaw = rmseIndex >= 0 ? Number(parts[rmseIndex]) : Number.NaN
    const smapeRaw = smapeIndex >= 0 ? Number(parts[smapeIndex]) : Number.NaN

    if (!Number.isFinite(horizon) || !Number.isFinite(wape) || !Number.isFinite(rbias)) {
      continue
    }

    rows.push({
      horizon: Math.max(1, Math.floor(horizon)),
      wape: Math.max(0, wape),
      rbias: Math.max(0, rbias),
      samples: Number.isFinite(samplesRaw) ? Math.max(0, Math.round(samplesRaw)) : 0,
      mae: Number.isFinite(maeRaw) ? Math.max(0, maeRaw) : undefined,
      rmse: Number.isFinite(rmseRaw) ? Math.max(0, rmseRaw) : undefined,
      smape: Number.isFinite(smapeRaw) ? Math.max(0, smapeRaw) : undefined,
    })
  }

  rows.sort((left, right) => left.horizon - right.horizon)
  return rows
}

function buildMetricsFileFromModelArtifacts(modelDir: string): MetricsFile | null {
  const csvPath = path.join(modelDir, "metrics_by_horizon.csv")
  const summaryPath = path.join(modelDir, "metrics_summary.json")

  const hasCsv = fs.existsSync(csvPath)
  const hasSummary = fs.existsSync(summaryPath)
  if (!hasCsv && !hasSummary) {
    return null
  }

  const horizonRows = hasCsv
    ? parseModelMetricsByHorizonCsv(fs.readFileSync(csvPath, "utf8"))
    : []

  const parsedSummary = hasSummary
    ? (readJsonFileSafe<{
        mean_wape?: number
        mean_rbias?: number
      }>(summaryPath) ?? {})
    : {}

  const per_horizon: Record<string, HorizonMetrics> = {}
  for (const row of horizonRows) {
    per_horizon[String(row.horizon)] = {
      samples: row.samples,
      model: {
        wape: row.wape,
        relative_bias: row.rbias,
        mae: row.mae,
        rmse: row.rmse,
        smape: row.smape,
      },
      // В model-артефактах baseline-поля отсутствуют, поэтому
      // используем те же значения как нейтральный fallback.
      baseline: {
        wape: row.wape,
        relative_bias: row.rbias,
        mae: row.mae,
        rmse: row.rmse,
        smape: row.smape,
      },
    }
  }

  const avgWapeFromHorizons =
    horizonRows.length > 0
      ? horizonRows.reduce((acc, row) => acc + row.wape, 0) / horizonRows.length
      : 0
  const avgRBiasFromHorizons =
    horizonRows.length > 0
      ? horizonRows.reduce((acc, row) => acc + row.rbias, 0) / horizonRows.length
      : 0

  const overallModelWape = Number.isFinite(parsedSummary.mean_wape)
    ? Math.max(0, parsedSummary.mean_wape ?? 0)
    : avgWapeFromHorizons
  const overallModelRBias = Number.isFinite(parsedSummary.mean_rbias)
    ? Math.max(0, parsedSummary.mean_rbias ?? 0)
    : avgRBiasFromHorizons

  return {
    per_horizon,
    model: {
      wape: overallModelWape,
      relative_bias: overallModelRBias,
    },
    baseline: {
      wape: overallModelWape,
      relative_bias: overallModelRBias,
    },
  }
}

function loadMetricsFile(weightsDir: string, modelDir: string): MetricsFile {
  if (runtimeCache.metricsFile) {
    return runtimeCache.metricsFile
  }

  const modelMetrics = buildMetricsFileFromModelArtifacts(modelDir)
  if (modelMetrics) {
    runtimeCache.metricsFile = modelMetrics
    return modelMetrics
  }

  const metricsPath = findFirstExistingFile([
    path.join(modelDir, "metrics.json"),
    path.join(weightsDir, "metrics.json"),
  ])
  if (!metricsPath) {
    throw new Error("Не удалось найти metrics-файл ни в model, ни в weights.")
  }

  const metricsText = fs.readFileSync(metricsPath, "utf8")
  const metrics = JSON.parse(metricsText) as MetricsFile

  runtimeCache.metricsFile = metrics
  return metrics
}

function ensureInferenceSummary(weightsDir: string, modelDir: string): InferenceSummary {
  if (runtimeCache.inferenceSummary) {
    return runtimeCache.inferenceSummary
  }

  const cacheDir = path.join(process.cwd(), ".cache")
  const summaryPath = path.join(cacheDir, "inference-summary.json")
  const scriptPath = path.join(process.cwd(), "scripts", "build_inference_summary.py")
  const configuredNpz = process.env.INFERENCE_STATE_NPZ?.trim()
  const resolvedConfiguredNpz =
    configuredNpz && configuredNpz.length > 0 ? path.resolve(configuredNpz) : undefined
  const npzPath = findFirstExistingFile([
    resolvedConfiguredNpz,
    path.join(modelDir, "inference_state.npz"),
    path.join(weightsDir, "inference_state.npz"),
  ])

  if (!npzPath && !fs.existsSync(summaryPath)) {
    throw new Error(
      "Не найден inference_state.npz и нет готового .cache/inference-summary.json."
    )
  }

  const shouldRebuildSummary = (() => {
    if (!npzPath) {
      return false
    }
    if (!fs.existsSync(summaryPath)) {
      return true
    }
    const parsedSummary = readJsonFileSafe<InferenceSummary>(summaryPath)
    const hasEnterpriseBaselines =
      Array.isArray(parsedSummary?.baseline_same_4w_by_route) &&
      Array.isArray(parsedSummary?.baseline_blend_by_route) &&
      Array.isArray(parsedSummary?.baseline_rule_based_by_route)
    if (!hasEnterpriseBaselines) {
      return true
    }
    const npzStat = fs.statSync(npzPath)
    const summaryStat = fs.statSync(summaryPath)
    return summaryStat.mtimeMs < npzStat.mtimeMs
  })()

  if (shouldRebuildSummary) {
    if (!npzPath) {
      throw new Error("Не найден inference_state.npz для пересборки inference-summary.")
    }
    fs.mkdirSync(cacheDir, { recursive: true })
    execFileSync("python", [scriptPath, npzPath, summaryPath], { stdio: "pipe" })
  }

  const summaryText = fs.readFileSync(summaryPath, "utf8")
  const summary = JSON.parse(summaryText) as InferenceSummary

  runtimeCache.inferenceSummary = summary
  return summary
}

function toUtcSlotIndex(
  timestampMs: number,
  slotsPerDay: number,
  freqMinutes: number
): number {
  const date = new Date(timestampMs)
  const jsDay = date.getUTCDay() // Sunday=0
  const weekdayMondayZero = (jsDay + 6) % 7 // Monday=0
  const minuteOfDay = date.getUTCHours() * 60 + date.getUTCMinutes()
  const slotOfDay = Math.floor(minuteOfDay / freqMinutes)
  return weekdayMondayZero * slotsPerDay + slotOfDay
}

function formatUnits(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Math.round(value))
}

function toOneDecimal(value: number): string {
  const safeValue = Number.isFinite(value) ? value : 0
  return safeValue.toFixed(1)
}

function formatSignedPercent(value: number): string {
  const safeValue = Number.isFinite(value) ? value : 0
  const sign = safeValue > 0 ? "+" : ""
  return `${sign}${toOneDecimal(safeValue)}%`
}

function safeRatio(numerator: number, denominator: number): number {
  if (denominator <= 0) {
    return numerator > 0 ? Number.POSITIVE_INFINITY : 1
  }
  return numerator / denominator
}

function parseRouteId(value: number | string | undefined): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.round(value)
  }
  if (typeof value === "string") {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) {
      return Math.round(parsed)
    }
  }
  return null
}

function readRouteBaselineValue(
  values: number[] | undefined,
  routeIndex: number,
  fallback = 0
): number {
  if (!Array.isArray(values)) {
    return fallback
  }
  const rawValue = values[routeIndex]
  return Number.isFinite(rawValue) ? Number(rawValue) : fallback
}

function readRouteWapeValue(
  routeWapeLookup: Record<string, number>,
  routeId: number | string,
  routeIndex: number,
  fallback: number
): number {
  const routeKey = String(routeId)
  const byRouteId = routeWapeLookup[routeKey]
  if (Number.isFinite(byRouteId)) {
    return Number(byRouteId)
  }

  const byIndex = routeWapeLookup[String(routeIndex)]
  if (Number.isFinite(byIndex)) {
    return Number(byIndex)
  }

  return fallback
}

function logRouteAlignmentDebug(input: {
  routeCount: number
  summaryRouteIds: Array<number | string>
  submission: SubmissionSnapshot
}) {
  if (hasLoggedRouteAlignmentDebug) {
    return
  }
  hasLoggedRouteAlignmentDebug = true

  const { routeCount, summaryRouteIds, submission } = input
  let mismatchCount = 0
  let missingForecastRows = 0
  const samples: Array<{
    rowIndex: number
    summaryRouteId: number | string | null
    submissionRouteId: number
    hasSubmissionRow: boolean
  }> = []

  for (let routeIndex = 0; routeIndex < routeCount; routeIndex += 1) {
    const summaryRouteRaw = summaryRouteIds[routeIndex]
    const summaryRouteId = parseRouteId(summaryRouteRaw)
    const submissionRouteId = routeIndex
    const hasSubmissionRow = Array.isArray(submission.forecastByRoute[routeIndex])
    if (!hasSubmissionRow) {
      missingForecastRows += 1
    }

    if (summaryRouteId === null || summaryRouteId !== submissionRouteId) {
      mismatchCount += 1
      if (samples.length < 10) {
        samples.push({
          rowIndex: routeIndex,
          summaryRouteId: summaryRouteRaw ?? null,
          submissionRouteId,
          hasSubmissionRow,
        })
      }
    }
  }

  console.log(
    `[dispatch-registry] route alignment check: routeCount=${routeCount}; submission.routeCount=${submission.routeCount}; summary.routeCount=${summaryRouteIds.length}; mismatches=${mismatchCount}; missingForecastRows=${missingForecastRows}; samples=${JSON.stringify(
      samples
    )}`
  )
}

function getRouteForecastAtStep(routeForecast: number[], step: number): number {
  const index = Math.max(0, step - 1)
  if (index < routeForecast.length) {
    return routeForecast[index]
  }
  const fallback = routeForecast[Math.max(0, routeForecast.length - 1)]
  return Number.isFinite(fallback) ? fallback : 0
}

function getRouteForecastAtIndex(routeForecast: number[], index: number): number {
  const safeIndex = Math.max(0, index)
  if (safeIndex < routeForecast.length) {
    return routeForecast[safeIndex]
  }
  const fallback = routeForecast[Math.max(0, routeForecast.length - 1)]
  return Number.isFinite(fallback) ? fallback : 0
}

function getWaveTargetIndex(selectedWave: WaveSelection): number {
  return selectedWave === 1 ? 3 : 7
}

function getWaveWindowBounds(selectedWave: WaveSelection, selectedTsMs: number): [number, number] {
  if (selectedWave === 1) {
    return [selectedTsMs, selectedTsMs + STEP_DURATION_MS]
  }
  return [selectedTsMs + STEP_DURATION_MS, selectedTsMs + STEP_DURATION_MS * 2]
}

function sumBaselineWindow(
  baselineBySlot: number[],
  startMs: number,
  endMs: number,
  slotsPerDay: number,
  freqMinutes: number
): number {
  const slotDurationMs = freqMinutes * 60_000
  let cursorMs = startMs
  let total = 0

  while (cursorMs < endMs) {
    const slotIndex = toUtcSlotIndex(cursorMs, slotsPerDay, freqMinutes)
    const slotValue = baselineBySlot[slotIndex] ?? 0
    total += slotValue
    cursorMs += slotDurationMs
  }

  return total
}

function sumBaselineWindowShifted(
  baselineBySlot: number[],
  startMs: number,
  endMs: number,
  slotsPerDay: number,
  freqMinutes: number,
  shiftSlots: number
): number {
  const slotDurationMs = freqMinutes * 60_000
  const slotsPerWeek = slotsPerDay * 7
  const normalizedShift =
    slotsPerWeek > 0 ? ((shiftSlots % slotsPerWeek) + slotsPerWeek) % slotsPerWeek : 0

  let cursorMs = startMs
  let total = 0

  while (cursorMs < endMs) {
    const slotIndex = toUtcSlotIndex(cursorMs, slotsPerDay, freqMinutes)
    const shiftedIndex =
      slotsPerWeek > 0
        ? (slotIndex - normalizedShift + slotsPerWeek) % slotsPerWeek
        : slotIndex
    const slotValue = baselineBySlot[shiftedIndex] ?? 0
    total += slotValue
    cursorMs += slotDurationMs
  }

  return total
}

function meanBaselineWindow(
  baselineBySlot: number[],
  startMs: number,
  endMs: number,
  slotsPerDay: number,
  freqMinutes: number
): number {
  const slotDurationMs = freqMinutes * 60_000
  const slotCount = Math.max(1, Math.round((endMs - startMs) / slotDurationMs))
  return sumBaselineWindow(
    baselineBySlot,
    startMs,
    endMs,
    slotsPerDay,
    freqMinutes
  ) / slotCount
}

function meanBaselineWindowShifted(
  baselineBySlot: number[],
  startMs: number,
  endMs: number,
  slotsPerDay: number,
  freqMinutes: number,
  shiftSlots: number
): number {
  const slotDurationMs = freqMinutes * 60_000
  const slotCount = Math.max(1, Math.round((endMs - startMs) / slotDurationMs))
  return sumBaselineWindowShifted(
    baselineBySlot,
    startMs,
    endMs,
    slotsPerDay,
    freqMinutes,
    shiftSlots
  ) / slotCount
}

function baselineValueAtTimestamp(
  baselineBySlot: number[],
  timestampMs: number,
  slotsPerDay: number,
  freqMinutes: number
): number {
  const slotIndex = toUtcSlotIndex(timestampMs, slotsPerDay, freqMinutes)
  return baselineBySlot[slotIndex] ?? 0
}

function baselineValueAtTimestampShifted(
  baselineBySlot: number[],
  timestampMs: number,
  slotsPerDay: number,
  freqMinutes: number,
  shiftSlots: number
): number {
  const slotIndex = toUtcSlotIndex(timestampMs, slotsPerDay, freqMinutes)
  const slotsPerWeek = slotsPerDay * 7
  const normalizedShift =
    slotsPerWeek > 0 ? ((shiftSlots % slotsPerWeek) + slotsPerWeek) % slotsPerWeek : 0
  const shiftedIndex =
    slotsPerWeek > 0
      ? (slotIndex - normalizedShift + slotsPerWeek) % slotsPerWeek
      : slotIndex
  return baselineBySlot[shiftedIndex] ?? 0
}

function buildFleetStatus(requiredFleet: number): { text: string; tone: MetricTone } {
  const delta = dashboardConfig.TOTAL_FLEET - requiredFleet

  if (delta > 0) {
    return {
      text: `Р РµР·РµСЂРІ: ${formatUnits(delta)} РјР°С€РёРЅ`,
      tone: "success",
    }
  }

  if (delta < 0) {
    return {
      text: `Р”РµС„РёС†РёС‚: ${formatUnits(Math.abs(delta))} РјР°С€РёРЅ`,
      tone: "danger",
    }
  }

  return {
    text: "Р РµР·РµСЂРІ: 0 РјР°С€РёРЅ",
    tone: "neutral",
  }
}

function calculateSelectedSteps(
  selectedHorizonHours: HorizonHours,
  availableHorizons: number,
  isCumulative: boolean
): number[] {
  const requestedStep = HORIZON_STEP_BY_HOURS[selectedHorizonHours]
  const effectiveStep = Math.max(1, Math.min(requestedStep, availableHorizons))

  if (isCumulative) {
    return Array.from({ length: effectiveStep }, (_, index) => index + 1)
  }

  return [effectiveStep]
}

export function calculateRequiredTransport(params: {
  forecasts: number[]
  wapes: number[]
  anomalyRatios: number[]
  cargoCapacity: number
  utilization: number
  reserveTrucks: number
  strategy: SimulationStrategy
}) {
  const safeCapacity = Math.max(params.cargoCapacity * params.utilization, 1)
  const routeCount = params.forecasts.length
  const safeForecasts = params.forecasts.map((value) => {
    const sanitizedForecast = Math.max(0, value)
    return sanitizedForecast < 0.5 ? 0 : sanitizedForecast
  })
  const baseByRoute = safeForecasts.map((forecast) =>
    forecast > 0 ? Math.ceil(forecast / safeCapacity) : 0
  )
  const totalBaseTrucks = baseByRoute.reduce((sum, value) => sum + value, 0)

  const allocateByLargestRemainder = (total: number, weights: number[]) => {
    const size = weights.length
    if (size === 0) {
      return [] as number[]
    }
    if (total <= 0) {
      return Array.from({ length: size }, () => 0)
    }

    const safeWeights = weights.map((value) => (Number.isFinite(value) ? Math.max(0, value) : 0))
    const sumWeights = safeWeights.reduce((sum, value) => sum + value, 0)
    const normalizedWeights =
      sumWeights > 0
        ? safeWeights
        : Array.from({ length: size }, () => 1)

    const denominator = normalizedWeights.reduce((sum, value) => sum + value, 0)
    const exactValues = normalizedWeights.map((value) => (value / denominator) * total)
    const allocations = exactValues.map((value) => Math.floor(value))

    let remainder = total - allocations.reduce((sum, value) => sum + value, 0)
    if (remainder <= 0) {
      return allocations
    }

    const fractions = exactValues
      .map((value, index) => ({
        index,
        fraction: value - Math.floor(value),
        weight: normalizedWeights[index],
      }))
      .sort((left, right) => {
        if (right.fraction !== left.fraction) {
          return right.fraction - left.fraction
        }
        return right.weight - left.weight
      })

    for (let i = 0; i < fractions.length && remainder > 0; i += 1) {
      allocations[fractions[i].index] += 1
      remainder -= 1
    }

    return allocations
  }

  // Резерв задается вручную, но стратегия определяет, какая доля
  // этого резерва реально участвует в расчете:
  // economy = 0%, balance = 50%, reliable = 100%.
  const rawReservePool = Math.max(0, Math.floor(params.reserveTrucks))
  const reserveFactor = STRATEGY_RESERVE_FACTORS[params.strategy] ?? 0.5
  const reservePool = Math.min(
    rawReservePool,
    Math.max(0, Math.round(rawReservePool * reserveFactor))
  )
  const strategyRiskFactor = STRATEGY_RISK_FACTORS[params.strategy] ?? 0.8

  const riskWeights = safeForecasts.map((forecast, index) => {
    if ((baseByRoute[index] ?? 0) <= 0) {
      return 0
    }
    const wape = Math.max(0.01, params.wapes[index] ?? 0)
    const anomalyRatio = params.anomalyRatios[index] ?? 1
    const anomalyBoost =
      Number.isFinite(anomalyRatio) && anomalyRatio > dashboardConfig.ANOMALY_THRESHOLD
        ? 1 + Math.min(1, anomalyRatio - dashboardConfig.ANOMALY_THRESHOLD)
        : 1
    return forecast * (1 + wape * strategyRiskFactor) * anomalyBoost
  })

  const insuranceByRoute = allocateByLargestRemainder(reservePool, riskWeights)
  const totalByRoute = Array.from({ length: routeCount }, (_, index) => {
    return (baseByRoute[index] ?? 0) + (insuranceByRoute[index] ?? 0)
  })

  return {
    baseByRoute,
    insuranceByRoute,
    totalByRoute,
    totalBaseTrucks,
    reservePool,
  }
}

export function calculateDispatchRegistry({
  selectedTimestampIso,
  selectedWave,
  cargoCapacity,
  utilization,
  reserveTrucks,
  strategy,
}: DispatchRegistryInput): DispatchRegistryResponse {
  const weightsDir = resolveWeightsDir()
  const modelDir = resolveModelDataDir()
  const submission = loadSubmissionSnapshot(weightsDir, modelDir)
  const metrics = loadMetricsFile(weightsDir, modelDir)
  const summary = ensureInferenceSummary(weightsDir, modelDir)
  const routeWapeLookup = loadRouteWapeLookup(weightsDir, modelDir)
  const trustLookupFile = loadTrustLookupFile(weightsDir, modelDir)

  const parsedSelectedTs = Number.isNaN(Date.parse(selectedTimestampIso))
    ? dashboardConfig.CURRENT_TIME
    : selectedTimestampIso
  const selectedTsMs = Date.parse(parsedSelectedTs)
  const targetIndex = getWaveTargetIndex(selectedWave)
  const [waveStartIndex, waveEndIndex] = WAVE_FORECAST_RANGES[selectedWave]
  const [waveStartMs, waveEndMs] = getWaveWindowBounds(selectedWave, selectedTsMs)
  const horizonTimelineLength = Math.min(10, submission.horizonCount)
  const horizonStepMinutes = summary.freq_minutes
  const horizonStepMs = horizonStepMinutes * 60_000
  // Timeline is always anchored to true horizons h1..h10:
  // h1 starts at the next slot after selected timestamp.
  const horizonTimelineStartMs = selectedTsMs + horizonStepMs
  const metricStep = Math.max(1, Math.min(targetIndex + 1, submission.horizonCount))
  const baseWape = metrics.per_horizon[String(metricStep)]?.model.wape ?? metrics.model.wape
  const baseRelativeBias =
    metrics.per_horizon[String(metricStep)]?.model.relative_bias ??
    metrics.model.relative_bias
  const baseMae = metrics.per_horizon[String(metricStep)]?.model.mae
  const baseRmse = metrics.per_horizon[String(metricStep)]?.model.rmse
  const baseSmape = metrics.per_horizon[String(metricStep)]?.model.smape
  const baseCompetitionScore = baseWape + Math.abs(baseRelativeBias)

  type RouteAccumulator = {
    routeId: string
    officeFromId: string
    forecast: number
    baseline: number
    baselineSameSlot: number
    baselineLag48: number
    baselineSame4w: number
    baselineBlend: number
    baselineRuleBased: number
    baselineSeries: number[]
    waveForecasts: number[]
    horizonForecasts: number[]
    horizonBaselineSame4w: number[]
    horizonBaselineBlend: number[]
    horizonBaselineRule: number[]
  }

  const routeMap = new Map<string, RouteAccumulator>()
  const routeCount = Math.min(
    submission.routeCount,
    summary.baseline_by_route_and_slot.length,
    summary.office_ids.length
  )

  logRouteAlignmentDebug({
    routeCount,
    summaryRouteIds: summary.route_ids,
    submission,
  })

  for (let routeIndex = 0; routeIndex < routeCount; routeIndex += 1) {
    const routeId = String(summary.route_ids[routeIndex] ?? routeIndex + 1)
    const officeFromId = String(summary.office_ids[routeIndex] ?? routeIndex + 1)
    const routeForecast = submission.forecastByRoute[routeIndex] ?? []
    const routeBaselineBySlot = summary.baseline_by_route_and_slot[routeIndex] ?? []
    const lag48ShiftSlots = summary.slots_per_day

    const acc = routeMap.get(routeId) ?? {
      routeId,
      officeFromId,
      forecast: 0,
      baseline: 0,
      baselineSameSlot: 0,
      baselineLag48: 0,
      baselineSame4w: 0,
      baselineBlend: 0,
      baselineRuleBased: 0,
      baselineSeries: Array.from({ length: summary.slots_per_week }, () => 0),
      waveForecasts: Array.from({ length: 4 }, () => 0),
      horizonForecasts: Array.from({ length: horizonTimelineLength }, () => 0),
      horizonBaselineSame4w: Array.from({ length: horizonTimelineLength }, () => 0),
      horizonBaselineBlend: Array.from({ length: horizonTimelineLength }, () => 0),
      horizonBaselineRule: Array.from({ length: horizonTimelineLength }, () => 0),
    }

    const baselineWindowSum = sumBaselineWindow(
      routeBaselineBySlot,
      waveStartMs,
      waveEndMs,
      summary.slots_per_day,
      summary.freq_minutes
    )
    const baselineWindowMean = meanBaselineWindow(
      routeBaselineBySlot,
      waveStartMs,
      waveEndMs,
      summary.slots_per_day,
      summary.freq_minutes
    )
    const baselineLag48Mean = meanBaselineWindowShifted(
      routeBaselineBySlot,
      waveStartMs,
      waveEndMs,
      summary.slots_per_day,
      summary.freq_minutes,
      lag48ShiftSlots
    )
    const routeBaselineSame4w = readRouteBaselineValue(
      summary.baseline_same_4w_by_route ?? summary.baseline_same_7d_by_route,
      routeIndex,
      baselineWindowMean
    )
    const routeBaselineBlend = readRouteBaselineValue(
      summary.baseline_blend_by_route,
      routeIndex,
      (routeBaselineSame4w + baselineLag48Mean) / 2
    )
    const routeBaselineRuleBased = readRouteBaselineValue(
      summary.baseline_rule_based_by_route,
      routeIndex,
      baselineLag48Mean
    )

    acc.forecast += getRouteForecastAtIndex(routeForecast, targetIndex)
    acc.baseline += baselineWindowSum
    acc.baselineSameSlot += baselineWindowMean
    acc.baselineLag48 += baselineLag48Mean
    acc.baselineSame4w = routeBaselineSame4w
    acc.baselineBlend = routeBaselineBlend
    acc.baselineRuleBased = routeBaselineRuleBased
    for (let slotOffset = 0; slotOffset < 4; slotOffset += 1) {
      acc.waveForecasts[slotOffset] += getRouteForecastAtIndex(
        routeForecast,
        waveStartIndex + slotOffset
      )
    }

    for (
      let horizonIndex = 0;
      horizonIndex < horizonTimelineLength;
      horizonIndex += 1
    ) {
      const horizonTsMs = horizonTimelineStartMs + horizonIndex * horizonStepMs
      const horizonForecast = getRouteForecastAtIndex(routeForecast, horizonIndex)
      const horizonBaselineSame4w = baselineValueAtTimestamp(
        routeBaselineBySlot,
        horizonTsMs,
        summary.slots_per_day,
        summary.freq_minutes
      )
      const horizonBaselineLag48 = baselineValueAtTimestampShifted(
        routeBaselineBySlot,
        horizonTsMs,
        summary.slots_per_day,
        summary.freq_minutes,
        lag48ShiftSlots
      )

      acc.horizonForecasts[horizonIndex] += horizonForecast
      acc.horizonBaselineSame4w[horizonIndex] += horizonBaselineSame4w
      acc.horizonBaselineBlend[horizonIndex] +=
        (horizonBaselineSame4w + horizonBaselineLag48) / 2
      acc.horizonBaselineRule[horizonIndex] += horizonBaselineLag48
    }

    const limit = Math.min(acc.baselineSeries.length, routeBaselineBySlot.length)
    for (let slot = 0; slot < limit; slot += 1) {
      acc.baselineSeries[slot] += routeBaselineBySlot[slot] ?? 0
    }

    routeMap.set(routeId, acc)
  }

  const routeRows = Array.from(routeMap.values())
    .map((routeAcc, routeIndex) => {
      const ratio = safeRatio(routeAcc.forecast, routeAcc.baseline)
      const rowWape = readRouteWapeValue(routeWapeLookup, routeAcc.routeId, routeIndex, baseWape)
      const trustCell = readRouteTrustCell(
        trustLookupFile,
        routeAcc.routeId,
        routeIndex,
        metricStep
      )

      const trustScoreFromPct = Number(trustCell?.trust_score_pct)
      const trustScoreFromRaw = Number(trustCell?.trust_score)
      const trustScore =
        Number.isFinite(trustScoreFromPct)
          ? Math.max(0, Math.min(100, trustScoreFromPct))
          : Number.isFinite(trustScoreFromRaw)
            ? Math.max(0, Math.min(100, trustScoreFromRaw * 100))
            : undefined

      return {
        id: routeAcc.routeId,
        routeId: routeAcc.routeId,
        officeFromId: routeAcc.officeFromId,
        forecast: routeAcc.forecast,
        baseline: routeAcc.baseline,
        baselineSameSlot: routeAcc.baselineSameSlot,
        baselineLag48: routeAcc.baselineLag48,
        baseline_same_4w: routeAcc.baselineSame4w,
        baseline_same_7d: routeAcc.baselineSame4w,
        baseline_blend: routeAcc.baselineBlend,
        baseline_rule_based: routeAcc.baselineRuleBased,
        wape: rowWape,
        trustScore,
        trustReasonShort: trustCell?.reason_short,
        trustReasonFull: trustCell?.reason_full,
        trustHorizon: metricStep,
        trustSource: (
          trustScore !== undefined ? "stage3_artifact" : "fallback_wape"
        ) as "stage3_artifact" | "fallback_wape",
        anomalyRatio: ratio,
        waveForecasts: routeAcc.waveForecasts.slice(
          0,
          waveEndIndex - waveStartIndex + 1
        ),
        horizonForecasts: routeAcc.horizonForecasts.slice(0, horizonTimelineLength),
        horizonBaselineSame4w: routeAcc.horizonBaselineSame4w.slice(
          0,
          horizonTimelineLength
        ),
        horizonBaselineBlend: routeAcc.horizonBaselineBlend.slice(
          0,
          horizonTimelineLength
        ),
        horizonBaselineRule: routeAcc.horizonBaselineRule.slice(
          0,
          horizonTimelineLength
        ),
      }
    })
    .sort((a, b) => b.forecast - a.forecast)

  const transportAllocation = calculateRequiredTransport({
    forecasts: routeRows.map((row) => row.forecast),
    wapes: routeRows.map((row) => row.wape),
    anomalyRatios: routeRows.map((row) => row.anomalyRatio),
    cargoCapacity,
    utilization,
    reserveTrucks,
    strategy,
  })

  const sameSlotTransportAllocation = calculateRequiredTransport({
    forecasts: routeRows.map((row) => row.baselineSameSlot),
    wapes: routeRows.map(() => baseWape),
    anomalyRatios: routeRows.map(() => 1),
    cargoCapacity,
    utilization,
    reserveTrucks,
    strategy,
  })

  const lag48TransportAllocation = calculateRequiredTransport({
    forecasts: routeRows.map((row) => row.baselineLag48),
    wapes: routeRows.map(() => baseWape),
    anomalyRatios: routeRows.map(() => 1),
    cargoCapacity,
    utilization,
    reserveTrucks,
    strategy,
  })

  const effectiveTruckCapacity = Math.max(1e-6, cargoCapacity * utilization)

  const rows = routeRows.map((row, index) => ({
    ...row,
    baseTrucks: transportAllocation.baseByRoute[index] ?? 0,
    insuranceBuffer: transportAllocation.insuranceByRoute[index] ?? 0,
    totalTrucks: transportAllocation.totalByRoute[index] ?? 0,
    dispatchModelTrucks: transportAllocation.totalByRoute[index] ?? 0,
    dispatchSameSlotTrucks: sameSlotTransportAllocation.totalByRoute[index] ?? 0,
    dispatchLag48Trucks: lag48TransportAllocation.totalByRoute[index] ?? 0,
    dispatchRuleTrucks: Math.max(
      0,
      Math.ceil((row.baseline_rule_based ?? row.baselineLag48) / effectiveTruckCapacity)
    ),
  }))

  return {
    rows,
    meta: {
      selectedTimestamp: parsedSelectedTs,
      selectedWave,
      strategy,
      cargoCapacity,
      utilization,
      reserveTrucks,
      modelWape: baseWape,
      modelRelativeBias: Math.abs(baseRelativeBias),
      competitionScore: baseCompetitionScore,
      modelMae: Number.isFinite(baseMae) ? baseMae : undefined,
      modelRmse: Number.isFinite(baseRmse) ? baseRmse : undefined,
      modelSmape: Number.isFinite(baseSmape) ? baseSmape : undefined,
      horizonTimelineLength,
      horizonStepMinutes,
    },
  }
}

export function calculateDashboardMetrics({
  selectedTimestampIso,
  selectedHorizonHours,
  isCumulative,
}: CalculatorInput): DashboardMetricsResponse {
  const weightsDir = resolveWeightsDir()
  const modelDir = resolveModelDataDir()
  const submission = loadSubmissionSnapshot(weightsDir, modelDir)
  const metrics = loadMetricsFile(weightsDir, modelDir)
  const summary = ensureInferenceSummary(weightsDir, modelDir)

  const parsedSelectedTs = Number.isNaN(Date.parse(selectedTimestampIso))
    ? dashboardConfig.CURRENT_TIME
    : selectedTimestampIso
  const selectedTsMs = Date.parse(parsedSelectedTs)

  const availableHorizons = submission.horizonCount
  const selectedSteps = calculateSelectedSteps(
    selectedHorizonHours,
    availableHorizons,
    isCumulative
  )
  const effectiveHorizonHours = selectedHorizonHours

  const routeCount = Math.min(
    submission.routeCount,
    summary.baseline_by_route_and_slot.length,
    summary.office_ids.length
  )

  const planStepCount = Math.min(10, availableHorizons)

  let planHorizonForecast20h = 0
  let current2hForecast = 0
  let previous2hActual = 0
  let selectedForecastVolume = 0
  let selectedBaselineVolume = 0

  const anomalyOffices = new Set<number | string>()
  const newFlowOffices = new Set<number | string>()

  for (let routeIndex = 0; routeIndex < routeCount; routeIndex += 1) {
    const routeForecast = submission.forecastByRoute[routeIndex] ?? []
    const routeBaselineBySlot = summary.baseline_by_route_and_slot[routeIndex] ?? []

    const trendStepCount = Math.min(4, availableHorizons)
    let routeForecastFor2h = 0
    for (let step = 1; step <= trendStepCount; step += 1) {
      routeForecastFor2h += getRouteForecastAtStep(routeForecast, step)
    }
    current2hForecast += routeForecastFor2h

    const prevWindowStartMs = selectedTsMs - STEP_DURATION_MS
    previous2hActual += sumBaselineWindow(
      routeBaselineBySlot,
      prevWindowStartMs,
      selectedTsMs,
      summary.slots_per_day,
      summary.freq_minutes
    )

    for (let step = 1; step <= planStepCount; step += 1) {
      const stepForecast = getRouteForecastAtStep(routeForecast, step)
      planHorizonForecast20h += stepForecast
    }

    let routeSelectedForecast = 0
    let routeSelectedBaseline = 0

    for (const step of selectedSteps) {
      const windowStart = selectedTsMs + (step - 1) * STEP_DURATION_MS
      const windowEnd = selectedTsMs + step * STEP_DURATION_MS
      const stepForecast = getRouteForecastAtStep(routeForecast, step)
      const stepBaseline = sumBaselineWindow(
        routeBaselineBySlot,
        windowStart,
        windowEnd,
        summary.slots_per_day,
        summary.freq_minutes
      )

      routeSelectedForecast += stepForecast
      routeSelectedBaseline += stepBaseline
    }

    selectedForecastVolume += routeSelectedForecast
    selectedBaselineVolume += routeSelectedBaseline

    const officeId = summary.office_ids[routeIndex]

    if (routeSelectedBaseline < dashboardConfig.ANOMALY_MIN_BASELINE_VOLUME) {
      if (
        routeSelectedBaseline <= 0 &&
        routeSelectedForecast >= dashboardConfig.ANOMALY_MIN_NEW_FLOW_VOLUME
      ) {
        newFlowOffices.add(officeId)
      }
      continue
    }

    const ratio = safeRatio(routeSelectedForecast, routeSelectedBaseline)
    const absoluteDelta = routeSelectedForecast - routeSelectedBaseline
    const isAnomaly =
      Number.isFinite(ratio) &&
      ratio > dashboardConfig.ANOMALY_THRESHOLD &&
      absoluteDelta > 100

    if (isAnomaly) {
      anomalyOffices.add(officeId)
    }
  }

  const trendPct =
    ((current2hForecast - previous2hActual) / Math.max(previous2hActual, 1e-6)) * 100

  const selectedVsNormPct =
    ((selectedForecastVolume - selectedBaselineVolume) /
      Math.max(selectedBaselineVolume, 1e-6)) *
    100

  const requiredFleet = Math.ceil(
    selectedForecastVolume / Math.max(dashboardConfig.CARGO_CAPACITY, 1)
  )
  const fleetStatus = buildFleetStatus(requiredFleet)

  const selectedMetricsSteps = selectedSteps.map((step) =>
    Math.max(1, Math.min(step, availableHorizons))
  )

  const modelWapeValues = selectedMetricsSteps.map((step) => {
    const key = String(step)
    return (metrics.per_horizon[key]?.model.wape ?? metrics.model.wape) * 100
  })
  const baselineWapeValues = selectedMetricsSteps.map((step) => {
    const key = String(step)
    return (metrics.per_horizon[key]?.baseline.wape ?? metrics.baseline.wape) * 100
  })

  const modelWapeAvg =
    modelWapeValues.reduce((acc, value) => acc + value, 0) /
    Math.max(modelWapeValues.length, 1)
  const baselineWapeAvg =
    baselineWapeValues.reduce((acc, value) => acc + value, 0) /
    Math.max(baselineWapeValues.length, 1)
  const accuracyAvg = 100 - modelWapeAvg
  const efficiencyVsBaselinePct =
    ((baselineWapeAvg - modelWapeAvg) / Math.max(baselineWapeAvg, 1e-6)) * 100

  const totalOfficeCount = new Set(summary.office_ids.map((id) => String(id))).size
  const hasNewFlowForWindow = newFlowOffices.size > 0

  return {
    selectedTimestamp: parsedSelectedTs,
    cards: {
      dailyPlan24h: {
        title: "РџР»Р°РЅ РѕС‚РіСЂСѓР·РѕРє РЅР° СЃРµРіРѕРґРЅСЏ",
        value: `${formatUnits(planHorizonForecast20h)} РµРґ.`,
        badge: "10 С€Р°РіРѕРІ РїРѕ 2 С‡Р°СЃР°",
        primaryHint: "РЎСѓРјРјР° РїСЂРѕРіРЅРѕР·Р° РЅР° РїРѕР»РЅС‹Р№ С‚Р°РєС‚РёС‡РµСЃРєРёР№ РіРѕСЂРёР·РѕРЅС‚",
        secondaryHint: "РћРєРЅРѕ: 20 С‡Р°СЃРѕРІ",
        tone: "neutral",
        badgeIcon: "flat",
      },
      loadTrend: {
        title: "РЎРµРіРѕРґРЅСЏС€РЅРёР№ С‚СЂРµРЅРґ РЅР°РіСЂСѓР·РєРё",
        value: formatSignedPercent(trendPct),
        badge: "РћРїРµСЂР°С‚РёРІРЅР°СЏ РґРёРЅР°РјРёРєР°",
        primaryHint: "РџСЂРѕРіРЅРѕР·РЅРѕРµ РёР·РјРµРЅРµРЅРёРµ С‚РµРјРїР°",
        secondaryHint:
          trendPct > 0
            ? "РќР°РіСЂСѓР·РєР° СЂР°СЃС‚РµС‚"
            : trendPct < 0
              ? "РќР°РіСЂСѓР·РєР° СЃРЅРёР¶Р°РµС‚СЃСЏ"
              : "РќР°РіСЂСѓР·РєР° СЃС‚Р°Р±РёР»СЊРЅР°",
        tone: trendPct > 0 ? "danger" : trendPct < 0 ? "success" : "neutral",
        badgeIcon: trendPct > 0 ? "up" : trendPct < 0 ? "down" : "flat",
      },
      operationalVolumeH1: {
        title: "РџСЂРѕРіРЅРѕР· РѕР±СЉРµРјР°",
        value: `${formatUnits(selectedForecastVolume)} РµРґ.`,
        badge: isCumulative
          ? `РќР°РєРѕРїРёС‚РµР»СЊРЅРѕ РґРѕ ${selectedHorizonHours} С‡Р°СЃРѕРІ`
          : `РћРєРЅРѕ: ${selectedHorizonHours} С‡Р°СЃРѕРІ`,
        primaryHint: isCumulative
          ? "РќР°РєРѕРїР»РµРЅРЅС‹Р№ РѕР±СЉРµРј Р·Р° РїРµСЂРёРѕРґ"
          : "РџСЂРѕРіРЅРѕР· РЅР° РІС‹Р±СЂР°РЅРЅС‹Р№ РёРЅС‚РµСЂРІР°Р»",
        secondaryHint: hasNewFlowForWindow
          ? "РќРѕРІС‹Р№ РїРѕС‚РѕРє РѕС‚РЅРѕСЃРёС‚РµР»СЊРЅРѕ РЅРѕСЂРјС‹"
          : `${formatSignedPercent(selectedVsNormPct)} Рє РёСЃС‚РѕСЂРёС‡РµСЃРєРѕР№ РЅРѕСЂРјРµ`,
        tone: "neutral",
        badgeIcon: "flat",
      },
      fleetRequirementH1: {
        title: "РџРѕС‚СЂРµР±РЅРѕСЃС‚СЊ РІ С‚СЂР°РЅСЃРїРѕСЂС‚Рµ",
        value: `${formatUnits(requiredFleet)} С„СѓСЂ`,
        badge: `РџРѕС‚СЂРµР±РЅРѕСЃС‚СЊ РЅР° ${selectedHorizonHours} С‡`,
        primaryHint: "РћС†РµРЅРєР° РїРѕС‚СЂРµР±РЅРѕСЃС‚Рё РґР»СЏ РІС‹Р±СЂР°РЅРЅРѕРіРѕ РѕРєРЅР°",
        secondaryHint: fleetStatus.text,
        tone: fleetStatus.tone,
        badgeIcon: "flat",
      },
      accuracyH1: {
        title: "РќР°РґРµР¶РЅРѕСЃС‚СЊ РїСЂРѕРіРЅРѕР·Р°",
        value: `${toOneDecimal(accuracyAvg)}%`,
        badge: isCumulative
          ? `РЎСЂРµРґРЅСЏСЏ РїРѕ РёРЅС‚РµСЂРІР°Р»Р°Рј РґРѕ ${selectedHorizonHours} С‡`
          : `РћРєРЅРѕ: ${selectedHorizonHours} С‡Р°СЃРѕРІ`,
        primaryHint: `РўРѕС‡РЅРѕСЃС‚СЊ РѕРєРЅР°: ${selectedHorizonHours} С‡`,
        secondaryHint: `Р­С„С„РµРєС‚РёРІРЅРµРµ Р±Р°Р·РѕРІРѕРіРѕ РїСЂРѕРіРЅРѕР·Р° РЅР° ${formatSignedPercent(efficiencyVsBaselinePct)}`,
        tone: "neutral",
        badgeIcon: "flat",
      },
      operationalAnomalies: {
        title: "РЎРєР»Р°РґС‹, С‚СЂРµР±СѓСЋС‰РёРµ РІРЅРёРјР°РЅРёСЏ",
        value: `${formatUnits(anomalyOffices.size)} РёР· ${formatUnits(totalOfficeCount)} РѕР±СЉРµРєС‚РѕРІ`,
        badge: `РџРѕСЂРѕРі: +${toOneDecimal((dashboardConfig.ANOMALY_THRESHOLD - 1) * 100)}%`,
        primaryHint: `РџСЂРѕРІРµСЂРєР° РґР»СЏ РѕРєРЅР° ${selectedHorizonHours} С‡`,
        secondaryHint:
          newFlowOffices.size > 0
            ? `РќРѕРІС‹Р№ РїРѕС‚РѕРє: ${formatUnits(newFlowOffices.size)}`
            : "РћС‚РєР»РѕРЅРµРЅРёРµ РІС‹С€Рµ РїРѕСЂРѕРіР°",
        tone: anomalyOffices.size > 0 ? "danger" : "success",
        badgeIcon: "flat",
      },
    },
    meta: {
      availableHorizons,
      intervalMinutes: 120,
      selectedHorizonHours,
      isCumulative,
      effectiveHorizonHours,
    },
  }
}

/**
 * Р Р°СЃРїСЂРµРґРµР»СЏРµС‚ РѕР±С‰РµРµ РєРѕР»РёС‡РµСЃС‚РІРѕ С„СѓСЂ РїРѕ С‚Р°Р№Рј-СЃР»РѕС‚Р°Рј РЅР° РѕСЃРЅРѕРІРµ РіСЂР°РґРёРµРЅС‚Р° (СЂРѕСЃС‚Р°) РїСЂРѕРіРЅРѕР·Р°.
 * РСЃРїРѕР»СЊР·СѓРµС‚ "РњРµС‚РѕРґ РЅР°РёР±РѕР»СЊС€РµРіРѕ РѕСЃС‚Р°С‚РєР°" (Largest Remainder Method) РґР»СЏ С‡РµСЃС‚РЅРѕРіРѕ СЂР°СЃРїСЂРµРґРµР»РµРЅРёСЏ РґСЂРѕР±РЅС‹С… Р·РЅР°С‡РµРЅРёР№.
 *
 * @param totalTrucks РћР±С‰РµРµ РєРѕР»РёС‡РµСЃС‚РІРѕ С„СѓСЂ, РєРѕС‚РѕСЂРѕРµ РЅСѓР¶РЅРѕ СЂР°СЃРїСЂРµРґРµР»РёС‚СЊ РЅР° РІС‹Р±СЂР°РЅРЅС‹Р№ РіРѕСЂРёР·РѕРЅС‚ (РЅР°РїСЂРёРјРµСЂ, 2 С‡Р°СЃР°).
 * @param y_preds РњР°СЃСЃРёРІ РёР· РїСЂРѕРіРЅРѕР·РѕРІ РјРѕРґРµР»Рё (target_2h) РґР»СЏ РІС‹С‡РёСЃР»РµРЅРёСЏ РґРµР»СЊС‚ РјРµР¶РґСѓ СЃР»РѕС‚Р°РјРё.
 * @returns РњР°СЃСЃРёРІ С†РµР»С‹С… С‡РёСЃРµР», СЃСѓРјРјР° РєРѕС‚РѕСЂС‹С… СЃС‚СЂРѕРіРѕ СЂР°РІРЅР° totalTrucks.
 */
export function distributeTrucksSmart(totalTrucks: number, y_preds: number[]): number[] {
  const NUM_SLOTS = y_preds.length // РћР±С‹С‡РЅРѕ 4 СЃР»РѕС‚Р° (РїРѕ 30 РјРёРЅСѓС‚) РІ 2-С‡Р°СЃРѕРІРѕРј РѕРєРЅРµ
  if (totalTrucks === 0 || NUM_SLOTS === 0) return new Array(NUM_SLOTS || 4).fill(0)

  // 1. РЎС‡РёС‚Р°РµРј РґРµР»СЊС‚С‹ (РіСЂР°РґРёРµРЅС‚С‹) РјРµР¶РґСѓ СЃРѕСЃРµРґРЅРёРјРё РїСЂРѕРіРЅРѕР·Р°РјРё. РџРµСЂРІС‹Р№ СЃР»РѕС‚ Р±РµСЂРµРј РєР°Рє Р±Р°Р·РѕРІС‹Р№ (0).
  const D = new Array(NUM_SLOTS).fill(0)
  for (let i = 1; i < NUM_SLOTS; i++) {
    D[i] = Math.max(0, y_preds[i] - y_preds[i - 1])
  }

  const sumD = D.reduce((a, b) => a + b, 0)

  // 2. РЎС‡РёС‚Р°РµРј РІРµСЃР°.
  // Р‘Р°Р·РѕРІР°СЏ РєРІРѕС‚Р° 15% РЅР° РєР°Р¶РґС‹Р№ СЃР»РѕС‚ РіР°СЂР°РЅС‚РёСЂСѓРµС‚, С‡С‚Рѕ СЃРєР»Р°Рґ РЅРµ Р±СѓРґРµС‚ РїСЂРѕСЃС‚Р°РёРІР°С‚СЊ РґР°Р¶Рµ РІРЅРµ РїРёРєРѕРІ.
  const baseWeight = 0.15
  const dynamicPool = Math.max(0, 1.0 - baseWeight * NUM_SLOTS) // РћСЃС‚Р°РІС€РёРµСЃСЏ 40% РґР»СЏ СЂР°СЃРїСЂРµРґРµР»РµРЅРёСЏ РїРѕ РїРёРєР°Рј

  const weights = D.map((d) => {
    // Р•СЃР»Рё С‚СЂРµРЅРґ С‚РѕР»СЊРєРѕ РїР°РґР°РµС‚ (СЃСѓРјРјР° РґРµР»СЊС‚ 0), СЂР°СЃРїСЂРµРґРµР»СЏРµРј РІСЃС‘ СЂР°РІРЅРѕРјРµСЂРЅРѕ
    if (sumD === 0) return 1 / NUM_SLOTS
    return baseWeight + dynamicPool * (d / sumD)
  })

  // 3. Р Р°Р·РґР°РµРј С†РµР»С‹Рµ РјР°С€РёРЅС‹ (Floor)
  const exactTrucks = weights.map((w) => totalTrucks * w)
  const result = exactTrucks.map((val) => Math.floor(val))

  const remainder = totalTrucks - result.reduce((a, b) => a + b, 0) // РћСЃС‚Р°РІС€РёРµСЃСЏ РЅРµСЂР°СЃРїСЂРµРґРµР»РµРЅРЅС‹Рµ РјР°С€РёРЅС‹

  // 4. РЎРѕСЂС‚РёСЂСѓРµРј СЃР»РѕС‚С‹ РїРѕ СЂР°Р·РјРµСЂСѓ РёС… РґСЂРѕР±РЅРѕР№ С‡Р°СЃС‚Рё РїРѕ СѓР±С‹РІР°РЅРёСЋ (РњРµС‚РѕРґ РЅР°РёР±РѕР»СЊС€РµРіРѕ РѕСЃС‚Р°С‚РєР°)
  const fractionalParts = exactTrucks.map((val, idx) => ({
    idx,
    frac: val - Math.floor(val),
  }))

  fractionalParts.sort((a, b) => b.frac - a.frac)

  // 5. Р Р°Р·РґР°РµРј РїРѕ 1 РјР°С€РёРЅРµ РёР· РѕСЃС‚Р°С‚РєР° С‚РµРј СЃР»РѕС‚Р°Рј, Сѓ РєРѕС‚РѕСЂС‹С… РґСЂРѕР±РЅР°СЏ С‡Р°СЃС‚СЊ Р±С‹Р»Р° СЃР°РјРѕР№ Р±РѕР»СЊС€РѕР№
  for (let i = 0; i < Math.round(remainder); i++) {
    if (fractionalParts[i]) {
      result[fractionalParts[i].idx] += 1
    }
  }

  return result
}

