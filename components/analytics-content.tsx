"use client"

import * as React from "react"
import { Info } from "lucide-react"
import {
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from "recharts"

import { SiteHeader } from "@/components/site-header"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { dashboardConfig } from "@/lib/dashboard-config"
import { SimulationProvider, useSimulation } from "@/lib/contexts/SimulationContext"

type OfficialVariantSummary = {
  score: number
  wape: number
  rbias: number
}

type OfficialHorizonPoint = {
  horizon: string
  evalRows: number
  mlScore: number
  mlWape: number
  mlRBias: number
  strongScore: number
  strongWape: number
  strongRBias: number
  primitiveScore: number
  primitiveWape: number
  primitiveRBias: number
}

type ProxyHorizonPoint = {
  horizon: string
  rows: number
  winRate: number
  tieRate: number
  expectedLossAbsMean: number
  expectedLossWapeLike: number
  deltaAbsErrMean: number
}

type HeatmapCell = {
  horizon: string
  rows: number
  winRate: number
  tieRate: number
  expectedLossAbsMean: number
  expectedLossWapeLike: number
  deltaAbsErrMean: number
}

type HeatmapRow = {
  officeFromId: string
  rowsTotal: number
  cells: HeatmapCell[]
}

type OperationsStatusPoint = {
  status: "ok" | "review" | "drop" | "critical"
  label: string
  count: number
  share: number
}

type FreshnessBlock = {
  source: string
  rows: number
  p50Minutes: number
  p95Minutes: number
  maxMinutes: number
  anchorTimestampUtc: string | null
  generatedAtUtc: string | null
}

type LatencyBlock = {
  p50: number
  p95: number
  mean: number
  min: number
  max: number
  samples: number
  windowSize: number
}

type TrustCalibrationGlobal = {
  min: number
  p10: number
  p50: number
  p90: number
  max: number
  shareGe70: number
  shareGe85: number
}

type TrustWeight = {
  signal: string
  weight: number
}

type TrustDistributionBin = {
  from: number
  to: number
  center: number
  label: string
  count: number
  share: number
}

type TrustDistribution = {
  routes: number
  bins: TrustDistributionBin[]
  stats: {
    min: number
    p10: number
    p50: number
    p90: number
    max: number
    mean: number
    shareGe70: number
    shareGe85: number
  }
}

type TrustBlock = {
  metricRole: string
  officialMetricGuardrail: string
  agreementMode: string
  agreementModeNote: string | null
  calibration: {
    mode: string
    applied: boolean
    note: string | null
    beforeGlobal: TrustCalibrationGlobal
    afterGlobal: TrustCalibrationGlobal
  }
  globalStats: {
    rows: number
    routes: number
    horizons: number
    meanPct: number
    p10Pct: number
    p50Pct: number
    p90Pct: number
  }
  weights: TrustWeight[]
  ablation: {
    deltaExpectedLossWapeLike: number
    deltaAucBinned: number
    deltaAutoVsManualWinLift: number
  } | null
  waveDistribution: TrustDistribution
  generatedAtUtc: string | null
  anchorTimestampUtc: string | null
  ablationGeneratedAtUtc: string | null
}

type AnalyticsBackendPayload = {
  generatedAtUtc: string
  officialMetrics: {
    metricName: string
    variants: {
      ml: OfficialVariantSummary
      strongBaselineBlendRoll48Same7d: OfficialVariantSummary
      primitiveBaselineSame4w: OfficialVariantSummary
    }
    deltas: {
      mlVsStrongScore: number
      mlVsPrimitiveScore: number
    }
  }
  horizonChart: {
    official: OfficialHorizonPoint[]
    proxy: ProxyHorizonPoint[]
  }
  officeHorizonHeatmap: {
    horizons: string[]
    rows: HeatmapRow[]
    coverage: {
      offices: number
      rows: number
    }
  }
  operations: {
    totalRoutes: number
    autoDecision: { count: number; share: number }
    manualReview: { count: number; share: number }
    operatorAttention: { count: number; share: number }
    pieStatuses: OperationsStatusPoint[]
    selectedWave: number
    selectedTimestamp: string
    strategy: "economy" | "balance" | "reliable"
  }
  system: {
    dataFreshness: FreshnessBlock
    inferenceLatencyMs: LatencyBlock
  }
  trust: TrustBlock
  sources: Record<string, string>
}

type MlInsightsPayload = {
  analyticsBackend?: AnalyticsBackendPayload
}

type HeatMetric = "winRate" | "expectedLossWapeLike"

const glassPanelClass =
  "rounded-xl bg-[rgba(22,27,29,0.38)] shadow-2xl backdrop-blur-md transition-all duration-300 ease-in-out will-change-transform hover:-translate-y-[4px] hover:shadow-[0_14px_34px_rgba(0,0,0,0.34)]"
const glassSectionClass = glassPanelClass
const glassPanelHoverClass = ""
const toggleActiveClass =
  "border-white/20 bg-gradient-to-r from-slate-600/70 to-sky-700/70 text-white shadow-sm hover:from-slate-500 hover:to-sky-600"
const toggleIdleClass = "border-white/20 bg-white/5 text-white hover:bg-white/10"

const analyticsWrapperGradient: React.CSSProperties = {
  backgroundColor: "#061219",
  backgroundImage:
    "radial-gradient(120% 44% at 50% -10%, rgba(255, 255, 255, 0.04) 0px, rgba(255, 255, 255, 0) 320px), linear-gradient(to bottom, rgba(0, 0, 0, 0.94) 0px, rgba(0, 0, 0, 0.9) 84px, rgba(2, 9, 14, 0.86) 150px, rgba(5, 18, 26, 0.8) 240px, rgba(9, 30, 40, 0.74) 360px, rgba(13, 43, 56, 0.68) 520px, rgba(18, 57, 72, 0.62) 720px, rgba(20, 66, 84, 0.53) 980px, rgba(26, 80, 99, 0.49) 1240px)",
}
const rechartsTooltipGlassStyle: React.CSSProperties = {
  backgroundColor: "rgba(7, 15, 24, 0.74)",
  border: "1px solid rgba(148, 163, 184, 0.36)",
  borderRadius: 12,
  color: "#f8fafc",
  backdropFilter: "blur(10px)",
  WebkitBackdropFilter: "blur(10px)",
  boxShadow: "0 10px 30px rgba(2, 6, 23, 0.45)",
}

const statusColors: Record<OperationsStatusPoint["status"], string> = {
  ok: "#10b981",
  review: "#f59e0b",
  drop: "#06b6d4",
  critical: "#ef4444",
}

const trustSignalLabels: Record<string, string> = {
  horizon: "Горизонт",
  stability: "Стабильность маршрута",
  agreement_with_blend: "Согласие с сильным ориентиром",
  route_error_history: "История ошибки маршрута",
  office_error_history: "История ошибки офиса",
  history_completeness: "Полнота истории",
  freshness: "Свежесть данных",
}

function toNumber(value: unknown, fallback = 0): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value))
}

function formatInt(value: number): string {
  return Math.round(value).toLocaleString("ru-RU")
}

function formatPercent(value: number, digits = 1): string {
  return `${(value * 100).toFixed(digits)}%`
}

function formatTrustPercent(value: number, digits = 1): string {
  return `${toNumber(value).toFixed(digits)}%`
}

function formatUtc(value: string | null | undefined): string {
  if (!value) return "-"
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return `${parsed.toLocaleDateString("ru-RU")} ${parsed.toLocaleTimeString("ru-RU", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "Europe/Moscow",
  })}`
}

function scoreDeltaTone(delta: number): "text-emerald-400" | "text-rose-400" | "text-slate-200" {
  if (delta < 0) return "text-emerald-400"
  if (delta > 0) return "text-rose-400"
  return "text-slate-200"
}

function scoreDeltaLabel(delta: number): string {
  if (delta < 0) return `Лучше на ${Math.abs(delta).toFixed(4)}`
  if (delta > 0) return `Хуже на ${Math.abs(delta).toFixed(4)}`
  return "Паритет"
}

function heatColorByWinRate(value: number): string {
  const normalized = clamp((value - 0.5) / 0.5, -1, 1)
  const intensity = Math.abs(normalized)
  if (normalized >= 0) {
    return `linear-gradient(135deg, rgba(4,120,87,${0.18 + intensity * 0.45}), rgba(16,185,129,${0.18 + intensity * 0.55}))`
  }
  return `linear-gradient(135deg, rgba(127,29,29,${0.18 + intensity * 0.45}), rgba(239,68,68,${0.18 + intensity * 0.55}))`
}

function heatColorByExpectedLoss(value: number, maxValue: number): string {
  if (maxValue <= 0) return "rgba(30,41,59,0.35)"
  const normalized = clamp(value / maxValue, 0, 1)
  return `linear-gradient(135deg, rgba(127,29,29,${0.12 + normalized * 0.45}), rgba(239,68,68,${0.14 + normalized * 0.5}))`
}

function Hint({ text }: { text: string }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            aria-label="Пояснение"
            className="inline-flex size-5 items-center justify-center rounded-full border border-white/20 bg-white/5 text-slate-300 shadow-2xl backdrop-blur-md transition-all duration-300 hover:border-[#4a86ad]/60 hover:bg-white/12 hover:text-white"
          >
            <Info className="size-3.5" />
          </button>
        </TooltipTrigger>
        <TooltipContent
          side="bottom"
          align="end"
          sideOffset={8}
          className="max-w-sm whitespace-pre-line text-xs leading-relaxed"
        >
          {text}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

function MetricCard({
  title,
  value,
  hint,
  tone = "text-white",
  sub,
  className,
}: {
  title: string
  value: string
  hint: string
  tone?: string
  sub?: string
  className?: string
}) {
  return (
    <div className={`${glassPanelClass} ${glassPanelHoverClass} p-4 ${className ?? ""}`}>
      <div className="flex items-start justify-between gap-3">
        <p className="text-sm font-semibold text-white">{title}</p>
        <Hint text={hint} />
      </div>
      <p className={`mt-3 text-3xl font-semibold ${tone}`}>{value}</p>
      {sub ? <p className="mt-2 text-sm text-slate-400">{sub}</p> : null}
    </div>
  )
}

function AnalyticsGrid() {
  const {
    selectedWave,
    cargoCapacity,
    utilization,
    reserveTrucks,
    strategy,
    refreshNonce,
    isHydrated,
  } = useSimulation()

  const [payload, setPayload] = React.useState<MlInsightsPayload | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const [isLoading, setIsLoading] = React.useState(true)
  const [activeStatusIndex, setActiveStatusIndex] = React.useState<number>(-1)
  const [heatMetric, setHeatMetric] = React.useState<HeatMetric>("winRate")

  React.useEffect(() => {
    if (!isHydrated) return

    const controller = new AbortController()
    const load = async () => {
      setIsLoading(true)
      setError(null)
      try {
        const params = new URLSearchParams({
          timestamp: dashboardConfig.CURRENT_TIME,
          wave: String(selectedWave),
          cargoCapacity: String(cargoCapacity),
          utilization: String(utilization),
          reserveTrucks: String(reserveTrucks),
          strategy,
        })

        const response = await fetch(`/api/ml-insights?${params.toString()}`, {
          method: "GET",
          cache: "no-store",
          signal: controller.signal,
        })
        if (!response.ok) throw new Error("Не удалось загрузить данные аналитики.")

        setPayload((await response.json()) as MlInsightsPayload)
      } catch (loadError) {
        if (loadError instanceof DOMException && loadError.name === "AbortError") return
        setError(loadError instanceof Error ? loadError.message : "Ошибка загрузки аналитики.")
      } finally {
        setIsLoading(false)
      }
    }

    void load()
    return () => controller.abort()
  }, [isHydrated, selectedWave, cargoCapacity, utilization, reserveTrucks, strategy, refreshNonce])

  if (error && !payload) {
    return <div className="flex h-64 items-center justify-center text-rose-300">{error}</div>
  }

  const analytics = payload?.analyticsBackend
  if (!analytics || isLoading) {
    return <div className="flex h-64 items-center justify-center text-slate-400">Загрузка аналитики...</div>
  }

  const variants = analytics.officialMetrics.variants
  const statusData = analytics.operations.pieStatuses
  const okEntry = statusData.find((item) => item.status === "ok")
  const reviewEntry = statusData.find((item) => item.status === "review")
  const dropEntry = statusData.find((item) => item.status === "drop")
  const criticalEntry = statusData.find((item) => item.status === "critical")
  const okCount = okEntry?.count ?? 0
  const okShare = okEntry?.share ?? 0
  const reviewCount = reviewEntry?.count ?? 0
  const reviewShare = reviewEntry?.share ?? 0
  const dropCount = dropEntry?.count ?? 0
  const criticalCount = criticalEntry?.count ?? 0
  const dropShare = dropEntry?.share ?? 0
  const criticalShare = criticalEntry?.share ?? 0
  const heatmapRows = analytics.officeHorizonHeatmap.rows
    .slice()
    .sort((a, b) => b.rowsTotal - a.rowsTotal)
    .slice(0, 10)
  const maxExpectedLoss = heatmapRows.reduce((maxValue, row) => {
    const rowMax = row.cells.reduce((cellMax, cell) => Math.max(cellMax, toNumber(cell.expectedLossWapeLike)), 0)
    return Math.max(maxValue, rowMax)
  }, 0)

  const trust = analytics.trust
  const trustAfter = trust?.calibration.afterGlobal
  const trustWave = trust?.waveDistribution
  const trustWeights = (trust?.weights ?? []).slice(0, 7)
  const trustP10 = toNumber(trustWave?.stats.p10, toNumber(trustAfter?.p10))
  const trustP50 = toNumber(trustWave?.stats.p50, toNumber(trustAfter?.p50))
  const trustP90 = toNumber(trustWave?.stats.p90, toNumber(trustAfter?.p90))
  const trustShareGe70 = toNumber(trustWave?.stats.shareGe70, toNumber(trustAfter?.shareGe70))
  const trustShareGe85 = toNumber(trustWave?.stats.shareGe85, toNumber(trustAfter?.shareGe85))
  const trustDistributionSeries = (trustWave?.bins ?? []).map((bin) => ({
    bucket: bin.label,
    sharePct: Number((bin.share * 100).toFixed(2)),
    count: bin.count,
  }))

  return (
    <div className="flex flex-col gap-4 rounded-2xl px-4 py-4 md:gap-6 md:py-6 lg:px-6" style={analyticsWrapperGradient}>
      <div className={`${glassSectionClass} p-4 md:p-5`}>
        <div className="flex items-center justify-between gap-3">
          <p className="text-base font-semibold text-white md:text-lg">Quality-блок: официальный score и сравнение с baseline</p>
        </div>

        <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-3">
          <MetricCard
            title="ML"
            value={variants.ml.score.toFixed(4)}
            hint={"Итоговое качество нашего прогноза по правилам конкурса. Чем ниже, тем лучше.\nФормула: Score = WAPE + |RBias|.\nWAPE = Σ|y - ŷ| / Σy.\nRBias = |(Σŷ / Σy) - 1|."}
            sub={`WAPE ${formatPercent(variants.ml.wape, 2)} • |RBias| ${formatPercent(variants.ml.rbias, 2)}`}
          />
          <MetricCard
            title="Strong baseline (blend_roll48_same7d)"
            value={variants.strongBaselineBlendRoll48Same7d.score.toFixed(4)}
            hint={"Сильный baseline, чтобы понять, дает ли модель дополнительную пользу.\nФормула: Score = WAPE + |RBias|.\nWAPE = Σ|y - ŷ| / Σy.\nRBias = |(Σŷ / Σy) - 1|."}
            sub={`WAPE ${formatPercent(variants.strongBaselineBlendRoll48Same7d.wape, 2)} • |RBias| ${formatPercent(variants.strongBaselineBlendRoll48Same7d.rbias, 2)}`}
          />
          <MetricCard
            title="Primitive baseline (same_4w)"
            value={variants.primitiveBaselineSame4w.score.toFixed(4)}
            hint={"Простой baseline как минимальный уровень качества, ниже которого опускаться нельзя.\nФормула: Score = WAPE + |RBias|.\nWAPE = Σ|y - ŷ| / Σy.\nRBias = |(Σŷ / Σy) - 1|."}
            sub={`WAPE ${formatPercent(variants.primitiveBaselineSame4w.wape, 2)} • |RBias| ${formatPercent(variants.primitiveBaselineSame4w.rbias, 2)}`}
          />
        </div>

        <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
          <MetricCard
            title="ML vs strong baseline"
            value={scoreDeltaLabel(analytics.officialMetrics.deltas.mlVsStrongScore)}
            tone={scoreDeltaTone(analytics.officialMetrics.deltas.mlVsStrongScore)}
            hint={"Показывает выигрыш/проигрыш нашей модели относительно strong baseline.\nФормула: Δscore = score_ML - score_strong_baseline.\nЕсли Δscore < 0, модель лучше."}
            sub={`Δ score = ${analytics.officialMetrics.deltas.mlVsStrongScore.toFixed(4)}`}
          />
          <MetricCard
            title="ML vs primitive baseline"
            value={scoreDeltaLabel(analytics.officialMetrics.deltas.mlVsPrimitiveScore)}
            tone={scoreDeltaTone(analytics.officialMetrics.deltas.mlVsPrimitiveScore)}
            hint={"Показывает выигрыш/проигрыш нашей модели относительно primitive baseline.\nФормула: Δscore = score_ML - score_primitive_baseline.\nЕсли Δscore < 0, модель лучше."}
            sub={`Δ score = ${analytics.officialMetrics.deltas.mlVsPrimitiveScore.toFixed(4)}`}
          />
        </div>

        <div className={`${glassPanelClass} mt-5 h-[320px] p-3`}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={analytics.horizonChart.official}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
              <XAxis dataKey="horizon" tick={{ fill: "#94a3b8", fontSize: 11 }} axisLine={{ stroke: "#334155" }} tickLine={false} />
              <YAxis
                tick={{ fill: "#94a3b8", fontSize: 11 }}
                axisLine={{ stroke: "#334155" }}
                tickLine={false}
                tickFormatter={(value: number) => value.toFixed(3)}
              />
              <RechartsTooltip
                formatter={(value: unknown, name: unknown) => [
                  toNumber(value).toFixed(4),
                  String(name ?? ""),
                ]}
                labelFormatter={(label) => `Горизонт ${label}`}
                contentStyle={rechartsTooltipGlassStyle}
              />
              <Line
                type="monotone"
                dataKey="mlScore"
                name="ML"
                stroke="#10b981"
                strokeWidth={2.5}
                dot={{ r: 3, fill: "#10b981" }}
              />
              <Line
                type="monotone"
                dataKey="strongScore"
                name="Strong baseline"
                stroke="#f59e0b"
                strokeWidth={2.2}
                dot={{ r: 3, fill: "#f59e0b" }}
              />
              <Line
                type="monotone"
                dataKey="primitiveScore"
                name="Primitive baseline"
                stroke="#38bdf8"
                strokeWidth={2.2}
                dot={{ r: 3, fill: "#38bdf8" }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.2fr_0.9fr]">
        <div className={`${glassSectionClass} p-4 md:p-5`}>
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <p className="text-base font-semibold text-white md:text-lg">Распределение маршрутов по режимам работы</p>
            </div>
          </div>
          <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2">
            <MetricCard
              title="Автоматически обработаны"
              value={`${formatInt(okCount)} (${formatPercent(okShare)})`}
              tone="text-emerald-400"
              hint="Маршруты, где система не требует вмешательства диспетчера."
              sub="В норме"
            />
            <MetricCard
              title="На проверке у диспетчера"
              value={`${formatInt(reviewCount)} (${formatPercent(reviewShare)})`}
              tone="text-amber-300"
              hint="Маршруты, где нужна ручная проверка перед отправкой заявки."
              sub="Требует проверки"
            />
            <MetricCard
              title="Риск снижения отгрузки"
              value={`${formatInt(dropCount)} (${formatPercent(dropShare)})`}
              tone="text-cyan-300"
              hint="Маршруты с аномальным падением объема относительно ожидаемого уровня."
              sub="Аномальное снижение"
            />
            <MetricCard
              title="Риск перегруза"
              value={`${formatInt(criticalCount)} (${formatPercent(criticalShare)})`}
              tone="text-rose-300"
              hint="Маршруты с аномальным ростом нагрузки и повышенным риском дефицита транспорта."
              sub="Критическая нагрузка"
            />
          </div>
        </div>

        <div className={`${glassSectionClass} w-full p-3 md:p-4 [&_[tabindex]:focus]:outline-none [&_[tabindex]:focus-visible]:outline-none [&_svg:focus]:outline-none [&_svg:focus-visible]:outline-none`}>
          <div className="relative h-[330px] select-none md:h-[350px]">
            <div className="pointer-events-none absolute inset-10 rounded-full bg-[radial-gradient(circle,rgba(16,185,129,0.16)_0%,rgba(2,6,23,0)_68%)] blur-2xl" />
            <ResponsiveContainer width="100%" height="100%">
              <PieChart accessibilityLayer={false}>
                <Pie
                  data={statusData}
                  dataKey="count"
                  nameKey="label"
                  cx="50%"
                  cy="50%"
                  innerRadius="58%"
                  outerRadius="95%"
                  paddingAngle={2}
                  onMouseEnter={(_, index) => setActiveStatusIndex(index)}
                  onMouseLeave={() => setActiveStatusIndex(-1)}
                >
                  {statusData.map((entry, index) => (
                    <Cell
                      key={entry.status}
                      fill={statusColors[entry.status]}
                      fillOpacity={activeStatusIndex === -1 || activeStatusIndex === index ? 1 : 0.55}
                      stroke={activeStatusIndex === index ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.65)"}
                      strokeWidth={activeStatusIndex === index ? 2 : 1}
                    />
                  ))}
                </Pie>
                <RechartsTooltip
                  cursor={false}
                  formatter={(value: unknown, _name: unknown, item: unknown) => {
                    const payload =
                      item && typeof item === "object" && "payload" in item
                        ? (item as { payload?: OperationsStatusPoint }).payload
                        : undefined
                    return [
                      `${formatInt(toNumber(value))} (${formatPercent(toNumber(payload?.share))})`,
                      payload?.label ?? "Статус",
                    ]
                  }}
                  contentStyle={rechartsTooltipGlassStyle}
                />
              </PieChart>
            </ResponsiveContainer>
            <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
              <span className="text-xl font-medium text-slate-300 md:text-2xl">Всего</span>
              <span className="mt-1 text-4xl font-semibold leading-none text-white md:text-5xl">{formatInt(analytics.operations.totalRoutes)}</span>
            </div>
          </div>
        </div>
      </div>

      <div className={`${glassSectionClass} p-4 md:p-5`}>
        <div className="flex items-center justify-between gap-3">
          <div>
            <p className="text-base font-semibold text-white md:text-lg">Надежность автопринятия решения</p>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-4">
          <MetricCard
            title="Нижняя граница надежности"
            value={formatTrustPercent(trustP10)}
            hint="P10: ниже этого уровня находится 10% маршрутов по надежности."
            sub="P10 (10-й перцентиль)"
          />
          <MetricCard
            title="Типовой уровень надежности"
            value={formatTrustPercent(trustP50)}
            hint="P50: центральный уровень надежности (медиана), половина маршрутов выше, половина ниже."
            sub="P50 (медиана)"
          />
          <MetricCard
            title="Верхняя граница надежности"
            value={formatTrustPercent(trustP90)}
            hint="P90: уровень верхних 10% маршрутов по надежности."
            sub="P90 (90-й перцентиль)"
          />
          <MetricCard
            title="Маршруты с высокой надежностью"
            value={formatPercent(trustShareGe70)}
            hint="Доля маршрутов, где можно чаще опираться на авто-решение."
            sub={`>=85%: ${formatPercent(trustShareGe85)}`}
          />
        </div>

        <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
          <div className={`${glassPanelClass} p-4`}>
            <p className="text-sm font-semibold text-white">Распределение надежности по маршрутам</p>
            <div className="mt-3 h-[360px] xl:h-[420px]">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={trustDistributionSeries}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                  <defs>
                    <linearGradient id="trustDistLine" x1="0" y1="0" x2="1" y2="0">
                      <stop offset="0%" stopColor="#0284c7" />
                      <stop offset="100%" stopColor="#10b981" />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="bucket" tick={{ fill: "#94a3b8", fontSize: 10 }} axisLine={{ stroke: "#334155" }} tickLine={false} />
                  <YAxis
                    tick={{ fill: "#94a3b8", fontSize: 10 }}
                    axisLine={{ stroke: "#334155" }}
                    tickLine={false}
                    tickFormatter={(value: number) => `${value.toFixed(0)}%`}
                  />
                  <RechartsTooltip
                    formatter={(value: unknown, name: unknown, item: unknown) => {
                      const payload =
                        item && typeof item === "object" && "payload" in item
                          ? (item as { payload?: { count?: number } }).payload
                          : undefined
                      const count = Number(payload?.count ?? 0)
                      return [`${toNumber(value).toFixed(2)}% (n=${formatInt(count)})`, String(name ?? "")]
                    }}
                    labelFormatter={(label) => `Диапазон trust ${label}`}
                    contentStyle={rechartsTooltipGlassStyle}
                  />
                  <Line
                    type="monotone"
                    dataKey="sharePct"
                    name="Доля маршрутов"
                    stroke="url(#trustDistLine)"
                    strokeWidth={2.5}
                    dot={{ r: 3, fill: "#10b981", stroke: "#0f172a", strokeWidth: 1 }}
                    activeDot={{ r: 5, fill: "#10b981", stroke: "#ffffff", strokeWidth: 1 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className={`${glassPanelClass} p-4`}>
            <p className="text-sm font-semibold text-white">Что влияет на надежность</p>
            <div className="mt-3 space-y-2">
              {trustWeights.map((item) => {
                const label = trustSignalLabels[item.signal] ?? item.signal
                return (
                  <div key={item.signal}>
                    <div className="flex items-center justify-between text-xs text-slate-300">
                      <span>{label}</span>
                      <span>{(item.weight * 100).toFixed(1)}%</span>
                    </div>
                    <div className="mt-1 h-2 rounded-full bg-slate-800">
                      <div
                        className="h-2 rounded-full bg-gradient-to-r from-sky-600 to-emerald-500"
                        style={{ width: `${clamp(item.weight * 100, 0, 100)}%` }}
                      />
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      </div>

      <div className={`${glassSectionClass} p-4 md:p-5`}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-base font-semibold text-white md:text-lg">Где наш прогноз надежнее ориентира (офис × горизонт)</p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setHeatMetric("winRate")}
              className={`rounded-full border px-3 py-1 text-xs font-semibold transition-colors duration-200 ${
                heatMetric === "winRate" ? toggleActiveClass : toggleIdleClass
              }`}
            >
              Чаще точнее
            </button>
            <button
              type="button"
              onClick={() => setHeatMetric("expectedLossWapeLike")}
              className={`rounded-full border px-3 py-1 text-xs font-semibold transition-colors duration-200 ${
                heatMetric === "expectedLossWapeLike" ? toggleActiveClass : toggleIdleClass
              }`}
            >
              Риск ошибки
            </button>
          </div>
        </div>

        <div className={`${glassPanelClass} mt-3 overflow-x-auto p-2`}>
          <table className="min-w-[920px] w-full border-separate border-spacing-y-1 text-xs">
            <thead>
              <tr className="border-b border-slate-700/60 text-slate-400">
                <th className="px-2 py-2 text-left font-medium">Офис</th>
                {analytics.officeHorizonHeatmap.horizons.map((horizon) => (
                  <th key={horizon} className="px-2 py-2 text-center font-medium">
                    {horizon}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {heatmapRows.map((row) => (
                <tr key={row.officeFromId}>
                  <td className="px-2 py-2 text-slate-200">
                    Офис {row.officeFromId}
                    <div className="text-[10px] text-slate-400">{formatInt(row.rowsTotal)} строк</div>
                  </td>
                  {row.cells.map((cell) => {
                    const winRate = toNumber(cell.winRate)
                    const loss = toNumber(cell.expectedLossWapeLike)
                    const background =
                      heatMetric === "winRate"
                        ? heatColorByWinRate(winRate)
                        : heatColorByExpectedLoss(loss, maxExpectedLoss)
                    return (
                      <td key={`${row.officeFromId}-${cell.horizon}`} className="px-1 py-1 text-center">
                        <div
                          className="rounded-md px-2 py-1 shadow-[inset_0_0_0_1px_rgba(148,163,184,0.08)]"
                          style={{ background }}
                          title={`Офис ${row.officeFromId}, ${cell.horizon}\nЧаще точнее: ${(winRate * 100).toFixed(1)}%\nРиск ошибки: ${(loss * 100).toFixed(2)}%\nN = ${formatInt(cell.rows)}`}
                        >
                          <div className="font-semibold text-slate-100">
                            {heatMetric === "winRate"
                              ? `${(winRate * 100).toFixed(1)}%`
                              : `${(loss * 100).toFixed(2)}%`}
                          </div>
                          <div className="text-[10px] text-slate-300/90">n={formatInt(cell.rows)}</div>
                        </div>
                      </td>
                    )
                  })}
                </tr>
              ))}
              {heatmapRows.length === 0 ? (
                <tr>
                  <td colSpan={analytics.officeHorizonHeatmap.horizons.length + 1} className="px-3 py-6 text-center text-slate-400">
                    Нет данных для heatmap.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </div>

      <div className={`${glassSectionClass} p-4 md:p-5`}>
        <p className="text-base font-semibold text-white md:text-lg">Стабильность данных и скорость расчета</p>
        <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
          <MetricCard
            title="Обычная задержка данных"
            value={`${analytics.system.dataFreshness.p50Minutes.toFixed(1)} мин`}
            hint="Типовая задержка между опорным временем и данными, на которых считается надежность."
            sub={`p95: ${analytics.system.dataFreshness.p95Minutes.toFixed(1)} мин`}
          />
          <MetricCard
            title="Максимальная задержка данных"
            value={`${analytics.system.dataFreshness.maxMinutes.toFixed(1)} мин`}
            hint="Худший случай задержки в текущем наборе."
            sub={`строк: ${formatInt(analytics.system.dataFreshness.rows)}`}
          />
          <MetricCard
            title="Обычное время расчета"
            value={`${analytics.system.inferenceLatencyMs.p50.toFixed(2)} ms`}
            hint="Типовое время ответа сервиса при расчете прогноза."
            sub={`p95: ${analytics.system.inferenceLatencyMs.p95.toFixed(2)} ms`}
          />
          <MetricCard
            title="Замеров скорости"
            value={formatInt(analytics.system.inferenceLatencyMs.samples)}
            hint="Сколько последних запусков участвует в расчете задержек."
            sub={`окно: ${formatInt(analytics.system.inferenceLatencyMs.windowSize)}`}
          />
        </div>

        <div className="mt-4 grid grid-cols-1 gap-4 lg:grid-cols-2">
          <div className={`${glassPanelClass} p-4`}>
            <p className="text-sm font-semibold text-white">Когда обновлялись данные</p>
            <div className="mt-2 space-y-1 text-xs text-slate-400">
              <p>Опорное время (anchor): {formatUtc(analytics.system.dataFreshness.anchorTimestampUtc)}</p>
              <p>Пересчет свежести данных: {formatUtc(analytics.system.dataFreshness.generatedAtUtc)}</p>
              <p>Пересчет backend: {formatUtc(analytics.generatedAtUtc)}</p>
            </div>
          </div>
          <div className={`${glassPanelClass} p-4`}>
            <p className="text-sm font-semibold text-white">Параметры текущего расчета</p>
            <div className="mt-2 space-y-1 text-xs text-slate-400">
              <p>Выбранная волна: {analytics.operations.selectedWave}</p>
              <p>Время расчета: {formatUtc(analytics.operations.selectedTimestamp)}</p>
              <p>Стратегия: {analytics.operations.strategy}</p>
              <p>Покрытие: офисов {formatInt(analytics.officeHorizonHeatmap.coverage.offices)} / строк {formatInt(analytics.officeHorizonHeatmap.coverage.rows)}</p>
            </div>
          </div>
        </div>
      </div>

    </div>
  )
}

export function AnalyticsContent() {
  return (
    <SimulationProvider>
      <SiteHeader title="Аналитика" />
      <div className="flex flex-1 flex-col">
        <div className="@container/main flex flex-1 flex-col gap-2">
          <AnalyticsGrid />
        </div>
      </div>
    </SimulationProvider>
  )
}

