export type MetricTone = "neutral" | "danger" | "success"
export type MetricBadgeIcon = "up" | "down" | "flat"
export type HorizonHours = 2 | 4 | 6 | 8 | 10 | 12 | 14 | 16 | 18 | 20
export type SimulationStrategy = "economy" | "balance" | "reliable"
export type WaveSelection = 1 | 2

export interface DashboardCardData {
  title: string
  value: string
  badge: string
  primaryHint: string
  secondaryHint: string
  tone?: MetricTone
  badgeIcon?: MetricBadgeIcon
}

export interface DashboardMetricsResponse {
  selectedTimestamp: string
  cards: {
    dailyPlan24h: DashboardCardData
    loadTrend: DashboardCardData
    operationalVolumeH1: DashboardCardData
    fleetRequirementH1: DashboardCardData
    accuracyH1: DashboardCardData
    operationalAnomalies: DashboardCardData
  }
  meta: {
    availableHorizons: number
    intervalMinutes: number
    selectedHorizonHours: HorizonHours
    isCumulative: boolean
    effectiveHorizonHours: number
  }
}

export interface SimulationSettings {
  cargoCapacity: number
  utilization: number
  strategy: SimulationStrategy
  reserveTrucks: number
  selectedWave: WaveSelection
}

export interface DispatchRegistryRow {
  id: string
  routeId: string
  officeFromId: string
  forecast: number
  baseline: number
  baselineSameSlot?: number
  baselineLag48?: number
  baseline_same_4w?: number
  baseline_same_7d?: number
  baseline_blend?: number
  baseline_rule_based?: number
  wape: number
  trustScore?: number
  trustReasonShort?: string
  trustReasonFull?: string
  trustHorizon?: number
  trustSource?: "stage3_artifact" | "fallback_wape"
  anomalyRatio: number
  baseTrucks: number
  insuranceBuffer: number
  totalTrucks: number
  dispatchModelTrucks?: number
  dispatchSameSlotTrucks?: number
  dispatchLag48Trucks?: number
  dispatchRuleTrucks?: number
  waveForecasts: number[]
  horizonForecasts?: number[]
  horizonBaselineSame4w?: number[]
  horizonBaselineBlend?: number[]
  horizonBaselineRule?: number[]
}

export interface DispatchRegistryResponse {
  rows: DispatchRegistryRow[]
  meta: {
    selectedTimestamp: string
    selectedWave: WaveSelection
    strategy: SimulationStrategy
    cargoCapacity: number
    utilization: number
    reserveTrucks: number
    modelWape?: number
    modelRelativeBias?: number
    competitionScore?: number
    modelMae?: number
    modelRmse?: number
    modelSmape?: number
    horizonTimelineLength?: number
    horizonStepMinutes?: number
  }
}
