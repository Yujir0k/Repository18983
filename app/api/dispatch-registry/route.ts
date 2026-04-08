import { NextResponse } from "next/server"

import { dashboardConfig } from "@/lib/dashboard-config"
import { calculateDispatchRegistry } from "@/lib/metrics-calculator"
import type { SimulationStrategy, WaveSelection } from "@/types/dashboard-metrics"

const WAVE_OPTIONS: WaveSelection[] = [1, 2]
const STRATEGY_OPTIONS: SimulationStrategy[] = ["economy", "balance", "reliable"]

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)

    const selectedTimestamp =
      searchParams.get("timestamp") ?? dashboardConfig.CURRENT_TIME
    const waveRaw = Number(searchParams.get("wave") ?? 1)
    const cargoCapacityRaw = Number(
      searchParams.get("cargoCapacity") ?? dashboardConfig.CARGO_CAPACITY
    )
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
      ? Math.min(1, Math.max(0.5, utilizationRaw))
      : 0.85
    const reserveTrucks = Number.isFinite(reserveTrucksRaw)
      ? Math.max(0, Math.floor(reserveTrucksRaw))
      : 0

    const registry = calculateDispatchRegistry({
      selectedTimestampIso: selectedTimestamp,
      selectedWave,
      cargoCapacity,
      utilization,
      reserveTrucks,
      strategy,
    })

    return NextResponse.json(registry, {
      headers: {
        "Cache-Control": "no-store",
      },
    })
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : "Не удалось сформировать реестр диспетчеризации."
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
