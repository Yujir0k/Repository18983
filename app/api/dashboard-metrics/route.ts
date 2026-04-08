import { NextResponse } from "next/server"

import { dashboardConfig } from "@/lib/dashboard-config"
import { calculateDashboardMetrics } from "@/lib/metrics-calculator"
import type { HorizonHours } from "@/types/dashboard-metrics"

const HORIZON_OPTIONS: HorizonHours[] = [...dashboardConfig.HORIZON_OPTIONS_HOURS]

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)
    const selectedTimestamp =
      searchParams.get("timestamp") ?? dashboardConfig.CURRENT_TIME
    const horizonRaw = Number(
      searchParams.get("horizonHours") ?? dashboardConfig.DEFAULT_SELECTED_HORIZON_HOURS
    )
    const isCumulative = searchParams.get("cumulative") === "true"

    const selectedHorizonHours = (
      HORIZON_OPTIONS.includes(horizonRaw as HorizonHours)
        ? (horizonRaw as HorizonHours)
        : dashboardConfig.DEFAULT_SELECTED_HORIZON_HOURS
    ) as HorizonHours

    const metrics = calculateDashboardMetrics({
      selectedTimestampIso: selectedTimestamp,
      selectedHorizonHours,
      isCumulative,
    })
    return NextResponse.json(metrics, {
      headers: {
        "Cache-Control": "no-store",
      },
    })
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Не удалось рассчитать KPI-карточки."

    return NextResponse.json(
      { error: message },
      {
        status: 500,
      }
    )
  }
}
