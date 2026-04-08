"use client"

import * as React from "react"

import { dashboardConfig } from "@/lib/dashboard-config"
import { SimulationProvider } from "@/lib/contexts/SimulationContext"
import { DashboardKpiCards } from "@/components/dashboard-kpi-cards"
import { DataTable } from "@/components/data-table"
import { SiteHeader } from "@/components/site-header"
import type { DispatchRegistryViewRow } from "@/components/columns"

type DashboardContentProps = {
  data: Array<{
    id: number
    header: string
    type: string
    status: string
    target: string
    limit: string
    reviewer: string
  }>
}

export function DashboardContent({ data }: DashboardContentProps) {
  const [selectedTimestamp] = React.useState(dashboardConfig.CURRENT_TIME)
  const [tableData, setTableData] = React.useState<DispatchRegistryViewRow[]>([])

  return (
    <SimulationProvider>
      <SiteHeader title="Центр управления" />
      <div className="flex flex-1 flex-col">
        <div className="@container/main flex flex-1 flex-col gap-2">
          <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
            <DashboardKpiCards data={tableData} />
            <DataTable
              data={data}
              selectedTimestamp={selectedTimestamp}
              onRowsChange={setTableData}
            />
          </div>
        </div>
      </div>
    </SimulationProvider>
  )
}
