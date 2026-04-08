"use client"

import * as React from "react"
import { AlertTriangle, CheckCircle, Truck } from "lucide-react"

import type { DispatchRegistryViewRow } from "@/components/columns"

type DashboardKpiCardsProps = {
  data: DispatchRegistryViewRow[]
}

function formatUnits(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Math.round(value))
}

export function DashboardKpiCards({ data }: DashboardKpiCardsProps) {
  const cardClassName =
    "rounded-xl bg-[rgba(22,27,29,0.38)] p-5 shadow-2xl backdrop-blur-md transition-all duration-300 ease-in-out will-change-transform hover:-translate-y-[4px] hover:shadow-[0_14px_34px_rgba(0,0,0,0.34)]"

  const metrics = React.useMemo(() => {
    const totalRequests = data.reduce(
      (acc, row) => acc + (row.displayTrucks ?? row.totalTrucks),
      0
    )
    const problematicCount = data.filter(
      (row) =>
        row.status === "critical" ||
        row.status === "review" ||
        row.status === "drop"
    ).length
    const totalObjects = data.length
    const sentCount = data.filter((row) => row.status === "sent").length
    const pendingCount = Math.max(0, totalObjects - sentCount)

    return {
      totalRequests,
      problematicCount,
      totalObjects,
      sentCount,
      pendingCount,
    }
  }, [data])

  return (
    <div className="grid grid-cols-1 gap-4 px-4 md:grid-cols-3 lg:px-6">
      <div className={cardClassName}>
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-sm font-medium text-slate-200">Потребность в транспорте (в ед.)</h3>
          <Truck className="size-5 text-[#61A0B7]" />
        </div>
        <div className="text-4xl font-bold text-white">
          {formatUnits(metrics.totalRequests)} шт.
        </div>
        <p className="mt-3 text-sm text-slate-300">Суммарно по выбранной волне</p>
      </div>

      <div className={cardClassName}>
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-sm font-medium text-slate-200">Проблемные объекты</h3>
          <AlertTriangle className="size-5 text-destructive" />
        </div>
        <div className="text-4xl font-bold text-white">
          {formatUnits(metrics.problematicCount)} из {formatUnits(metrics.totalObjects)}
        </div>
        <p className="mt-3 text-sm text-slate-300">Требуют внимания диспетчера</p>
      </div>

      <div className={cardClassName}>
        <div className="mb-3 flex items-center justify-between">
          <h3 className="text-sm font-medium text-slate-200">Статус диспетчеризации</h3>
          <CheckCircle className="size-5 text-emerald-500" />
        </div>
        <div className="text-3xl font-bold text-white">
          Отправлено: {metrics.sentCount} / Ожидают: {formatUnits(metrics.pendingCount)}
        </div>
        <p className="mt-3 text-sm text-slate-300">Заявки, переданные в ТК</p>
      </div>
    </div>
  )
}
