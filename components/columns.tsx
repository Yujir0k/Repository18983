"use client"

import * as React from "react"
import { ArrowUpDown } from "lucide-react"
import type { Column, ColumnDef, SortingFn } from "@tanstack/react-table"

import { distributeTrucksSmart } from "@/lib/utils/sla-calculator"
import { confidenceFromWape } from "@/lib/utils/dispatch-status"
import type { DispatchRegistryRow } from "@/types/dashboard-metrics"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Input } from "@/components/ui/input"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"

export type DispatchStatus =
  | "corrected"
  | "critical"
  | "drop"
  | "review"
  | "ok"
  | "sent"

export type DispatchRegistryViewRow = DispatchRegistryRow & {
  status: DispatchStatus
  displayTrucks: number
  manualOverride?: number
}

export type ManualDispatchStatus = "ok" | "review" | "critical"

export const STATUS_FACET_OPTIONS: Array<{
  value: DispatchStatus
  label: string
}> = [
  { value: "critical", label: "Критическая нагрузка" },
  { value: "drop", label: "Аномальное снижение нагрузки" },
  { value: "review", label: "Требует проверки" },
  { value: "ok", label: "В норме" },
  { value: "corrected", label: "Скорректировано" },
  { value: "sent", label: "Отправлено" },
]

const STATUS_LABEL_MAP: Record<DispatchStatus, string> = {
  critical: "Критическая нагрузка",
  drop: "Аномальное снижение нагрузки",
  review: "Требует проверки",
  ok: "В норме",
  corrected: "Скорректировано",
  sent: "Отправлено",
}

const GLASS_MENU_CONTENT_CLASS =
  "border border-slate-800 bg-[rgba(22,27,29,0.5)] text-white shadow-2xl backdrop-blur-md"
const GLASS_MENU_ITEM_CLASS =
  "rounded-md text-slate-100 focus:bg-white/10 focus:text-white data-[state=checked]:bg-transparent"
const GLASS_FILTER_TRIGGER_CLASS =
  "inline-flex h-7 items-center justify-center rounded-full border border-transparent bg-transparent px-2.5 text-sm font-semibold text-white transition-colors hover:text-white focus:outline-none focus-visible:outline-none focus-visible:ring-0 data-[state=open]:border-white/15 data-[state=open]:bg-white/5"

function formatUnits(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Math.round(value))
}

function getRowConfidence(row: DispatchRegistryViewRow): number {
  if (typeof row.trustScore === "number" && Number.isFinite(row.trustScore)) {
    return Math.max(0, Math.min(100, Math.round(row.trustScore)))
  }
  return confidenceFromWape(row.wape, 75)
}

function formatConfidence(value: number): string {
  return `${Math.round(value)}%`
}

function FacetedHeader({
  column,
  title,
  options,
}: {
  column: Column<DispatchRegistryViewRow, unknown>
  title: string
  options: Array<{ value: string; label: string }>
}) {
  const selected = (column.getFilterValue() as string[] | undefined) ?? []

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button type="button" className={GLASS_FILTER_TRIGGER_CLASS}>
          {title}
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="start"
        className={`w-56 ${GLASS_MENU_CONTENT_CLASS}`}
      >
        {options.map((option) => {
          const isChecked = selected.includes(option.value)
          return (
            <DropdownMenuCheckboxItem
              key={option.value}
              className={GLASS_MENU_ITEM_CLASS}
              checked={isChecked}
              onCheckedChange={(checked) => {
                const next = checked
                  ? [...selected, option.value]
                  : selected.filter((item) => item !== option.value)
                column.setFilterValue(next.length ? next : undefined)
              }}
            >
              {option.label}
            </DropdownMenuCheckboxItem>
          )
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function FacetedSortableHeader({
  column,
  title,
  options,
}: {
  column: Column<DispatchRegistryViewRow, unknown>
  title: string
  options: Array<{ value: string; label: string }>
}) {
  const selected = (column.getFilterValue() as string[] | undefined) ?? []
  const sortState = column.getIsSorted()

  return (
    <div className="flex items-center justify-center gap-1.5">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <button type="button" className={GLASS_FILTER_TRIGGER_CLASS}>
            {title}
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent
          align="start"
          className={`w-56 ${GLASS_MENU_CONTENT_CLASS}`}
        >
          {options.map((option) => {
            const isChecked = selected.includes(option.value)
            return (
              <DropdownMenuCheckboxItem
                key={option.value}
                className={GLASS_MENU_ITEM_CLASS}
                checked={isChecked}
                onCheckedChange={(checked) => {
                  const next = checked
                    ? [...selected, option.value]
                    : selected.filter((item) => item !== option.value)
                  column.setFilterValue(next.length ? next : undefined)
                }}
              >
                {option.label}
              </DropdownMenuCheckboxItem>
            )
          })}
        </DropdownMenuContent>
      </DropdownMenu>

        <Button
          variant="ghost"
          size="icon-sm"
          className="size-6 text-slate-300 hover:bg-white/10 hover:text-white"
          onClick={() => column.toggleSorting(sortState === "asc")}
        >
        <ArrowUpDown className={sortState ? "size-4 text-white" : "size-4"} />
        <span className="sr-only">Сортировать {title}</span>
      </Button>
    </div>
  )
}

const numericIdSorting: SortingFn<DispatchRegistryViewRow> = (
  rowA,
  rowB,
  columnId
) => {
  const valueA = Number(rowA.getValue(columnId))
  const valueB = Number(rowB.getValue(columnId))

  const safeA = Number.isFinite(valueA) ? valueA : 0
  const safeB = Number.isFinite(valueB) ? valueB : 0

  if (safeA === safeB) {
    return 0
  }

  return safeA > safeB ? 1 : -1
}

function StatusBadge({ status }: { status: DispatchStatus }) {
  if (status === "corrected") {
    return (
      <Badge className="border-blue-400/40 bg-blue-500/20 text-blue-100">
        Скорректировано
      </Badge>
    )
  }

  if (status === "critical") {
    return (
      <Badge className="border-red-400/40 bg-red-500/20 text-red-100">
        Критическая нагрузка
      </Badge>
    )
  }

  if (status === "drop") {
    return (
      <Badge className="border-cyan-400/40 bg-cyan-500/20 text-cyan-100">
        Аномальное снижение нагрузки
      </Badge>
    )
  }

  if (status === "review") {
    return (
      <Badge className="border-yellow-400/40 bg-yellow-500/20 text-yellow-100">
        Требует проверки
      </Badge>
    )
  }

  if (status === "sent") {
    return (
      <Badge className="border-emerald-400/40 bg-emerald-500/20 text-emerald-100">
        Отправлено
      </Badge>
    )
  }

  return (
    <Badge className="border-emerald-400/40 bg-emerald-500/20 text-emerald-100">
      В норме
    </Badge>
  )
}

function EditableStatusCell({
  row,
  onUpdateStatus,
  onResetRow,
}: {
  row: DispatchRegistryViewRow
  onUpdateStatus: (rowId: string, status: ManualDispatchStatus) => void
  onResetRow: (rowId: string) => void
}) {
  const statusOptions: Array<{ value: ManualDispatchStatus; label: string }> = [
    { value: "ok", label: "🟢 В норме" },
    { value: "review", label: "🟡 Требует проверки" },
    { value: "critical", label: "🔴 Критическая нагрузка" },
  ]

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          className="inline-flex h-auto items-center justify-center p-0 hover:text-white focus:outline-none focus-visible:outline-none focus-visible:ring-0"
        >
          <StatusBadge status={row.status} />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="start"
        className={`w-60 ${GLASS_MENU_CONTENT_CLASS}`}
      >
        {statusOptions.map((option) => (
          <DropdownMenuItem
            key={option.value}
            className="text-slate-100 focus:bg-white/10 focus:text-white"
            onSelect={() => onUpdateStatus(row.id, option.value)}
          >
            {option.label}
          </DropdownMenuItem>
        ))}
        <DropdownMenuSeparator />
        <DropdownMenuItem
          className="text-slate-100 focus:bg-white/10 focus:text-white"
          onSelect={() => onResetRow(row.id)}
        >
          🔄 Сбросить к исходному
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function EditableTransportCell({
  row,
  onUpdateTransport,
}: {
  row: DispatchRegistryViewRow
  onUpdateTransport: (rowId: string, newValue: number) => void
}) {
  const [isEditing, setIsEditing] = React.useState(false)
  const [draftValue, setDraftValue] = React.useState(String(row.displayTrucks))
  const isManual = typeof row.manualOverride === "number"

  React.useEffect(() => {
    setDraftValue(String(row.displayTrucks))
  }, [row.displayTrucks])

  const commitValue = React.useCallback(() => {
    const parsedValue = Number(draftValue)
    if (!Number.isFinite(parsedValue) || parsedValue <= 0) {
      setDraftValue(String(row.displayTrucks))
      setIsEditing(false)
      return
    }
    onUpdateTransport(row.id, Math.ceil(parsedValue))
    setIsEditing(false)
  }, [draftValue, onUpdateTransport, row.displayTrucks, row.id])

  return (
    <div className="flex items-center justify-center gap-2">
      {isEditing ? (
        <Input
          type="number"
          className="h-8 w-20"
          value={draftValue}
          autoFocus
          onChange={(event) => setDraftValue(event.target.value)}
          onBlur={commitValue}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              commitValue()
            }
            if (event.key === "Escape") {
              setDraftValue(String(row.displayTrucks))
              setIsEditing(false)
            }
          }}
        />
      ) : (
        <Button
          variant="ghost"
          className="h-auto p-0 font-semibold text-white hover:bg-transparent hover:text-white"
          onClick={() => setIsEditing(true)}
        >
          {formatUnits(row.displayTrucks)} шт
        </Button>
      )}

      {!isManual && row.insuranceBuffer > 0 ? (
        <span className="text-xs text-slate-400">+{row.insuranceBuffer}</span>
      ) : null}
    </div>
  )
}

function getSlaSlots(row: DispatchRegistryViewRow): readonly [number, number, number, number] {
  const totalTrucks = Math.max(0, Math.round(row.displayTrucks))
  const waveForecasts = row.waveForecasts
  const r = distributeTrucksSmart(totalTrucks, waveForecasts)
  return [r[0] ?? 0, r[1] ?? 0, r[2] ?? 0, r[3] ?? 0]
}

export function getStatusLabel(status: DispatchStatus): string {
  return STATUS_LABEL_MAP[status]
}

export function createRegistryColumns(options: {
  onUpdateTransport: (rowId: string, newValue: number) => void
  onUpdateStatus: (rowId: string, status: ManualDispatchStatus) => void
  onResetRow: (rowId: string) => void
}): ColumnDef<DispatchRegistryViewRow>[] {
  return [
    {
      id: "select",
      header: ({ table }) => (
        <Checkbox
          checked={
            table.getIsAllPageRowsSelected() ||
            (table.getIsSomePageRowsSelected() && "indeterminate")
          }
          onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
          aria-label="Выбрать все строки"
        />
      ),
      cell: ({ row }) => (
        <Checkbox
          checked={row.getIsSelected()}
          onCheckedChange={(value) => row.toggleSelected(!!value)}
          aria-label="Выбрать строку"
        />
      ),
      enableSorting: false,
      enableHiding: false,
    },
    {
      accessorKey: "routeId",
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => String(value))
          .sort((a, b) => Number(a) - Number(b))
          .map((value) => ({ value, label: `ID: ${value}` }))

        return (
          <FacetedSortableHeader
            column={column}
            title="Маршрут"
            options={options}
          />
        )
      },
      cell: ({ row }) => (
        <div className="text-center font-medium text-white">ID: {row.original.routeId}</div>
      ),
      sortingFn: numericIdSorting,
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
      enableHiding: false,
    },
    {
      accessorKey: "officeFromId",
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => String(value))
          .sort((a, b) => Number(a) - Number(b))
          .map((value) => ({ value, label: `Склад ${value}` }))

        return (
          <FacetedSortableHeader column={column} title="Склад" options={options} />
        )
      },
      cell: ({ row }) => (
        <div className="text-center text-slate-200">Склад {row.original.officeFromId}</div>
      ),
      sortingFn: numericIdSorting,
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      accessorKey: "forecast",
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value))
          .sort((a, b) => a - b)
          .map((value) => ({ value: String(value), label: `${formatUnits(value)} ед.` }))
        return (
          <FacetedSortableHeader
            column={column}
            title="Прогноз"
            options={options}
          />
        )
      },
      cell: ({ row }) => (
        <div className="text-center font-medium text-slate-100">
          {formatUnits(row.original.forecast)} ед.
        </div>
      ),
      sortingFn: "basic",
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      id: "confidence",
      accessorFn: (row) => getRowConfidence(row),
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value))
          .sort((a, b) => a - b)
          .map((value) => ({ value: String(value), label: `${value}%` }))
        return (
          <FacetedSortableHeader
            column={column}
            title="Уверенность"
            options={options}
          />
        )
      },
      cell: ({ row }) => (
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="text-center text-slate-200 cursor-help">
                {formatConfidence(getRowConfidence(row.original))}
              </div>
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-xs text-xs leading-relaxed">
              {row.original.trustReasonShort
                ? `Trust score: ${row.original.trustReasonShort}`
                : "Историческая уверенность модели на данном маршруте (fallback из out-of-fold WAPE за последние 7 дней)."}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
      ),
      sortingFn: "basic",
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      accessorKey: "displayTrucks",
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value))
          .sort((a, b) => a - b)
          .map((value) => ({ value: String(value), label: `${value} шт` }))
        return (
          <FacetedSortableHeader
            column={column}
            title="Транспорт"
            options={options}
          />
        )
      },
      cell: ({ row }) => (
        <EditableTransportCell
          row={row.original}
          onUpdateTransport={options.onUpdateTransport}
        />
      ),
      sortingFn: "basic",
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      id: "sla30",
      accessorFn: (row) => getSlaSlots(row)[0],
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value))
          .sort((a, b) => a - b)
          .map((value) => ({ value: String(value), label: String(value) }))
        return (
          <FacetedSortableHeader
            column={column}
            title="30 мин"
            options={options}
          />
        )
      },
      cell: ({ getValue }) => (
        <div className="text-center font-semibold tabular-nums text-slate-100">
          {Number(getValue<number>())}
        </div>
      ),
      sortingFn: "basic",
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      id: "sla60",
      accessorFn: (row) => getSlaSlots(row)[1],
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value))
          .sort((a, b) => a - b)
          .map((value) => ({ value: String(value), label: String(value) }))
        return (
          <FacetedSortableHeader
            column={column}
            title="1 час"
            options={options}
          />
        )
      },
      cell: ({ getValue }) => (
        <div className="text-center font-semibold tabular-nums text-slate-100">
          {Number(getValue<number>())}
        </div>
      ),
      sortingFn: "basic",
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      id: "sla90",
      accessorFn: (row) => getSlaSlots(row)[2],
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value))
          .sort((a, b) => a - b)
          .map((value) => ({ value: String(value), label: String(value) }))
        return (
          <FacetedSortableHeader
            column={column}
            title="1.5 часа"
            options={options}
          />
        )
      },
      cell: ({ getValue }) => (
        <div className="text-center font-semibold tabular-nums text-slate-100">
          {Number(getValue<number>())}
        </div>
      ),
      sortingFn: "basic",
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      id: "sla120",
      accessorFn: (row) => getSlaSlots(row)[3],
      header: ({ column }) => {
        const options = Array.from(column.getFacetedUniqueValues().keys())
          .map((value) => Number(value))
          .filter((value) => Number.isFinite(value))
          .sort((a, b) => a - b)
          .map((value) => ({ value: String(value), label: String(value) }))
        return (
          <FacetedSortableHeader
            column={column}
            title="2 часа"
            options={options}
          />
        )
      },
      cell: ({ getValue }) => (
        <div className="text-center font-semibold tabular-nums text-slate-100">
          {Number(getValue<number>())}
        </div>
      ),
      sortingFn: "basic",
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
    {
      accessorKey: "status",
      header: ({ column }) => (
        <FacetedHeader
          column={column}
          title="Статус"
          options={STATUS_FACET_OPTIONS}
        />
      ),
      cell: ({ row }) => (
        <div className="flex justify-center">
          <EditableStatusCell
            row={row.original}
            onUpdateStatus={options.onUpdateStatus}
            onResetRow={options.onResetRow}
          />
        </div>
      ),
      filterFn: (row, id, value) => {
        if (!Array.isArray(value) || value.length === 0) {
          return true
        }
        return value.includes(String(row.getValue(id)))
      },
    },
  ]
}
