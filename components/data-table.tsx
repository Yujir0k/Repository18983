"use client"

import * as React from "react"
import {
  flexRender,
  getCoreRowModel,
  getFacetedRowModel,
  getFacetedUniqueValues,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnFiltersState,
  type RowSelectionState,
  type SortingState,
  type VisibilityState,
} from "@tanstack/react-table"
import {
  IconChevronDown,
  IconChevronLeft,
  IconChevronRight,
  IconLayoutColumns,
  IconRefresh,
  IconSettings,
} from "@tabler/icons-react"
import { toast } from "sonner"

import { useSimulation } from "@/lib/contexts/SimulationContext"
import {
  confidenceFromWape,
  getVolumeDeltaStatus,
} from "@/lib/utils/dispatch-status"
import { distributeTrucksSmart } from "@/lib/utils/sla-calculator"
import type {
  DispatchRegistryResponse,
  SimulationStrategy,
  WaveSelection,
} from "@/types/dashboard-metrics"
import {
  createRegistryColumns,
  getStatusLabel,
  type DispatchRegistryViewRow,
  type DispatchStatus,
  type ManualDispatchStatus,
} from "@/components/columns"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"

type DataTableProps = {
  data: Array<{
    id: number
    header: string
    type: string
    status: string
    target: string
    limit: string
    reviewer: string
  }>
  selectedTimestamp: string
  onRowsChange?: (rows: DispatchRegistryViewRow[]) => void
}

type StatusFilter =
  | "all"
  | "critical"
  | "drop"
  | "warning"
  | "normal"
  | "overridden"
  | "sent"
type WaveRowMap<T> = Record<WaveSelection, Record<string, T>>

const PAGE_SIZE_OPTIONS = [10, 20, 30, 40, 50, 100, 250, 500, 1000, 2000] as const
const STORAGE_KEY = "rwb-flow-dashboard-state-v1"

const STATUS_TABS: Array<{ key: StatusFilter; label: string }> = [
  { key: "all", label: "Все объекты" },
  { key: "critical", label: "Критические отклонения" },
  { key: "drop", label: "Аномальное снижение" },
  { key: "warning", label: "На ручную проверку" },
  { key: "normal", label: "Штатный режим" },
  { key: "overridden", label: "Скорректированные" },
  { key: "sent", label: "Отправленные" },
]

const COLUMN_LABELS: Record<string, string> = {
  routeId: "Маршрут",
  officeFromId: "Склад",
  forecast: "Прогноз",
  confidence: "Уверенность",
  displayTrucks: "Транспорт",
  sla30: "Через 30 мин (ед.)",
  sla60: "Через час (ед.)",
  sla90: "Через 1.5 часа (ед.)",
  sla120: "Через 2 часа (ед.)",
  status: "Статус",
}

const strategyLabel = {
  economy: "Экономия",
  balance: "Баланс",
  reliable: "Надежность",
} satisfies Record<SimulationStrategy, string>

function createEmptyWaveMap<T>(): WaveRowMap<T> {
  return {
    1: {},
    2: {},
  }
}

function readWaveMapFromStorage<T>(
  source: unknown,
  validate: (value: unknown) => value is T
): WaveRowMap<T> {
  const result = createEmptyWaveMap<T>()
  if (!source || typeof source !== "object") {
    return result
  }

  for (const wave of [1, 2] as const) {
    const waveValue = (source as Record<string, unknown>)[String(wave)]
    if (!waveValue || typeof waveValue !== "object") {
      continue
    }

    for (const [rowId, rawValue] of Object.entries(
      waveValue as Record<string, unknown>
    )) {
      if (validate(rawValue)) {
        result[wave][rowId] = rawValue
      }
    }
  }

  return result
}

function formatUnits(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Math.round(value))
}

function toCsvCell(value: string | number): string {
  const text = String(value)
  if (/[",;\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`
  }
  return text
}

function useDebouncedValue<T>(value: T, delayMs: number): T {
  const [debouncedValue, setDebouncedValue] = React.useState(value)

  React.useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      setDebouncedValue(value)
    }, delayMs)

    return () => window.clearTimeout(timeoutId)
  }, [value, delayMs])

  return debouncedValue
}

function getStatus(
  row: DispatchRegistryViewRow,
  hasManualOverride: boolean,
  isDispatched: boolean
): DispatchStatus {
  if (isDispatched) {
    return "sent"
  }
  if (hasManualOverride) {
    return "corrected"
  }

  const statusBaseline =
    (typeof row.baseline_blend === "number" ? row.baseline_blend : undefined) ??
    (typeof row.baseline_same_4w === "number" ? row.baseline_same_4w : undefined) ??
    (typeof row.baselineSameSlot === "number" ? row.baselineSameSlot : undefined) ??
    (typeof row.baseline === "number" ? row.baseline / 4 : 0)

  const confidence =
    typeof row.trustScore === "number" && Number.isFinite(row.trustScore)
      ? Math.max(0, Math.min(100, Math.round(row.trustScore)))
      : confidenceFromWape(row.wape, 75)

  return getVolumeDeltaStatus({
    forecast: row.forecast,
    baseline: statusBaseline,
    confidence,
  }).status
}

export function DataTable({
  data: _initialData,
  selectedTimestamp,
  onRowsChange,
}: DataTableProps) {
  void _initialData

  const {
    cargoCapacity,
    utilization,
    strategy,
    reserveTrucks,
    selectedWave,
    isHydrated: isSimulationHydrated,
    refreshNonce,
    setCargoCapacity,
    setUtilization,
    setStrategy,
    setReserveTrucks,
  } = useSimulation()

  const debouncedCargoCapacity = useDebouncedValue(cargoCapacity, 120)
  const debouncedUtilization = useDebouncedValue(utilization, 120)
  const debouncedReserveTrucks = useDebouncedValue(reserveTrucks, 120)
  const debouncedStrategy = useDebouncedValue(strategy, 90)

  const [rows, setRows] = React.useState<DispatchRegistryResponse["rows"]>([])
  const [isLoading, setIsLoading] = React.useState(true)
  const [error, setError] = React.useState<string | null>(null)
  const [statusFilter, setStatusFilter] = React.useState<StatusFilter>("all")
  const [columnVisibility, setColumnVisibility] = React.useState<VisibilityState>({})
  const [columnFilters, setColumnFilters] = React.useState<ColumnFiltersState>([])
  const [sorting, setSorting] = React.useState<SortingState>([
    { id: "routeId", desc: false },
  ])
  const [pagination, setPagination] = React.useState({ pageIndex: 0, pageSize: 10 })
  const [rowSelection, setRowSelection] = React.useState<RowSelectionState>({})
  const [manualOverridesByWave, setManualOverridesByWave] =
    React.useState<WaveRowMap<number>>(createEmptyWaveMap)
  const [manualStatusesByWave, setManualStatusesByWave] =
    React.useState<WaveRowMap<ManualDispatchStatus>>(createEmptyWaveMap)
  const [dispatchedByWave, setDispatchedByWave] =
    React.useState<WaveRowMap<boolean>>(createEmptyWaveMap)
  const [isStorageHydrated, setIsStorageHydrated] = React.useState(false)
  const hasLoadedOnceRef = React.useRef(false)

  React.useEffect(() => {
    const isManualStatus = (value: unknown): value is ManualDispatchStatus =>
      value === "ok" || value === "review" || value === "critical"
    const isSafeNumber = (value: unknown): value is number =>
      typeof value === "number" && Number.isFinite(value) && value >= 0
    const isBoolean = (value: unknown): value is boolean =>
      typeof value === "boolean"

    try {
      const savedState = window.localStorage.getItem(STORAGE_KEY)
      if (!savedState) {
        setIsStorageHydrated(true)
        return
      }

      const parsed = JSON.parse(savedState) as {
        manualOverridesByWave?: unknown
        manualStatusesByWave?: unknown
        dispatchedByWave?: unknown
      }

      setManualOverridesByWave(
        readWaveMapFromStorage(parsed.manualOverridesByWave, isSafeNumber)
      )
      setManualStatusesByWave(
        readWaveMapFromStorage(parsed.manualStatusesByWave, isManualStatus)
      )
      setDispatchedByWave(
        readWaveMapFromStorage(parsed.dispatchedByWave, isBoolean)
      )
    } catch {
      setManualOverridesByWave(createEmptyWaveMap<number>())
      setManualStatusesByWave(createEmptyWaveMap<ManualDispatchStatus>())
      setDispatchedByWave(createEmptyWaveMap<boolean>())
    } finally {
      setIsStorageHydrated(true)
    }
  }, [])

  React.useEffect(() => {
    if (!isStorageHydrated) {
      return
    }
    const payload = {
      manualOverridesByWave,
      manualStatusesByWave,
      dispatchedByWave,
    }
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload))
  }, [
    isStorageHydrated,
    manualOverridesByWave,
    manualStatusesByWave,
    dispatchedByWave,
  ])

  React.useEffect(() => {
    const handleDemoDataReplaced = () => {
      setManualOverridesByWave(createEmptyWaveMap<number>())
      setManualStatusesByWave(createEmptyWaveMap<ManualDispatchStatus>())
      setDispatchedByWave(createEmptyWaveMap<boolean>())
      setRowSelection({})
      try {
        window.localStorage.removeItem(STORAGE_KEY)
      } catch {
        // Ignore storage errors and continue with in-memory reset.
      }
    }

    window.addEventListener(
      "rwb-demo-data-replaced",
      handleDemoDataReplaced as EventListener
    )
    return () => {
      window.removeEventListener(
        "rwb-demo-data-replaced",
        handleDemoDataReplaced as EventListener
      )
    }
  }, [])

  React.useEffect(() => {
    if (!isSimulationHydrated) {
      return
    }

    const abortController = new AbortController()

    const fetchRows = async () => {
      if (!hasLoadedOnceRef.current) {
        setIsLoading(true)
      }
      setError(null)
      let didSucceed = false
      try {
        const query = new URLSearchParams({
          timestamp: selectedTimestamp,
          wave: String(selectedWave),
          cargoCapacity: String(debouncedCargoCapacity),
          utilization: String(debouncedUtilization),
          reserveTrucks: String(debouncedReserveTrucks),
          strategy: debouncedStrategy,
        })

        const response = await fetch(`/api/dispatch-registry?${query.toString()}`, {
          method: "GET",
          cache: "no-store",
          signal: abortController.signal,
        })

        if (!response.ok) {
          throw new Error("Не удалось загрузить реестр диспетчеризации.")
        }

        const payload = (await response.json()) as DispatchRegistryResponse
        setRows(payload.rows)
        didSucceed = true
      } catch (fetchError) {
        if (
          !(fetchError instanceof DOMException && fetchError.name === "AbortError")
        ) {
          const message =
            fetchError instanceof Error
              ? fetchError.message
              : "Не удалось загрузить реестр диспетчеризации."
          setError(message)
        }
      } finally {
        if (!abortController.signal.aborted) {
          if (!hasLoadedOnceRef.current) {
            setIsLoading(false)
            if (didSucceed) {
              hasLoadedOnceRef.current = true
            }
          }
        }
      }
    }

    void fetchRows()
    return () => abortController.abort()
  }, [
    selectedTimestamp,
    selectedWave,
    debouncedCargoCapacity,
    debouncedUtilization,
    debouncedReserveTrucks,
    debouncedStrategy,
    isSimulationHydrated,
    refreshNonce,
  ])

  React.useEffect(() => {
    setRowSelection({})
  }, [selectedWave, statusFilter])

  const activeManualOverrides = manualOverridesByWave[selectedWave]
  const activeManualStatuses = manualStatusesByWave[selectedWave]
  const activeDispatched = dispatchedByWave[selectedWave]

  const viewRows = React.useMemo<DispatchRegistryViewRow[]>(() => {
    return rows.map((row) => {
      const manualOverride = activeManualOverrides[row.id]
      const hasManualOverride = typeof manualOverride === "number"
      const isDispatched = Boolean(activeDispatched[row.id])
      const manualStatus = activeManualStatuses[row.id]
      const requestedTrucks = hasManualOverride ? manualOverride : row.totalTrucks

      const calculatedStatus = getStatus(
        row as DispatchRegistryViewRow,
        hasManualOverride,
        isDispatched
      )
      const status = isDispatched ? "sent" : (manualStatus ?? calculatedStatus)

      return {
        ...row,
        status,
        manualOverride,
        displayTrucks: requestedTrucks,
      }
    })
  }, [
    rows,
    activeManualOverrides,
    activeManualStatuses,
    activeDispatched,
  ])

  React.useEffect(() => {
    if (isLoading) {
      return
    }
    onRowsChange?.(viewRows)
  }, [isLoading, onRowsChange, viewRows])

  const filteredRows = React.useMemo(() => {
    if (statusFilter === "all") {
      return viewRows
    }
    if (statusFilter === "critical") {
      return viewRows.filter((row) => row.status === "critical")
    }
    if (statusFilter === "drop") {
      return viewRows.filter((row) => row.status === "drop")
    }
    if (statusFilter === "warning") {
      return viewRows.filter((row) => row.status === "review")
    }
    if (statusFilter === "overridden") {
      return viewRows.filter(
        (row) =>
          typeof activeManualOverrides[row.id] === "number" ||
          typeof activeManualStatuses[row.id] === "string"
      )
    }
    if (statusFilter === "sent") {
      return viewRows.filter((row) => row.status === "sent")
    }
    return viewRows.filter((row) => row.status === "ok")
  }, [statusFilter, viewRows, activeManualOverrides, activeManualStatuses])

  const updateTransport = React.useCallback(
    (rowId: string, newValue: number) => {
      setManualOverridesByWave((previous) => ({
        ...previous,
        [selectedWave]: {
          ...previous[selectedWave],
          [rowId]: Math.ceil(newValue),
        },
      }))

      setDispatchedByWave((previous) => {
        if (!previous[selectedWave][rowId]) {
          return previous
        }
        const nextWaveMap = { ...previous[selectedWave] }
        delete nextWaveMap[rowId]
        return {
          ...previous,
          [selectedWave]: nextWaveMap,
        }
      })
    },
    [selectedWave]
  )

  const updateStatus = React.useCallback(
    (rowId: string, status: ManualDispatchStatus) => {
      setManualStatusesByWave((previous) => ({
        ...previous,
        [selectedWave]: {
          ...previous[selectedWave],
          [rowId]: status,
        },
      }))

      setDispatchedByWave((previous) => {
        if (!previous[selectedWave][rowId]) {
          return previous
        }
        const nextWaveMap = { ...previous[selectedWave] }
        delete nextWaveMap[rowId]
        return {
          ...previous,
          [selectedWave]: nextWaveMap,
        }
      })
    },
    [selectedWave]
  )

  const resetRowToOriginal = React.useCallback(
    (rowId: string) => {
      setManualOverridesByWave((previous) => {
        if (!previous[selectedWave][rowId]) {
          return previous
        }
        const nextWaveMap = { ...previous[selectedWave] }
        delete nextWaveMap[rowId]
        return {
          ...previous,
          [selectedWave]: nextWaveMap,
        }
      })

      setManualStatusesByWave((previous) => {
        if (!previous[selectedWave][rowId]) {
          return previous
        }
        const nextWaveMap = { ...previous[selectedWave] }
        delete nextWaveMap[rowId]
        return {
          ...previous,
          [selectedWave]: nextWaveMap,
        }
      })

      setDispatchedByWave((previous) => {
        if (!previous[selectedWave][rowId]) {
          return previous
        }
        const nextWaveMap = { ...previous[selectedWave] }
        delete nextWaveMap[rowId]
        return {
          ...previous,
          [selectedWave]: nextWaveMap,
        }
      })
    },
    [selectedWave]
  )

  const columns = React.useMemo(
    () =>
      createRegistryColumns({
        onUpdateTransport: updateTransport,
        onUpdateStatus: updateStatus,
        onResetRow: resetRowToOriginal,
      }),
    [resetRowToOriginal, updateStatus, updateTransport]
  )

  const table = useReactTable({
    data: filteredRows,
    columns,
    state: {
      sorting,
      rowSelection,
      columnVisibility,
      columnFilters,
      pagination,
    },
    initialState: {
      sorting: [{ id: "routeId", desc: false }],
    },
    getRowId: (row) => row.id,
    enableRowSelection: true,
    onSortingChange: setSorting,
    onRowSelectionChange: setRowSelection,
    onColumnVisibilityChange: setColumnVisibility,
    onColumnFiltersChange: setColumnFilters,
    onPaginationChange: setPagination,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFacetedRowModel: getFacetedRowModel(),
    getFacetedUniqueValues: getFacetedUniqueValues(),
  })

  const selectedRowsCount = table.getFilteredSelectedRowModel().rows.length
  const visibleColumnsCount = table.getVisibleLeafColumns().length

  const dispatchSelected = React.useCallback(() => {
    const selectedRows = table.getFilteredSelectedRowModel().rows
    if (!selectedRows.length) {
      return
    }

    const selectedIds = selectedRows.map((row) => row.original.id)

    setDispatchedByWave((previous) => {
      const nextWaveMap = { ...previous[selectedWave] }
      for (const rowId of selectedIds) {
        nextWaveMap[rowId] = true
      }
      return {
        ...previous,
        [selectedWave]: nextWaveMap,
      }
    })

    setRowSelection({})
    table.toggleAllRowsSelected(false)
    toast("Успешно", { description: "Заявки переданы перевозчикам" })
  }, [selectedWave, table])

  const resetSelectedStatusesToDefault = React.useCallback(() => {
    const selectedRows = table.getFilteredSelectedRowModel().rows
    if (!selectedRows.length) {
      toast("Нет выбранных строк", { description: "Отметьте строки чекбоксами." })
      return
    }

    const selectedIds = selectedRows.map((row) => row.original.id)
    const selectedSet = new Set(selectedIds)

    setManualStatusesByWave((previous) => {
      const nextWaveMap = { ...previous[selectedWave] }
      let hasChanges = false

      for (const rowId of selectedSet) {
        if (nextWaveMap[rowId]) {
          delete nextWaveMap[rowId]
          hasChanges = true
        }
      }

      if (!hasChanges) {
        return previous
      }

      return {
        ...previous,
        [selectedWave]: nextWaveMap,
      }
    })

    setDispatchedByWave((previous) => {
      const nextWaveMap = { ...previous[selectedWave] }
      let hasChanges = false

      for (const rowId of selectedSet) {
        if (nextWaveMap[rowId]) {
          delete nextWaveMap[rowId]
          hasChanges = true
        }
      }

      if (!hasChanges) {
        return previous
      }

      return {
        ...previous,
        [selectedWave]: nextWaveMap,
      }
    })

    setRowSelection({})
    table.toggleAllRowsSelected(false)
    toast("Статусы восстановлены", {
      description: `Сброшено для ${selectedIds.length} выбранных строк`,
    })
  }, [selectedWave, table])

  const exportSelectedCsv = React.useCallback(() => {
    const selectedRows = table.getFilteredSelectedRowModel().rows
    if (!selectedRows.length) {
      toast("Нет строк для выгрузки", { description: "Выберите хотя бы одну строку." })
      return
    }

    const header = [
      "Маршрут",
      "Склад",
      "Прогноз",
      "Транспорт",
      "Статус",
      "Через 30 мин (ед.)",
      "Через час (ед.)",
      "Через 1.5 часа (ед.)",
      "Через 2 часа (ед.)",
    ]

    const body = selectedRows.map((tableRow) => {
      const row = tableRow.original
      const sla = distributeTrucksSmart(
        Math.max(0, Math.round(row.displayTrucks)),
        row.waveForecasts
      )

      return [
        row.routeId,
        row.officeFromId,
        formatUnits(row.forecast),
        row.displayTrucks,
        getStatusLabel(row.status),
        sla[0] ?? 0,
        sla[1] ?? 0,
        sla[2] ?? 0,
        sla[3] ?? 0,
      ]
    })

    const csvContent = [header, ...body]
      .map((line) => line.map((cell) => toCsvCell(cell)).join(","))
      .join("\n")

    const blob = new Blob(["\ufeff" + csvContent], {
      type: "text/csv;charset=utf-8;",
    })
    const url = URL.createObjectURL(blob)
    const link = document.createElement("a")
    link.href = url
    link.download = "dispatch_registry.csv"
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
  }, [table])

  return (
    <>
      <div className="w-full px-4 lg:px-6">
        <div className="rounded-xl bg-[rgba(22,27,29,0.32)] p-4 shadow-2xl backdrop-blur-md">
          <div className="mb-4 flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <Tabs
              value={statusFilter}
              onValueChange={(value) => setStatusFilter(value as StatusFilter)}
              className="w-full xl:w-auto"
            >
              <TabsList className="h-auto w-full flex-wrap justify-start rounded-xl bg-white/5 p-1 xl:w-auto">
                {STATUS_TABS.map((tab) => (
                  <TabsTrigger key={tab.key} value={tab.key}>
                    {tab.label}
                  </TabsTrigger>
                ))}
              </TabsList>
            </Tabs>

            <div className="flex flex-wrap items-center justify-end gap-2 xl:flex-nowrap">
              <div className="hidden items-center gap-2 xl:flex">
                <Label htmlFor="rows-per-page" className="text-sm text-slate-300">
                  Строк на странице
                </Label>
                <Select
                  value={`${table.getState().pagination.pageSize}`}
                  onValueChange={(value) => table.setPageSize(Number(value))}
                >
                  <SelectTrigger
                    id="rows-per-page"
                    size="sm"
                    className="w-20 border border-slate-800 bg-[rgba(22,27,29,0.5)] text-white shadow-2xl backdrop-blur-md hover:bg-white/10"
                  >
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent
                    position="popper"
                    align="end"
                    className="w-20 min-w-20 border border-slate-800 bg-[rgba(22,27,29,0.5)] text-white shadow-2xl backdrop-blur-md"
                  >
                    {PAGE_SIZE_OPTIONS.map((size) => (
                      <SelectItem
                        key={size}
                        value={`${size}`}
                        className="rounded-md text-slate-100 focus:bg-white/10 focus:text-white data-[state=checked]:bg-transparent"
                      >
                        {size}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="outline"
                    size="sm"
                    className="border-white/20 bg-white/5 text-white hover:bg-white/10"
                  >
                    <IconLayoutColumns />
                    <span className="hidden lg:inline">Настроить колонки</span>
                    <span className="lg:hidden">Колонки</span>
                    <IconChevronDown />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent
                  align="end"
                  className="w-56 border border-slate-800 bg-[rgba(22,27,29,0.5)] text-white shadow-2xl backdrop-blur-md"
                >
                  {table
                    .getAllColumns()
                    .filter((column) => column.getCanHide())
                    .map((column) => (
                      <DropdownMenuCheckboxItem
                        key={column.id}
                        className="capitalize rounded-md text-slate-100 focus:bg-white/10 focus:text-white data-[state=checked]:bg-transparent"
                        checked={column.getIsVisible()}
                        onCheckedChange={(value) => column.toggleVisibility(!!value)}
                      >
                        {COLUMN_LABELS[column.id] ?? column.id}
                      </DropdownMenuCheckboxItem>
                    ))}
                </DropdownMenuContent>
              </DropdownMenu>

              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    size="sm"
                    className="border-white/20 bg-white/5 text-white hover:bg-white/10"
                  >
                    <IconSettings />
                    Сценарии
                  </Button>
                </PopoverTrigger>
                <PopoverContent
                  align="end"
                  sideOffset={10}
                  className="w-80 border border-slate-800 bg-[rgba(22,27,29,0.5)] text-white shadow-2xl backdrop-blur-md"
                >
                  <div className="space-y-4">
                    <h3 className="text-sm font-semibold text-white">Параметры парка</h3>

                    <div className="space-y-2 rounded-xl border border-white/10 bg-white/5 p-3">
                      <Label htmlFor="cargoCapacity">Вместимость ТС (ед.)</Label>
                      <Input
                        id="cargoCapacity"
                        type="number"
                        min={500}
                        step={50}
                        value={cargoCapacity}
                        className="border-white/15 bg-white/5 text-white"
                        onChange={(event) => {
                          const value = Number(event.target.value)
                          if (Number.isFinite(value) && value > 0) {
                            setCargoCapacity(value)
                          }
                        }}
                      />
                    </div>

                    <div className="space-y-2 rounded-xl border border-white/10 bg-white/5 p-3">
                      <div className="flex items-center justify-between">
                        <Label htmlFor="utilization">Полезная загрузка кузова</Label>
                        <span className="text-xs text-slate-300">{utilization.toFixed(2)}</span>
                      </div>
                      <input
                        id="utilization"
                        type="range"
                        min={0.5}
                        max={1}
                        step={0.01}
                        value={utilization}
                        onChange={(event) => setUtilization(Number(event.target.value))}
                        className="w-full accent-[#61A0B7]"
                      />
                    </div>

                    <div className="space-y-2 rounded-xl border border-white/10 bg-white/5 p-3">
                      <Label htmlFor="reserveTrucks">Резерв машин (шт.)</Label>
                      <Input
                        id="reserveTrucks"
                        type="number"
                        min={0}
                        step={1}
                        value={reserveTrucks}
                        className="border-white/15 bg-white/5 text-white"
                        onChange={(event) => {
                          const value = Number(event.target.value)
                          if (Number.isFinite(value)) {
                            setReserveTrucks(Math.max(0, Math.floor(value)))
                          }
                        }}
                      />
                    </div>

                    <div className="space-y-2 rounded-xl border border-white/10 bg-white/5 p-3">
                      <Label htmlFor="strategy">Стратегия перестраховки</Label>
                      <Select
                        value={strategy}
                        onValueChange={(value) => setStrategy(value as SimulationStrategy)}
                      >
                        <SelectTrigger
                          id="strategy"
                          className="w-full border-white/15 bg-white/5 text-white"
                        >
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="border-white/15 bg-[rgba(22,27,29,0.9)] text-white">
                          <SelectItem value="economy">{strategyLabel.economy}</SelectItem>
                          <SelectItem value="balance">{strategyLabel.balance}</SelectItem>
                          <SelectItem value="reliable">{strategyLabel.reliable}</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </PopoverContent>
              </Popover>

              <Button
                variant="outline"
                size="sm"
                className="border-white/20 bg-white/5 text-white hover:bg-white/10"
                onClick={resetSelectedStatusesToDefault}
                disabled={selectedRowsCount === 0}
              >
                <IconRefresh />
                <span className="hidden lg:inline">Сбросить статус</span>
                <span className="lg:hidden">Сброс</span>
              </Button>
            </div>
          </div>

          <div className="relative overflow-hidden rounded-xl bg-[rgba(22,27,29,0.16)] backdrop-blur-md">
            <div>
              <Table className="text-sm">
                <TableHeader className="bg-[rgba(22,27,29,0.52)]">
                  {table.getHeaderGroups().map((headerGroup) => (
                    <TableRow key={headerGroup.id}>
                      {headerGroup.headers.map((header) => (
                        <TableHead
                          key={header.id}
                          className={
                            header.column.id === "select"
                              ? "w-9 px-2.5 text-center"
                              : "px-2.5 text-center text-sm text-slate-200"
                          }
                        >
                          {header.isPlaceholder
                            ? null
                            : flexRender(
                                header.column.columnDef.header,
                                header.getContext()
                              )}
                        </TableHead>
                      ))}
                    </TableRow>
                  ))}
                </TableHeader>
                <TableBody>
                  {isLoading ? (
                    <TableRow>
                      <TableCell
                        colSpan={visibleColumnsCount}
                        className="h-20 text-center text-slate-300"
                      >
                        Загрузка реестра диспетчеризации...
                      </TableCell>
                    </TableRow>
                  ) : error ? (
                    <TableRow>
                      <TableCell
                        colSpan={visibleColumnsCount}
                        className="h-20 text-center text-red-300"
                      >
                        {error}
                      </TableCell>
                    </TableRow>
                  ) : table.getRowModel().rows.length ? (
                    table.getRowModel().rows.map((row) => (
                      <TableRow key={row.id}>
                        {row.getVisibleCells().map((cell) => (
                          <TableCell
                            key={cell.id}
                            className={
                              cell.column.id === "select"
                                ? "w-9 px-2.5 py-2.5 text-center"
                                : "px-2.5 py-2.5 text-center"
                            }
                          >
                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                          </TableCell>
                        ))}
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell
                        colSpan={visibleColumnsCount}
                        className="h-20 text-center text-slate-300"
                      >
                        Нет объектов для выбранного фильтра.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </div>

          <div className="mt-4 flex items-center justify-between">
            <div className="text-sm text-slate-300">
              Отображено {table.getRowModel().rows.length} из {filteredRows.length} результатов
            </div>
            <div className="flex items-center gap-4">
              <div className="text-sm">
                Страница {table.getState().pagination.pageIndex + 1} из{" "}
                {table.getPageCount() || 1}
              </div>

              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="icon"
                  className="size-8 border-white/20 bg-white/5 text-white hover:bg-white/10"
                  onClick={() => table.previousPage()}
                  disabled={!table.getCanPreviousPage()}
                >
                  <IconChevronLeft />
                  <span className="sr-only">Назад</span>
                </Button>
                <Button
                  variant="outline"
                  size="icon"
                  className="size-8 border-white/20 bg-white/5 text-white hover:bg-white/10"
                  onClick={() => table.nextPage()}
                  disabled={!table.getCanNextPage()}
                >
                  <IconChevronRight />
                  <span className="sr-only">Вперед</span>
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {selectedRowsCount > 0 ? (
        <div className="fixed bottom-8 left-1/2 z-50 flex -translate-x-1/2 items-center gap-4 rounded-2xl border border-[#274f68]/55 bg-[rgba(6,24,40,0.64)] p-4 shadow-2xl backdrop-blur-md">
          <span className="font-semibold text-white">Выбрано: {selectedRowsCount} заявок</span>
          <Button
            className="border border-white/20 bg-gradient-to-r from-slate-600/70 to-sky-700/70 text-white hover:from-slate-500 hover:to-sky-600"
            onClick={dispatchSelected}
          >
            Отправить заявки в ТК
          </Button>
          <Button
            className="border border-white/20 bg-gradient-to-r from-slate-600/70 to-sky-700/70 text-white hover:from-slate-500 hover:to-sky-600"
            onClick={exportSelectedCsv}
          >
            Скачать Excel
          </Button>
        </div>
      ) : null}
    </>
  )
}
