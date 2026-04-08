"use client"

import * as React from "react"
import { RefreshCw } from "lucide-react"
import { toast } from "sonner"

import { cn } from "@/lib/utils"
import { useSimulation } from "@/lib/contexts/SimulationContext"
import { Button } from "@/components/ui/button"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Separator } from "@/components/ui/separator"
import { SidebarTrigger } from "@/components/ui/sidebar"

const WAVE_OPTIONS = [
  { value: 1, label: "Ближайшие 2 часа" },
  { value: 2, label: "Следующие 2 часа" },
] as const

const TOP_PANEL_BUTTON_CLASS =
  "h-9 w-[186px] rounded-full px-4 text-[16px] font-semibold leading-none whitespace-nowrap"

type DemoChanges = {
  comparedAtUtc: string
  changedCells: number
  changedRoutes: number
  totalsBefore: {
    wave1: number
    wave2: number
    total20h: number
  }
  totalsAfter: {
    wave1: number
    wave2: number
    total20h: number
  }
  totalsDelta: {
    wave1: number
    wave2: number
    total20h: number
  }
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

type DemoStatusResponse = {
  isUsingBaseline: boolean
  hasStagedUpload: boolean
  stagedRows: number
  stagedFileName: string | null
  stagedAtUtc: string | null
  stagedTimestampMinUtc: string | null
  stagedTimestampMaxUtc: string | null
  activeFileName: string
  activeAtUtc: string | null
  activeInferenceMeta: DemoInferenceMeta | null
  lastDiff: DemoChanges | null
}

function formatUnits(value: number): string {
  return new Intl.NumberFormat("ru-RU", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Math.round(value))
}

function formatSignedUnits(value: number): string {
  const rounded = Math.round(value)
  const sign = rounded > 0 ? "+" : ""
  return `${sign}${formatUnits(rounded)}`
}

function formatUtcDateTime(value: string | null | undefined): string {
  if (!value) {
    return "-"
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return value
  }

  return `${parsed.toLocaleString("ru-RU", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "Europe/Moscow",
  })}`
}

type SiteHeaderProps = {
  title?: string
}

export function SiteHeader({ title = "Центр управления" }: SiteHeaderProps) {
  const { selectedWave, setSelectedWave, triggerRefresh } = useSimulation()
  const [isDemoPopoverOpen, setIsDemoPopoverOpen] = React.useState(false)
  const [isUploadingCsv, setIsUploadingCsv] = React.useState(false)
  const [isProcessingCsv, setIsProcessingCsv] = React.useState(false)
  const [isLoadingChanges, setIsLoadingChanges] = React.useState(false)
  const [demoStatus, setDemoStatus] = React.useState<DemoStatusResponse | null>(null)
  const fileInputRef = React.useRef<HTMLInputElement | null>(null)

  const loadDemoStatus = React.useCallback(async () => {
    setIsLoadingChanges(true)
    try {
      const response = await fetch("/api/demo/changes", {
        method: "GET",
        cache: "no-store",
      })

      if (!response.ok) {
        throw new Error("Не удалось получить изменения демо.")
      }

      const payload = (await response.json()) as DemoStatusResponse
      setDemoStatus(payload)
      return payload
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Не удалось получить изменения демо."
      toast.error(message)
      return null
    } finally {
      setIsLoadingChanges(false)
    }
  }, [])

  React.useEffect(() => {
    if (!isDemoPopoverOpen) {
      return
    }
    void loadDemoStatus()
  }, [isDemoPopoverOpen, loadDemoStatus])

  const handleUploadCsvClick = () => {
    if (isUploadingCsv || isProcessingCsv) {
      return
    }
    fileInputRef.current?.click()
  }

  const handleCsvSelected = async (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = event.target.files?.[0]
    event.currentTarget.value = ""

    if (!file) {
      return
    }

    setIsUploadingCsv(true)
    try {
      const formData = new FormData()
      formData.append("file", file)

      const response = await fetch("/api/demo/upload-csv", {
        method: "POST",
        body: formData,
      })

      const payload = (await response.json()) as
        | {
            status: "staged"
            stagedRows: number
            routeCount: number
            horizonCount: number
            fileName: string
            stagedAtUtc: string
            stagedTimestampMinUtc: string | null
            stagedTimestampMaxUtc: string | null
          }
        | { error: string }

      if (!response.ok || "error" in payload) {
        throw new Error(
          "error" in payload ? payload.error : "Не удалось загрузить CSV."
        )
      }

      toast.success("CSV загружен", {
        description: `${payload.stagedRows} строк в stage (${payload.fileName})`,
      })
      setDemoStatus((previous) => ({
        ...(previous ?? {
          isUsingBaseline: true,
          hasStagedUpload: false,
          stagedRows: 0,
          stagedFileName: null,
          stagedAtUtc: null,
          stagedTimestampMinUtc: null,
          stagedTimestampMaxUtc: null,
          activeFileName: "submission.csv",
          activeAtUtc: null,
          activeInferenceMeta: null,
          lastDiff: null,
        }),
        hasStagedUpload: true,
        stagedRows: payload.stagedRows,
        stagedFileName: payload.fileName,
        stagedAtUtc: payload.stagedAtUtc,
        stagedTimestampMinUtc: payload.stagedTimestampMinUtc,
        stagedTimestampMaxUtc: payload.stagedTimestampMaxUtc,
      }))
      await loadDemoStatus()
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Не удалось загрузить CSV."
      toast.error(message)
    } finally {
      setIsUploadingCsv(false)
    }
  }

  const handleProcessCsv = async () => {
    setIsProcessingCsv(true)
    try {
      const response = await fetch("/api/demo/process-upload", {
        method: "POST",
      })

      const payload = (await response.json()) as
        | {
            diff: DemoChanges
            activeFileName: string
          }
        | { error: string }

      if (!response.ok || "error" in payload) {
        throw new Error(
          "error" in payload
            ? payload.error
            : "Не удалось обработать загруженные данные."
        )
      }

      toast.success("Новые данные обработаны", {
        description: `Обновлено маршрутов: ${payload.diff.changedRoutes}`,
      })

      window.dispatchEvent(new CustomEvent("rwb-demo-data-replaced"))
      triggerRefresh()
      await loadDemoStatus()
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Не удалось обработать загруженные данные."
      toast.error(message)
    } finally {
      setIsProcessingCsv(false)
    }
  }

  const handleShowChanges = async () => {
    const status = await loadDemoStatus()
    if (!status?.lastDiff) {
      toast.message("Изменений пока нет", {
        description: "Сначала загрузите и обработайте новый CSV.",
      })
      return
    }

    toast.success("Изменения рассчитаны", {
      description: `Δ ближайшие 2ч: ${formatSignedUnits(
        status.lastDiff.totalsDelta.wave1
      )} ед.`,
    })
  }

  const handleResetBaseline = async () => {
    setIsProcessingCsv(true)
    try {
      const response = await fetch("/api/demo/reset", {
        method: "POST",
      })
      const payload = (await response.json()) as
        | {
            status: "reset"
            activeFileName: string
          }
        | { error: string }

      if (!response.ok || "error" in payload) {
        throw new Error(
          "error" in payload
            ? payload.error
            : "Не удалось вернуть исходный прогноз submission.csv."
        )
      }

      toast.success("Вернули исходный прогноз", {
        description: "Активирован исходный submission.csv",
      })
      window.dispatchEvent(new CustomEvent("rwb-demo-data-replaced"))
      triggerRefresh()
      await loadDemoStatus()
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Не удалось вернуть исходный прогноз submission.csv."
      toast.error(message)
    } finally {
      setIsProcessingCsv(false)
    }
  }

  return (
    <header className="flex h-(--header-height) shrink-0 items-center gap-2 border-b transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-(--header-height)">
      <div className="flex w-full items-center gap-2 px-4 lg:px-6">
        <SidebarTrigger className="-ml-1" />
        <Separator
          orientation="vertical"
          className="mx-1 data-[orientation=vertical]:h-4"
        />
        <h1 className="text-base font-semibold text-white">{title}</h1>

        <div className="ml-auto flex items-center gap-2">
          <div className="flex items-center gap-2">
            {WAVE_OPTIONS.map((option) => {
              const isActive = selectedWave === option.value
              return (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => setSelectedWave(option.value)}
                  className={cn(
                    TOP_PANEL_BUTTON_CLASS,
                    "border transition-colors duration-200",
                    isActive
                      ? "border border-white/20 bg-gradient-to-r from-slate-600/70 to-sky-700/70 text-white shadow-sm hover:from-slate-500 hover:to-sky-600"
                      : "border-white/20 bg-white/5 text-white hover:bg-white/10"
                  )}
                >
                  {option.label}
                </button>
              )
            })}
          </div>

          <Popover open={isDemoPopoverOpen} onOpenChange={setIsDemoPopoverOpen}>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                size="icon"
                className="size-9 rounded-full border-white/20 bg-white/5 text-white hover:bg-white/10"
                aria-label="Обновить данные"
              >
                <RefreshCw
                  className={cn(
                    "size-4",
                    (isUploadingCsv || isProcessingCsv || isLoadingChanges) &&
                      "animate-spin"
                  )}
                />
              </Button>
            </PopoverTrigger>
            <PopoverContent
              align="end"
              sideOffset={8}
              className="w-96 border border-slate-800 bg-[rgba(22,27,29,0.5)] text-white shadow-2xl backdrop-blur-md"
            >
              <div className="space-y-3">
                <h3 className="text-sm font-semibold text-white">Обновление данных</h3>

                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv,text/csv"
                  className="hidden"
                  onChange={handleCsvSelected}
                />

                <div className="grid gap-2">
                  <Button
                    variant="outline"
                    className="justify-start border-white/20 bg-white/5 text-white hover:bg-white/10"
                    disabled={isUploadingCsv || isProcessingCsv}
                    onClick={handleUploadCsvClick}
                  >
                    {isUploadingCsv ? "Загрузка CSV..." : "Загрузить CSV"}
                  </Button>

                  <Button
                    variant="outline"
                    className="justify-start border-white/20 bg-white/5 text-white hover:bg-white/10"
                    disabled={
                      isUploadingCsv ||
                      isProcessingCsv ||
                      !demoStatus?.hasStagedUpload
                    }
                    onClick={handleProcessCsv}
                  >
                    {isProcessingCsv
                      ? "Обработка новых данных..."
                      : "Обработать новые данные"}
                  </Button>

                  <Button
                    variant="outline"
                    className="justify-start border-white/20 bg-white/5 text-white hover:bg-white/10"
                    disabled={isLoadingChanges}
                    onClick={handleShowChanges}
                  >
                    Показать изменения
                  </Button>

                  <Button
                    variant="outline"
                    className="justify-start border-white/20 bg-white/5 text-white hover:bg-white/10"
                    disabled={isUploadingCsv || isProcessingCsv || demoStatus?.isUsingBaseline}
                    onClick={handleResetBaseline}
                  >
                    Вернуть исходный прогноз
                  </Button>
                </div>

                <div className="rounded-xl border border-white/10 bg-white/5 p-3 text-xs text-slate-300">
                  <p>
                    Stage:{" "}
                    {demoStatus?.hasStagedUpload
                      ? `${demoStatus.stagedRows} строк (${demoStatus.stagedFileName ?? "csv"})`
                      : "нет загруженного CSV"}
                  </p>
                  <p className="mt-1">
                    Окно загруженных данных:{" "}
                    {demoStatus?.stagedTimestampMinUtc || demoStatus?.stagedTimestampMaxUtc
                      ? `${formatUtcDateTime(demoStatus?.stagedTimestampMinUtc)} - ${formatUtcDateTime(
                          demoStatus?.stagedTimestampMaxUtc
                        )}`
                      : "-"}
                  </p>
                  <p className="mt-1">
                    Активный источник: {demoStatus?.activeFileName ?? "submission.csv"}
                  </p>
                  <p className="mt-1">
                    Срез для прогноза:{" "}
                    {formatUtcDateTime(
                      demoStatus?.activeInferenceMeta?.anchor_timestamp_max_utc ?? null
                    )}
                  </p>
                  <p className="mt-1">
                    h4 window:{" "}
                    {demoStatus?.activeInferenceMeta?.horizon_4_min_utc ||
                    demoStatus?.activeInferenceMeta?.horizon_4_max_utc
                      ? `${formatUtcDateTime(
                          demoStatus?.activeInferenceMeta?.horizon_4_min_utc ?? null
                        )} - ${formatUtcDateTime(
                          demoStatus?.activeInferenceMeta?.horizon_4_max_utc ?? null
                        )}`
                      : "-"}
                  </p>
                  <p className="mt-1">
                    h8 window:{" "}
                    {demoStatus?.activeInferenceMeta?.horizon_8_min_utc ||
                    demoStatus?.activeInferenceMeta?.horizon_8_max_utc
                      ? `${formatUtcDateTime(
                          demoStatus?.activeInferenceMeta?.horizon_8_min_utc ?? null
                        )} - ${formatUtcDateTime(
                          demoStatus?.activeInferenceMeta?.horizon_8_max_utc ?? null
                        )}`
                      : "-"}
                  </p>
                  <p className="mt-1">
                    Маршрутов во входе/прогнозе:{" "}
                    {demoStatus?.activeInferenceMeta?.input_routes ?? "-"} /{" "}
                    {demoStatus?.activeInferenceMeta?.predicted_routes ?? "-"}
                  </p>
                  {demoStatus?.lastDiff ? (
                    <div className="mt-2 space-y-1">
                      <p>
                        Изменено маршрутов: {demoStatus.lastDiff.changedRoutes}, ячеек:{" "}
                        {demoStatus.lastDiff.changedCells}
                      </p>
                      <p>
                        Δ Ближайшие 2 часа:{" "}
                        {formatSignedUnits(demoStatus.lastDiff.totalsDelta.wave1)} ед.
                      </p>
                      <p>
                        Δ Следующие 2 часа:{" "}
                        {formatSignedUnits(demoStatus.lastDiff.totalsDelta.wave2)} ед.
                      </p>
                    </div>
                  ) : null}
                </div>
              </div>
            </PopoverContent>
          </Popover>
        </div>
      </div>
    </header>
  )
}
