"use client"

import * as React from "react"
import {
  IconAlertTriangle,
  IconMinus,
  IconTrendingDown,
  IconTrendingUp,
} from "@tabler/icons-react"
import { Info } from "lucide-react"

import type {
  DashboardCardData,
  DashboardMetricsResponse,
  HorizonHours,
  MetricBadgeIcon,
} from "@/types/dashboard-metrics"
import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardAction,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

type SectionCardsProps = {
  selectedTimestamp: string
  selectedHorizonHours: HorizonHours
  isCumulative: boolean
}

type DashboardCardKey = keyof DashboardMetricsResponse["cards"]
type CardSize = "strategy" | "tactic"

const CARD_TOOLTIPS: Record<DashboardCardKey, string> = {
  dailyPlan24h:
    "РЎСѓРјРјР°СЂРЅС‹Р№ РїСЂРѕРіРЅРѕР· РѕС‚РіСЂСѓР·РѕРє РЅР° СЃРµРіРѕРґРЅСЏ РїРѕ РІСЃРµРј РґРѕСЃС‚СѓРїРЅС‹Рј РіРѕСЂРёР·РѕРЅС‚Р°Рј. РџРѕРєР°Р·С‹РІР°РµС‚ РѕР¶РёРґР°РµРјС‹Р№ РѕР±СЉРµРј Рё РїР»Р°РЅРѕРІРѕРµ РІС‹РїРѕР»РЅРµРЅРёРµ Рє С‚РµРєСѓС‰РµРјСѓ РІСЂРµРјРµРЅРё.",
  loadTrend:
    "РџРѕРєР°Р·С‹РІР°РµС‚ РѕРїРµСЂР°С‚РёРІРЅСѓСЋ РґРёРЅР°РјРёРєСѓ РїРѕС‚РѕРєР°. РњС‹ СЃСЂР°РІРЅРёРІР°РµРј РїСЂРѕРіРЅРѕР· РЅР° СЃР»РµРґСѓСЋС‰РёРµ 2 С‡Р°СЃР° СЃ СЂРµР°Р»СЊРЅС‹Рј С„Р°РєС‚РѕРј Р·Р° РїСЂРѕС€РµРґС€РёРµ 2 С‡Р°СЃР°, С‡С‚РѕР±С‹ РїРѕРЅСЏС‚СЊ, СЂР°СЃС‚РµС‚ РЅР°РіСЂСѓР·РєР° РёР»Рё РїР°РґР°РµС‚.",
  operationalVolumeH1:
    "РџСЂРѕРіРЅРѕР·РЅС‹Р№ РѕР±СЉРµРј РґР»СЏ РІС‹Р±СЂР°РЅРЅРѕРіРѕ РіРѕСЂРёР·РѕРЅС‚Р°. Р’ РЅР°РєРѕРїРёС‚РµР»СЊРЅРѕРј СЂРµР¶РёРјРµ СЃСѓРјРјРёСЂСѓСЋС‚СЃСЏ РІСЃРµ РёРЅС‚РµСЂРІР°Р»С‹ РѕС‚ С‚РµРєСѓС‰РµРіРѕ РјРѕРјРµРЅС‚Р° РґРѕ РєРѕРЅС†Р° РІС‹Р±СЂР°РЅРЅРѕРіРѕ РѕРєРЅР°.",
  fleetRequirementH1:
    "РљРѕР»РёС‡РµСЃС‚РІРѕ С„СѓСЂ СЂР°СЃСЃС‡РёС‚С‹РІР°РµС‚СЃСЏ РєР°Рє РѕР±С‰РёР№ РѕР±СЉРµРј, РґРµР»РµРЅРЅС‹Р№ РЅР° 1800 РµРґ. (СЃСЂРµРґРЅСЏСЏ РІРјРµСЃС‚РёРјРѕСЃС‚СЊ РѕРґРЅРѕР№ РјР°С€РёРЅС‹), СЃ РѕРєСЂСѓРіР»РµРЅРёРµРј РІ Р±РѕР»СЊС€СѓСЋ СЃС‚РѕСЂРѕРЅСѓ. РџСЂРѕС„РёС†РёС‚ РёР»Рё РґРµС„РёС†РёС‚ СЃС‡РёС‚Р°РµС‚СЃСЏ РѕС‚РЅРѕСЃРёС‚РµР»СЊРЅРѕ РїР°СЂРєР° РІ 500 РјР°С€РёРЅ.",
  accuracyH1:
    "РќР°РґРµР¶РЅРѕСЃС‚СЊ (С‚РѕС‡РЅРѕСЃС‚СЊ) СЂР°СЃСЃС‡РёС‚С‹РІР°РµС‚СЃСЏ РЅР° РѕСЃРЅРѕРІРµ СЃСЂРµРґРЅРµРіРѕ РѕС‚РєР»РѕРЅРµРЅРёСЏ РїСЂРѕРіРЅРѕР·Р° РѕС‚ С„Р°РєС‚Р° РїРѕ РІС‹Р±СЂР°РЅРЅС‹Рј РёРЅС‚РµСЂРІР°Р»Р°Рј. Р§РµРј РІС‹С€Рµ РїСЂРѕС†РµРЅС‚, С‚РµРј СЃС‚Р°Р±РёР»СЊРЅРµРµ Рё С‚РѕС‡РЅРµРµ РґР°РЅРЅС‹Рµ.",
  operationalAnomalies:
    "РЎРєР»Р°Рґ РїРѕРїР°РґР°РµС‚ РІ СЃРїРёСЃРѕРє, РµСЃР»Рё РїСЂРѕРіРЅРѕР· РѕС‚РєР»РѕРЅСЏРµС‚СЃСЏ РѕС‚ РЅРѕСЂРјС‹ Р±РѕР»РµРµ С‡РµРј РЅР° 20%, РїСЂРё СЌС‚РѕРј СЂР°Р·РЅРёС†Р° СЃРѕСЃС‚Р°РІР»СЏРµС‚ Р±РѕР»РµРµ 100 РµРґРёРЅРёС†. Р­С‚Рѕ РїРѕР·РІРѕР»СЏРµС‚ С„РѕРєСѓСЃРёСЂРѕРІР°С‚СЊСЃСЏ С‚РѕР»СЊРєРѕ РЅР° РєСЂСѓРїРЅС‹С… Р°РЅРѕРјР°Р»РёСЏС….",
}

function toneClassName(tone: DashboardCardData["tone"]) {
  if (tone === "danger") {
    return "text-red-500"
  }
  if (tone === "success") {
    return "text-emerald-500"
  }
  return "text-slate-300"
}

function badgeToneClassName(cardKey: DashboardCardKey) {
  void cardKey
  return "kpi-neon-badge kpi-neon-badge--primary border-transparent bg-transparent text-primary-foreground [&_svg]:text-primary-foreground"
}

function valueClassName(size: CardSize): string {
  if (size === "strategy") {
    return "text-5xl font-semibold tabular-nums text-white"
  }
  return "text-4xl font-semibold tabular-nums text-white"
}

function badgeIconByType(icon: MetricBadgeIcon | undefined) {
  if (icon === "down") {
    return <IconTrendingDown className="size-3" />
  }
  if (icon === "flat") {
    return <IconMinus className="size-3" />
  }
  if (icon === "up") {
    return <IconTrendingUp className="size-3" />
  }
  return null
}

function InfoHint({ text }: { text: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button
          type="button"
          aria-label="РџРѕСЏСЃРЅРµРЅРёРµ"
          className="kpi-info-icon inline-flex size-6 items-center justify-center rounded-full border bg-white/5 transition-all duration-200 hover:brightness-110"
        >
          <Info className="size-3.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent
        side="top"
        align="end"
        sideOffset={8}
        className="kpi-glass-tooltip max-w-[24rem] rounded-xl"
        arrowClassName="kpi-glass-tooltip-arrow"
      >
        {text}
      </TooltipContent>
    </Tooltip>
  )
}

function KpiCard({
  cardKey,
  card,
  tooltip,
  size,
  isRefreshing,
}: {
  cardKey: DashboardCardKey
  card: DashboardCardData
  tooltip: string
  size: CardSize
  isRefreshing: boolean
}) {
  return (
    <Card
      className={`@container/card relative border-white/15 bg-[rgba(22,27,29,0.62)] shadow-[0_8px_24px_rgba(0,0,0,0.28)] backdrop-blur-md transition-all duration-300 ease-in-out will-change-transform ${isRefreshing ? "translate-y-[5px] opacity-80" : "translate-y-0 opacity-100 hover:-translate-y-[4px] hover:shadow-[0_14px_34px_rgba(0,0,0,0.34)]"}`}
    >
      <CardHeader>
        <CardDescription className="text-slate-300">{card.title}</CardDescription>
        <CardTitle className={valueClassName(size)}>{card.value}</CardTitle>
        <CardAction>
          <Badge variant="outline" className={badgeToneClassName(cardKey)}>
            {badgeIconByType(card.badgeIcon)}
            {card.badge}
          </Badge>
        </CardAction>
      </CardHeader>
      <CardFooter className="flex-col items-start gap-2.5 pt-3 pr-10 text-sm">
        <div className="line-clamp-1 flex gap-2 font-medium text-slate-100">{card.primaryHint}</div>
        <div className={toneClassName(card.tone)}>{card.secondaryHint}</div>
      </CardFooter>
      <div className="absolute right-3 bottom-3">
        <InfoHint text={tooltip} />
      </div>
    </Card>
  )
}

function LoadingCard({ size }: { size: CardSize }) {
  return (
    <Card className="@container/card border-white/15 bg-[rgba(22,27,29,0.62)] shadow-[0_8px_24px_rgba(0,0,0,0.28)] backdrop-blur-md">
      <CardHeader>
        <CardDescription className="text-slate-300">Р—Р°РіСЂСѓР·РєР° РјРµС‚СЂРёРє...</CardDescription>
        <CardTitle className={valueClassName(size)}>...</CardTitle>
        <CardAction>
          <Badge variant="outline">
            <IconTrendingUp className="size-3" />
            РѕР±РЅРѕРІР»РµРЅРёРµ
          </Badge>
        </CardAction>
      </CardHeader>
      <CardFooter className="flex-col items-start gap-2.5 pt-3 text-sm text-slate-300">
        <div className="line-clamp-1 flex gap-2 font-medium">РџРѕРґРіРѕС‚РѕРІРєР° РґР°РЅРЅС‹С…...</div>
        <div className="text-slate-400">РСЃС‚РѕС‡РЅРёРє: /weights</div>
      </CardFooter>
    </Card>
  )
}

export function SectionCards({
  selectedTimestamp,
  selectedHorizonHours,
  isCumulative,
}: SectionCardsProps) {
  const [metrics, setMetrics] = React.useState<DashboardMetricsResponse | null>(null)
  const [isLoading, setIsLoading] = React.useState(true)
  const [isRefreshing, setIsRefreshing] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)
  const hasLoadedRef = React.useRef(false)

  React.useEffect(() => {
    const abortController = new AbortController()
    let refreshTimer: number | null = null

    const fetchMetrics = async () => {
      const isFirstLoad = !hasLoadedRef.current
      if (isFirstLoad) {
        setIsLoading(true)
      } else {
        setIsRefreshing(true)
      }
      setError(null)

      try {
        const response = await fetch(
          `/api/dashboard-metrics?timestamp=${encodeURIComponent(selectedTimestamp)}&horizonHours=${selectedHorizonHours}&cumulative=${isCumulative}`,
          {
            method: "GET",
            cache: "no-store",
            signal: abortController.signal,
          }
        )

        if (!response.ok) {
          throw new Error("РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё KPI-РјРµС‚СЂРёРє.")
        }

        const payload = (await response.json()) as DashboardMetricsResponse
        setMetrics(payload)
        hasLoadedRef.current = true
      } catch (err) {
        if (!(err instanceof DOMException && err.name === "AbortError")) {
          const message = err instanceof Error ? err.message : "РћС€РёР±РєР° Р·Р°РіСЂСѓР·РєРё KPI-РјРµС‚СЂРёРє."
          setError(message)
        }
      } finally {
        if (!abortController.signal.aborted) {
          setIsLoading(false)
          refreshTimer = window.setTimeout(() => {
            if (!abortController.signal.aborted) {
              setIsRefreshing(false)
            }
          }, 300)
        }
      }
    }

    void fetchMetrics()

    return () => {
      abortController.abort()
      if (refreshTimer) {
        window.clearTimeout(refreshTimer)
      }
    }
  }, [selectedTimestamp, selectedHorizonHours, isCumulative])

  const strategyCards = metrics
    ? ([
        { key: "dailyPlan24h", card: metrics.cards.dailyPlan24h },
        { key: "loadTrend", card: metrics.cards.loadTrend },
      ] as const)
    : []

  const tacticalCards = metrics
    ? ([
        { key: "operationalVolumeH1", card: metrics.cards.operationalVolumeH1 },
        { key: "fleetRequirementH1", card: metrics.cards.fleetRequirementH1 },
        { key: "accuracyH1", card: metrics.cards.accuracyH1 },
        { key: "operationalAnomalies", card: metrics.cards.operationalAnomalies },
      ] as const)
    : []

  return (
    <div className="space-y-4 px-4 lg:px-6">
      {isLoading ? (
        <>
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <LoadingCard size="strategy" />
            <LoadingCard size="strategy" />
          </div>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4 @5xl/main:grid-cols-4">
            <LoadingCard size="tactic" />
            <LoadingCard size="tactic" />
            <LoadingCard size="tactic" />
            <LoadingCard size="tactic" />
          </div>
        </>
      ) : null}

      {!isLoading && error ? (
        <Card className="@container/card col-span-full border-white/15 bg-[rgba(22,27,29,0.62)] shadow-[0_8px_24px_rgba(0,0,0,0.28)] backdrop-blur-md">
          <CardHeader>
            <CardDescription className="text-slate-300">РћС€РёР±РєР° СЂР°СЃС‡РµС‚Р° KPI</CardDescription>
            <CardTitle className="text-2xl font-semibold text-white @[250px]/card:text-3xl">
              Р”Р°РЅРЅС‹Рµ РІСЂРµРјРµРЅРЅРѕ РЅРµРґРѕСЃС‚СѓРїРЅС‹
            </CardTitle>
            <CardAction>
              <Badge variant="outline" className="border-red-500/40 text-red-300">
                <IconAlertTriangle className="size-3" />
                РѕС€РёР±РєР°
              </Badge>
            </CardAction>
          </CardHeader>
          <CardFooter className="flex-col items-start gap-2.5 pt-3 text-sm">
            <div className="font-medium text-red-400">{error}</div>
            <div className="text-slate-300">
              РџСЂРѕРІРµСЂСЊС‚Рµ РЅР°Р»РёС‡РёРµ С„Р°Р№Р»РѕРІ `submission.csv`, `inference_state.npz` Рё `metrics.json` РІ РїР°РїРєРµ
              `/weights`.
            </div>
          </CardFooter>
        </Card>
      ) : null}

      {!isLoading && !error ? (
        <>
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            {strategyCards.map(({ key, card }) => (
              <KpiCard
                key={key}
                cardKey={key}
                card={card}
                tooltip={CARD_TOOLTIPS[key]}
                size="strategy"
                isRefreshing={isRefreshing}
              />
            ))}
          </div>
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4 @5xl/main:grid-cols-4">
            {tacticalCards.map(({ key, card }) => (
              <KpiCard
                key={key}
                cardKey={key}
                card={card}
                tooltip={CARD_TOOLTIPS[key]}
                size="tactic"
                isRefreshing={isRefreshing}
              />
            ))}
          </div>
        </>
      ) : null}
    </div>
  )
}

