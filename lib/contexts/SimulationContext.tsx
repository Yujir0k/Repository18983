"use client"

import * as React from "react"

import type {
  SimulationSettings,
  SimulationStrategy,
  WaveSelection,
} from "@/types/dashboard-metrics"

type SimulationContextValue = SimulationSettings & {
  isHydrated: boolean
  refreshNonce: number
  setCargoCapacity: (value: number) => void
  setUtilization: (value: number) => void
  setStrategy: (value: SimulationStrategy) => void
  setReserveTrucks: (value: number) => void
  setSelectedWave: (value: WaveSelection) => void
  triggerRefresh: () => void
}

const DEFAULT_VALUE: SimulationContextValue = {
  cargoCapacity: 1800,
  utilization: 0.85,
  strategy: "balance",
  reserveTrucks: 0,
  selectedWave: 1,
  isHydrated: false,
  refreshNonce: 0,
  setCargoCapacity: () => undefined,
  setUtilization: () => undefined,
  setStrategy: () => undefined,
  setReserveTrucks: () => undefined,
  setSelectedWave: () => undefined,
  triggerRefresh: () => undefined,
}

const SIMULATION_SETTINGS_STORAGE_KEY = "rwb-flow-simulation-settings-v1"

function isSimulationStrategy(value: unknown): value is SimulationStrategy {
  return value === "economy" || value === "balance" || value === "reliable"
}

function isFinitePositiveNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
}

function isFiniteFraction(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1
}

function isFiniteNonNegativeInteger(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isFinite(value) &&
    value >= 0 &&
    Number.isInteger(value)
  )
}

const SimulationContext = React.createContext<SimulationContextValue>(DEFAULT_VALUE)

export function SimulationProvider({ children }: { children: React.ReactNode }) {
  const [cargoCapacity, setCargoCapacity] = React.useState(1800)
  const [utilization, setUtilization] = React.useState(0.85)
  const [strategy, setStrategy] = React.useState<SimulationStrategy>("balance")
  const [reserveTrucks, setReserveTrucks] = React.useState(0)
  const [selectedWave, setSelectedWave] = React.useState<WaveSelection>(1)
  const [refreshNonce, setRefreshNonce] = React.useState(0)
  const [isStorageHydrated, setIsStorageHydrated] = React.useState(false)

  React.useEffect(() => {
    try {
      const saved = window.localStorage.getItem(SIMULATION_SETTINGS_STORAGE_KEY)
      if (!saved) {
        return
      }

      const parsed = JSON.parse(saved) as {
        cargoCapacity?: unknown
        utilization?: unknown
        strategy?: unknown
        reserveTrucks?: unknown
      }

      if (isFinitePositiveNumber(parsed.cargoCapacity)) {
        setCargoCapacity(parsed.cargoCapacity)
      }
      if (isFiniteFraction(parsed.utilization)) {
        setUtilization(parsed.utilization)
      }
      if (isSimulationStrategy(parsed.strategy)) {
        setStrategy(parsed.strategy)
      }
      if (isFiniteNonNegativeInteger(parsed.reserveTrucks)) {
        setReserveTrucks(parsed.reserveTrucks)
      }
    } catch {
      // Ignore malformed localStorage payload and keep safe defaults.
    } finally {
      setIsStorageHydrated(true)
    }
  }, [])

  React.useEffect(() => {
    if (!isStorageHydrated) {
      return
    }

    const payload = {
      cargoCapacity,
      utilization,
      strategy,
      reserveTrucks,
    }

    window.localStorage.setItem(SIMULATION_SETTINGS_STORAGE_KEY, JSON.stringify(payload))
  }, [cargoCapacity, utilization, strategy, reserveTrucks, isStorageHydrated])

  const value = React.useMemo(
    () => ({
      cargoCapacity,
      utilization,
      strategy,
      reserveTrucks,
      selectedWave,
      isHydrated: isStorageHydrated,
      refreshNonce,
      setCargoCapacity,
      setUtilization,
      setStrategy,
      setReserveTrucks,
      setSelectedWave,
      triggerRefresh: () => setRefreshNonce((prev) => prev + 1),
    }),
    [
      cargoCapacity,
      utilization,
      strategy,
      reserveTrucks,
      selectedWave,
      isStorageHydrated,
      refreshNonce,
    ]
  )

  return <SimulationContext.Provider value={value}>{children}</SimulationContext.Provider>
}

export function useSimulation() {
  return React.useContext(SimulationContext)
}
