export type UnifiedDispatchStatus = "critical" | "drop" | "review" | "ok"

function toSafeNonNegativeNumber(value: unknown): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) return 0
  return Math.max(0, parsed)
}

export function getVolumeDeltaStatus(params: {
  forecast: unknown
  baseline: unknown
  confidence: unknown
}): {
  predVolume: number
  baseVolume: number
  volumeDelta: number
  status: UnifiedDispatchStatus
} {
  const predVolume = toSafeNonNegativeNumber(params.forecast)
  const baseVolume = toSafeNonNegativeNumber(params.baseline)
  const conf = toSafeNonNegativeNumber(params.confidence)

  const volumeDelta = predVolume - baseVolume
  // 1. Идеальное совпадение или Полный Автопилот (>= 85%)
  if (volumeDelta === 0 || conf >= 85) {
    return { predVolume, baseVolume, volumeDelta, status: "ok" }
  }

  const standardThreshold = Math.max(30, Math.ceil(baseVolume * 0.3))
  const extremeThreshold = Math.max(50, Math.ceil(baseVolume * 1.0))

  // 2. Уверенность 71% - 84% (Хорошая, но не идеальная)
  if (conf > 70) {
    if (volumeDelta >= standardThreshold) {
      return { predVolume, baseVolume, volumeDelta, status: "critical" }
    }
    if (volumeDelta <= -standardThreshold) {
      return { predVolume, baseVolume, volumeDelta, status: "drop" }
    }

    // Если разница МЕНЬШЕ порога — не дергаем человека, это штатный шум.
    return { predVolume, baseVolume, volumeDelta, status: "ok" }
  }

  // 3. Уверенность <= 70% (Модель сомневается)
  // Ловим Цунами (рост х2)
  if (volumeDelta >= extremeThreshold) {
    return { predVolume, baseVolume, volumeDelta, status: "critical" }
  }

  // Ловим заметные, но не экстремальные отклонения (отправляем на проверку)
  if (Math.abs(volumeDelta) >= standardThreshold) {
    return { predVolume, baseVolume, volumeDelta, status: "review" }
  }

  // Если разница микроскопическая, а модель неуверенная — игнорируем шум.
  return { predVolume, baseVolume, volumeDelta, status: "ok" }
}

export function confidenceFromWape(wape: unknown, fallback = 75): number {
  const normalizedFallback = Math.max(0, Math.min(100, Math.round(fallback)))
  const parsedWape = Number(wape)

  if (!Number.isFinite(parsedWape)) return normalizedFallback
  return Math.max(0, Math.min(100, Math.round((1 - parsedWape) * 100)))
}
