import { NextResponse } from "next/server"

import { applyStagedSubmission } from "@/lib/metrics-calculator"

export async function POST() {
  try {
    const result = applyStagedSubmission()
    return NextResponse.json(
      {
        status: "processed",
        ...result,
      },
      {
        headers: {
          "Cache-Control": "no-store",
        },
      }
    )
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Не удалось обработать загруженный CSV."
    const statusCode = message.includes("Нет загруженного CSV") ? 409 : 500
    return NextResponse.json({ error: message }, { status: statusCode })
  }
}

