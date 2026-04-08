import { NextResponse } from "next/server"

import { resetDemoSubmissionToBaseline } from "@/lib/metrics-calculator"

export async function POST() {
  try {
    const result = resetDemoSubmissionToBaseline()
    return NextResponse.json(
      {
        status: "reset",
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
      error instanceof Error
        ? error.message
        : "Не удалось вернуть baseline submission.csv."
    return NextResponse.json({ error: message }, { status: 500 })
  }
}

