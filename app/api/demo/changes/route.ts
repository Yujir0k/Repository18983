import { NextResponse } from "next/server"

import { getDemoSubmissionStatus } from "@/lib/metrics-calculator"

export async function GET() {
  try {
    const status = getDemoSubmissionStatus()
    return NextResponse.json(status, {
      headers: {
        "Cache-Control": "no-store",
      },
    })
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Не удалось получить изменения."
    return NextResponse.json({ error: message }, { status: 500 })
  }
}

