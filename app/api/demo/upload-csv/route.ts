import { NextResponse } from "next/server"

import { stageSubmissionCsv } from "@/lib/metrics-calculator"

export async function POST(request: Request) {
  try {
    const formData = await request.formData()
    const file = formData.get("file")

    if (!(file instanceof File)) {
      return NextResponse.json(
        { error: "Файл не передан. Используйте поле form-data `file`." },
        { status: 400 }
      )
    }

    const csvText = await file.text()
    const staged = stageSubmissionCsv(csvText, file.name)

    return NextResponse.json(
      {
        status: "staged",
        ...staged,
      },
      {
        headers: {
          "Cache-Control": "no-store",
        },
      }
    )
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Не удалось загрузить CSV."
    return NextResponse.json({ error: message }, { status: 500 })
  }
}

