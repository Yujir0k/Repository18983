"use client"

import * as React from "react"

type AppIntroShellProps = {
  children: React.ReactNode
}

export function AppIntroShell({ children }: AppIntroShellProps) {
  return <>{children}</>
}
