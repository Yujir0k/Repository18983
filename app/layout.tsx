import type { Metadata } from "next"
import { Inter } from "next/font/google"

import "./globals.css"
import { AppIntroShell } from "@/components/app-intro-shell"
import { ThemeProvider } from "@/components/theme-provider"
import { TooltipProvider } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import { Toaster } from "sonner"

const inter = Inter({
  subsets: ["latin", "cyrillic"],
  variable: "--font-sans",
})

export const metadata: Metadata = {
  icons: {
    icon: "/RF_Browser_Logo.png",
    shortcut: "/RF_Browser_Logo.png",
    apple: "/RF_Browser_Logo.png",
  },
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html
      lang="ru"
      suppressHydrationWarning
      className={cn("antialiased", "font-sans", inter.variable)}
    >
      <body>
        <ThemeProvider>
          <AppIntroShell>
            <TooltipProvider>{children}</TooltipProvider>
          </AppIntroShell>
          <Toaster
            theme="dark"
            richColors
            toastOptions={{
              style: {
                background: "rgba(22, 27, 29, 0.9)",
                color: "#ffffff",
                border: "1px solid rgba(255, 255, 255, 0.16)",
              },
            }}
          />
        </ThemeProvider>
      </body>
    </html>
  )
}
