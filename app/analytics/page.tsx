import { AppSidebar } from "@/components/app-sidebar"
import { AnalyticsContent } from "@/components/analytics-content"
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"

export default function Page() {
  return (
    <SidebarProvider
      style={
        {
          "--sidebar-width": "calc(var(--spacing) * 72)",
          "--header-height": "calc(var(--spacing) * 12)",
        } as React.CSSProperties
      }
    >
      <AppSidebar variant="inset" />
      <SidebarInset>
        <AnalyticsContent />
      </SidebarInset>
    </SidebarProvider>
  )
}

