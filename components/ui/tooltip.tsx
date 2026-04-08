"use client"

import * as React from "react"
import { Tooltip as TooltipPrimitive } from "radix-ui"

import { cn } from "@/lib/utils"

const FORCED_GLASS_TOOLTIP_STYLE: React.CSSProperties = {
  // Keep tooltip glass exactly in the same palette as KPI cards.
  backgroundColor: "rgba(22, 27, 29, 0.42)",
  border: "1px solid rgba(255, 255, 255, 0.1)",
  color: "#FFFFFF",
  boxShadow: "0 4px 14px rgba(0, 0, 0, 0.22)",
  backdropFilter: "blur(14px)",
  WebkitBackdropFilter: "blur(14px)",
}

const FORCED_GLASS_TOOLTIP_ARROW_STYLE: React.CSSProperties = {
  backgroundColor: "rgba(22, 27, 29, 0.42)",
  fill: "rgba(22, 27, 29, 0.42)",
}

function TooltipProvider({
  delayDuration = 0,
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Provider>) {
  return (
    <TooltipPrimitive.Provider
      data-slot="tooltip-provider"
      delayDuration={delayDuration}
      {...props}
    />
  )
}

function Tooltip({
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Root>) {
  return <TooltipPrimitive.Root data-slot="tooltip" {...props} />
}

function TooltipTrigger({
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Trigger>) {
  return <TooltipPrimitive.Trigger data-slot="tooltip-trigger" {...props} />
}

function TooltipContent({
  className,
  sideOffset = 0,
  arrowClassName,
  children,
  style,
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Content> & {
  arrowClassName?: string
}) {
  return (
    <TooltipPrimitive.Portal>
      <TooltipPrimitive.Content
        data-slot="tooltip-content"
        sideOffset={sideOffset}
        className={cn(
          "z-50 inline-flex w-fit max-w-xs origin-(--radix-tooltip-content-transform-origin) items-center gap-1.5 rounded-2xl bg-transparent px-3 py-1.5 text-xs text-white has-data-[slot=kbd]:pr-1.5 **:data-[slot=kbd]:relative **:data-[slot=kbd]:isolate **:data-[slot=kbd]:z-50 **:data-[slot=kbd]:rounded-4xl data-[state=delayed-open]:animate-in data-[state=delayed-open]:fade-in-0 data-[state=delayed-open]:duration-200 data-open:animate-in data-open:fade-in-0 data-open:duration-200 data-closed:animate-out data-closed:fade-out-0 data-closed:duration-200",
          className
        )}
        style={{ ...(style ?? {}), ...FORCED_GLASS_TOOLTIP_STYLE }}
        {...props}
      >
        {children}
        <TooltipPrimitive.Arrow
          className={cn(
            "z-50 size-2.5 translate-y-[calc(-50%_-_2px)] rotate-45 rounded-[2px] bg-transparent fill-transparent data-[side=left]:translate-x-[-1.5px] data-[side=right]:translate-x-[1.5px]",
            arrowClassName
          )}
          style={FORCED_GLASS_TOOLTIP_ARROW_STYLE}
        />
      </TooltipPrimitive.Content>
    </TooltipPrimitive.Portal>
  )
}

export { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger }

