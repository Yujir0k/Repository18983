"use client"

import Link from "next/link"
import { usePathname } from "next/navigation"
import type { ComponentType, SVGProps } from "react"

import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"

export function NavMain({
  items,
}: {
  items: {
    title: string
    url: string
    icon?: ComponentType<SVGProps<SVGSVGElement>>
  }[]
}) {
  const pathname = usePathname()
  const [primaryItem, ...secondaryItems] = items

  return (
    <SidebarGroup>
      <SidebarGroupContent className="flex flex-col gap-2">
        {primaryItem ? (
          <SidebarMenu>
            <SidebarMenuItem>
              <SidebarMenuButton
                asChild
                tooltip={primaryItem.title}
                className="min-w-8 border border-white/20 bg-gradient-to-r from-slate-600/70 to-sky-700/70 text-white duration-200 ease-linear hover:from-slate-500 hover:to-sky-600 hover:text-white active:text-white"
              >
                <Link href={primaryItem.url}>
                  {primaryItem.icon && <primaryItem.icon />}
                  <span>{primaryItem.title}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
        ) : null}
        <SidebarMenu>
          {secondaryItems.map((item) => {
            const isRoute = item.url.startsWith("/")
            const isActive = isRoute ? pathname === item.url : false

            return (
              <SidebarMenuItem key={item.title}>
                <SidebarMenuButton asChild tooltip={item.title} isActive={isActive}>
                  {isRoute ? (
                    <Link href={item.url}>
                      {item.icon && <item.icon />}
                      <span>{item.title}</span>
                    </Link>
                  ) : (
                    <a href={item.url}>
                      {item.icon && <item.icon />}
                      <span>{item.title}</span>
                    </a>
                  )}
                </SidebarMenuButton>
              </SidebarMenuItem>
            )
          })}
        </SidebarMenu>
      </SidebarGroupContent>
    </SidebarGroup>
  )
}
