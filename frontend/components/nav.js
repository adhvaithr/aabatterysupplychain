'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'

export default function Nav() {
  const path = usePathname()
  const isActive = (p) => {
    if (p === '/') return path === '/'
    return path?.startsWith(p)
  }
  const Link2 = ({ href, children }) => (
    <Link
      href={href}
      className={`relative px-1 py-1 text-sm tracking-wide transition-colors ${
        isActive(href) ? 'text-neutral-100' : 'text-neutral-400 hover:text-neutral-200'
      }`}
    >
      {children}
      {isActive(href) && (
        <span className="absolute -bottom-[10px] left-0 right-0 h-[2px] bg-[#F59E0B]" />
      )}
    </Link>
  )
  return (
    <header className="sticky top-0 z-40 border-b border-neutral-900/90 bg-[#0A0A0B]/85 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-[1400px] items-center justify-between px-6">
        <Link href="/" className="mono text-sm font-medium tracking-wider">
          <span className="text-[#F59E0B]">POP</span>
          <span className="text-neutral-200">_SENTINEL</span>
        </Link>
        <nav className="flex items-center gap-7">
          <Link2 href="/">Dashboard</Link2>
          <Link2 href="/approvals">Approvals</Link2>
        </nav>
        <div className="mono text-xs text-neutral-500">
          <span className="mr-3 inline-block h-1.5 w-1.5 rounded-full bg-[#22C55E] align-middle" />
          LIVE · v1.2.4
        </div>
      </div>
    </header>
  )
}
