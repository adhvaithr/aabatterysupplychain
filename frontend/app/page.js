'use client'

import Link from 'next/link'
import { useEffect, useMemo, useState } from 'react'
import { ArrowRight, ArrowDown, ArrowUp } from 'lucide-react'
import Nav from '../components/nav'
import { StateBadge, RiskPill, DCBadge, ActionText } from '../components/badges'
import { fmtDate, fmtMoney, supplyColor } from '../lib/format'
import { EVENTS, HERO_STATS } from '../lib/mock-data'

const COLS = [
  { key: 'sku', label: 'SKU' },
  { key: 'dest_dc', label: 'Dest DC' },
  { key: 'days_of_supply', label: 'Days of supply' },
  { key: 'stockout_date', label: 'Stockout date' },
  { key: 'risk', label: 'Risk' },
  { key: 'action', label: 'Recommended' },
  { key: 'state', label: 'State' },
]

const RISK_ORDER = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 }

export default function DashboardPage() {
  const [events, setEvents] = useState(EVENTS)
  const [hero, setHero] = useState(HERO_STATS)
  const isLoading = false

  useEffect(() => {
    // no-op; data is immediately available from static mock
  }, [])

  const [sortKey, setSortKey] = useState('days_of_supply')
  const [sortDir, setSortDir] = useState('asc')

  const rows = useMemo(() => {
    if (!events) return []
    const sorted = [...events].sort((a, b) => {
      let av = a[sortKey]
      let bv = b[sortKey]
      if (sortKey === 'risk') {
        av = RISK_ORDER[av]
        bv = RISK_ORDER[bv]
      }
      if (typeof av === 'string') return sortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return sortDir === 'asc' ? av - bv : bv - av
    })
    return sorted
  }, [events, sortKey, sortDir])

  const toggleSort = (key) => {
    if (sortKey === key) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="page-fade mx-auto max-w-[1400px] px-6 py-8">
        {/* Hero */}
        <section className="relative overflow-hidden rounded-md border border-neutral-900 bg-[#0D0D0F] shadow-[inset_3px_0_0_0_#F59E0B]">
          <div className="grid grid-cols-1 gap-6 px-8 py-7 md:grid-cols-[1.2fr_1px_1fr_1fr] md:items-center">
            <div>
              <div className="mono text-[56px] leading-none font-medium tracking-tight text-neutral-50">
                {fmtMoney(hero?.penalty_exposure || 47200)}
              </div>
              <div className="mt-2 text-[11px] uppercase tracking-[0.18em] text-neutral-500">
                projected penalty exposure
              </div>
            </div>
            <div className="hidden h-16 w-px bg-neutral-800 md:block" />
            <div>
              <div className="flex items-center gap-3">
                <span className="relative inline-flex h-2.5 w-2.5">
                  <span className="pulse-dot absolute inset-0 rounded-full bg-[#F59E0B]" />
                  <span className="absolute inset-0 rounded-full bg-[#F59E0B]" />
                </span>
                <div className="mono text-2xl text-neutral-100">
                  {hero?.transfers_recommended || 8} <span className="text-neutral-400 text-base">transfers recommended</span>
                </div>
              </div>
              <div className="mt-2 text-[11px] uppercase tracking-[0.18em] text-neutral-500">
                active recommendations
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2 md:justify-end">
              <Pill color="#EF4444" label={`${hero?.critical ?? 3} critical`} />
              <Pill color="#F59E0B" label={`${hero?.high ?? 3} high`} />
              <Pill color="#EAB308" label={`${hero?.medium ?? 2} medium`} />
            </div>
          </div>
        </section>

        {/* Table */}
        <section className="mt-8">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-sm uppercase tracking-[0.18em] text-neutral-500">
              Active risk events
            </h2>
            <div className="mono text-xs text-neutral-500">
              sorted by {COLS.find((c) => c.key === sortKey)?.label.toLowerCase()} ·{' '}
              {sortDir}
            </div>
          </div>
          <div className="overflow-hidden rounded-md border border-neutral-900 bg-[#0D0D0F]">
            <table className="w-full">
              <thead>
                <tr className="border-b border-neutral-900 bg-[#101013]">
                  {COLS.map((c) => (
                    <th
                      key={c.key}
                      onClick={() => toggleSort(c.key)}
                      className="cursor-pointer select-none px-5 py-3 text-left text-[10px] uppercase tracking-[0.18em] text-neutral-500 hover:text-neutral-300"
                    >
                      <span className="inline-flex items-center gap-1">
                        {c.label}
                        {sortKey === c.key ? (
                          sortDir === 'asc' ? (
                            <ArrowUp className="h-3 w-3" />
                          ) : (
                            <ArrowDown className="h-3 w-3" />
                          )
                        ) : null}
                      </span>
                    </th>
                  ))}
                  <th className="w-10 px-3" />
                </tr>
              </thead>
              <tbody>
                {isLoading && <SkeletonRows />}
                {!isLoading &&
                  rows.map((r) => (
                    <tr
                      key={r.id}
                      className="group border-b border-neutral-900 transition-colors last:border-0 hover:bg-[#131316]"
                    >
                      <td className="px-5 py-4">
                        <Link href={`/events/${r.id}`} className="block">
                          <div className="mono text-sm font-medium text-neutral-50">
                            {r.sku}
                          </div>
                          <div className="text-xs text-neutral-500">{r.product}</div>
                        </Link>
                      </td>
                      <td className="px-5 py-4">
                        <DCBadge code={r.dest_dc} />
                      </td>
                      <td className={`mono px-5 py-4 text-sm ${supplyColor(r.days_of_supply)}`}>
                        {r.days_of_supply} days
                      </td>
                      <td className="mono px-5 py-4 text-sm text-neutral-400">
                        {fmtDate(r.stockout_date)}
                      </td>
                      <td className="px-5 py-4">
                        <RiskPill risk={r.risk} />
                      </td>
                      <td className="px-5 py-4">
                        <ActionText action={r.action} />
                      </td>
                      <td className="px-5 py-4">
                        <StateBadge state={r.state} />
                      </td>
                      <td className="px-3 py-4 text-right">
                        <Link
                          href={`/events/${r.id}`}
                          className="inline-flex text-neutral-600 transition-all group-hover:translate-x-0 group-hover:text-[#F59E0B] opacity-0 group-hover:opacity-100"
                        >
                          <ArrowRight className="h-4 w-4" />
                        </Link>
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  )
}

function Pill({ color, label }) {
  return (
    <span
      className="mono inline-flex items-center gap-2 rounded-full border px-3 py-1 text-[11px] tracking-widest"
      style={{ borderColor: `${color}55`, color, backgroundColor: `${color}12` }}
    >
      <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: color }} />
      {label.toUpperCase()}
    </span>
  )
}

function SkeletonRows() {
  return Array.from({ length: 6 }).map((_, i) => (
    <tr key={i} className="border-b border-neutral-900">
      {Array.from({ length: 8 }).map((_, j) => (
        <td key={j} className="px-5 py-5">
          <div className="shimmer h-4 w-full rounded" />
        </td>
      ))}
    </tr>
  ))
}
