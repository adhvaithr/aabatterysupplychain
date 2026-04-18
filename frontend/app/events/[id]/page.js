'use client'

import Link from 'next/link'
import { useParams, useRouter } from 'next/navigation'
import { useEffect, useMemo, useState } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  CartesianGrid,
  ResponsiveContainer,
} from 'recharts'
import Nav from '../../../components/nav'
import { StateBadge, RiskPill, DCBadge } from '../../../components/badges'
import { fmtDate, fmtMoney, fmtUnits, supplyColor, supplyBorder, daysBetween } from '../../../lib/format'
import { buildDepletion, EVENTS, getEvent, DC_INFO } from '../../../lib/mock-data'
import { ChevronRight, Sparkles, Loader2 } from 'lucide-react'

export default function EventPage() {
  const { id } = useParams()
  const router = useRouter()
  const [event, setEvent] = useState(() => (typeof id === 'string' ? getEvent(id) : null))
  const [chartWidth, setChartWidth] = useState(1200)
  const isLoading = !event

  useEffect(() => {
    setEvent(getEvent(id))
  }, [id])

  useEffect(() => {
    const update = () => {
      const el = document.getElementById('depletion-chart-wrap')
      if (el) setChartWidth(el.clientWidth)
    }
    update()
    window.addEventListener('resize', update)
    return () => window.removeEventListener('resize', update)
  }, [event])

  const [analyzing, setAnalyzing] = useState(false)
  const [transferCreated, setTransferCreated] = useState(false)
  const [monitoring, setMonitoring] = useState(false)

  const dep = useMemo(() => (event ? buildDepletion(event) : null), [event])

  if (isLoading || !event) {
    return (
      <div className="min-h-screen">
        <Nav />
        <main className="mx-auto max-w-[1400px] px-6 py-10">
          <div className="shimmer h-8 w-64 rounded" />
          <div className="shimmer mt-6 h-40 w-full rounded" />
          <div className="shimmer mt-6 h-80 w-full rounded" />
        </main>
      </div>
    )
  }

  const cards = [
    { code: event.source_dc, units: unitsFor(event, event.source_dc), days: daysFor(event, event.source_dc), po: poFor(event, event.source_dc), role: 'source' },
    { code: event.dest_dc, units: unitsFor(event, event.dest_dc), days: event.days_of_supply, po: poFor(event, event.dest_dc), role: 'dest' },
    { code: event.third_dc, units: unitsFor(event, event.third_dc), days: daysFor(event, event.third_dc), po: poFor(event, event.third_dc), role: 'neutral' },
  ]

  // ordered SF / NJ / LA
  const order = ['SF', 'NJ', 'LA']
  const ordered = order.map((c) => cards.find((x) => x.code === c)).filter(Boolean)

  const postTransferDays = Math.round(
    (event.nj_units + event.transfer_qty) / event.nj_daily_burn
  )

  const stockoutBeforePO =
    daysBetween(new Date().toISOString(), event.po_eta) - event.days_of_supply

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="page-fade mx-auto max-w-[1400px] px-6 py-8">
        {/* Breadcrumb */}
        <div className="mb-3 flex items-center gap-2 text-xs text-neutral-500">
          <Link href="/" className="hover:text-neutral-300">Dashboard</Link>
          <ChevronRight className="h-3.5 w-3.5" />
          <span className="mono text-neutral-300">{event.sku} — {event.dest_dc}</span>
          <div className="ml-auto">
            <StateBadge state={event.state} size="lg" />
          </div>
        </div>

        {/* SKU header row */}
        <div className="rounded-md border border-neutral-900 bg-[#0D0D0F] p-6">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <div className="flex items-center gap-3">
                <h1 className="text-2xl font-medium text-neutral-50">{event.product}</h1>
                <RiskPill risk={event.risk} />
              </div>
              <div className="mt-2 flex items-center gap-3 text-sm">
                <span className="mono text-neutral-400">{event.sku}</span>
                <span className="text-neutral-700">·</span>
                <span className="text-neutral-500">{event.category}</span>
              </div>
            </div>
            <div className="text-right">
              <div className="text-[11px] uppercase tracking-[0.18em] text-neutral-500">Destination</div>
              <div className="mt-1 flex items-center gap-2">
                <DCBadge code={event.dest_dc} />
                <span className="text-sm text-neutral-400">{DC_INFO[event.dest_dc].city}</span>
              </div>
            </div>
          </div>
        </div>

        {/* DC cards */}
        <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
          {ordered.map((c) => (
            <DCCard key={c.code} c={c} dest={event.dest_dc} source={event.source_dc} />
          ))}
        </div>

        {/* Depletion chart */}
        <section className="mt-6 rounded-md border border-neutral-900 bg-[#0D0D0F] p-6">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h3 className="text-sm uppercase tracking-[0.18em] text-neutral-500">
                {event.dest_dc} Depletion forecast · 60 days
              </h3>
              <p className="mt-1 text-xs text-neutral-600">
                Daily burn {fmtUnits(event.nj_daily_burn)} units · PO arrival replenishes {fmtUnits(event.po_units)} units
              </p>
            </div>
          </div>
          <div id="depletion-chart-wrap" className="w-full overflow-hidden" style={{ height: 288 }}>
            <LineChart width={chartWidth} height={288} data={dep.points} margin={{ top: 16, right: 24, left: 8, bottom: 16 }}>
              <CartesianGrid stroke="#1a1a1a" strokeDasharray="2 4" />
              <XAxis
                dataKey="day"
                stroke="#3f3f46"
                tick={{ fill: '#71717a', fontSize: 11, fontFamily: 'var(--font-dm-mono)' }}
                label={{ value: 'Days from today', position: 'insideBottom', offset: -8, fill: '#52525b', fontSize: 11 }}
              />
              <YAxis
                stroke="#3f3f46"
                tickFormatter={(v) => fmtUnits(v)}
                tick={{ fill: '#71717a', fontSize: 11, fontFamily: 'var(--font-dm-mono)' }}
              />
              <Tooltip content={<ChartTooltip />} cursor={{ stroke: '#F59E0B', strokeOpacity: 0.3 }} />
              <ReferenceLine
                x={dep.stockoutOffset}
                stroke="#EF4444"
                strokeWidth={1.5}
                label={{ value: 'Stockout', fill: '#EF4444', fontSize: 10, position: 'top', fontFamily: 'var(--font-dm-mono)' }}
              />
              <ReferenceLine
                x={dep.poOffset}
                stroke="#3B82F6"
                strokeDasharray="4 4"
                strokeWidth={1.5}
                label={{ value: 'PO arrives', fill: '#3B82F6', fontSize: 10, position: 'top', fontFamily: 'var(--font-dm-mono)' }}
              />
              <Line
                type="monotone"
                dataKey="units"
                stroke="#F59E0B"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: '#F59E0B', stroke: '#0A0A0B', strokeWidth: 2 }}
              />
            </LineChart>
          </div>
        </section>

        {/* Action comparison */}
        <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2">
          <div className="rounded-md border border-neutral-900 bg-[#0D0D0F] p-6" style={{ boxShadow: 'inset 3px 0 0 0 #F59E0B' }}>
            <div className="flex items-center justify-between">
              <div className="text-[11px] uppercase tracking-[0.18em] text-[#F59E0B]">Transfer Now</div>
              <RiskPill risk="HIGH" />
            </div>
            <div className="mono mt-3 text-4xl font-medium text-neutral-50">
              {fmtMoney(event.transfer_cost)}
            </div>
            <div className="text-[11px] uppercase tracking-[0.18em] text-neutral-500 mt-1">freight cost</div>
            <div className="mt-5 divide-y divide-neutral-900 text-sm">
              <Row k="Arrival estimate" v={`~${event.transit_days} days transit`} />
              <Row k={`Post-transfer ${event.dest_dc} stock`} v={`${postTransferDays} days of supply`} vClass={supplyColor(postTransferDays)} />
              <Row k="Risk eliminated" v={`${fmtMoney(event.penalty_exposure)} penalty avoided`} vClass="text-[#22C55E]" />
              <Row k="Transfer qty" v={`${fmtUnits(event.transfer_qty)} units`} />
            </div>
          </div>
          <div className="rounded-md border border-neutral-900 bg-[#0D0D0F] p-6">
            <div className="flex items-center justify-between">
              <div className="text-[11px] uppercase tracking-[0.18em] text-neutral-400">Wait for PO</div>
              <span className="mono rounded-full bg-neutral-800 px-2 py-0.5 text-[10px] text-neutral-400 tracking-widest">PASSIVE</span>
            </div>
            <div className="mono mt-3 text-4xl font-medium text-neutral-300">
              {fmtDate(event.po_eta)}
            </div>
            <div className="text-[11px] uppercase tracking-[0.18em] text-neutral-500 mt-1">
              PO ETA · {Math.max(0, daysBetween(new Date().toISOString(), event.po_eta))} days
            </div>
            <div className="mt-5 divide-y divide-neutral-900 text-sm">
              <Row
                k="Stockout before arrival"
                v={stockoutBeforePO > 0 ? `YES — ${stockoutBeforePO} days before PO arrives` : 'No — PO arrives in time'}
                vClass={stockoutBeforePO > 0 ? 'text-[#EF4444]' : 'text-[#22C55E]'}
              />
              <Row k="Penalty exposure" v={`${fmtMoney(event.penalty_exposure)} (${event.confidence} confidence)`} vClass="text-[#F59E0B]" />
              <Row k="Inbound units" v={`${fmtUnits(event.po_units)} units`} />
              <Row k="Lost margin (est.)" v={fmtMoney(Math.round(event.penalty_exposure * 0.4))} vClass="text-neutral-300" />
            </div>
          </div>
        </div>

        {/* Claude recommendation */}
        <section className="mt-6 rounded-md border border-neutral-900 bg-[#0D0D0F]" style={{ boxShadow: 'inset 0 3px 0 0 #F59E0B' }}>
          <div className="p-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2 text-[11px] uppercase tracking-[0.18em] text-[#F59E0B]">
                <Sparkles className="h-3.5 w-3.5" /> AI Recommendation
              </div>
              <div className="flex items-center gap-2">
                <span className="mono rounded-full bg-[#F59E0B]/10 px-2 py-0.5 text-[10px] tracking-widest text-[#F59E0B] ring-1 ring-inset ring-[#F59E0B]/30">
                  {event.action}
                </span>
                <span className="mono rounded-full bg-neutral-800 px-2 py-0.5 text-[10px] tracking-widest text-neutral-300 ring-1 ring-inset ring-neutral-700">
                  {event.confidence}
                </span>
              </div>
            </div>
            <p className="mt-3 text-sm leading-relaxed text-neutral-200">
              {event.reasoning}
            </p>
            <div className="mt-5">
              <button
                onClick={() => {
                  setAnalyzing(true)
                  setTimeout(() => setAnalyzing(false), 1600)
                }}
                disabled={analyzing}
                className="mono inline-flex items-center gap-2 rounded-md border border-neutral-800 bg-neutral-900 px-3 py-1.5 text-xs text-neutral-200 tracking-widest hover:border-[#F59E0B]/60 hover:text-[#F59E0B] disabled:opacity-60"
              >
                {analyzing ? (
                  <><Loader2 className="h-3.5 w-3.5 animate-spin" /> ANALYZING…</>
                ) : (
                  <>⟳ TRIGGER ANALYSIS</>
                )}
              </button>
            </div>
          </div>
        </section>

        {/* CTAs */}
        <div className="mt-8 flex flex-wrap items-center gap-3 pb-12">
          <button
            onClick={() => {
              setTransferCreated(true)
              setTimeout(() => router.push('/approvals'), 700)
            }}
            className="mono rounded-md bg-[#F59E0B] px-5 py-2.5 text-xs font-medium tracking-widest text-neutral-950 hover:bg-[#F59E0B]/90"
          >
            {transferCreated ? '✓ TRANSFER REQUEST CREATED' : 'CREATE TRANSFER REQUEST'}
          </button>
          <button
            onClick={() => setMonitoring((m) => !m)}
            className="mono rounded-md border border-neutral-800 bg-transparent px-5 py-2.5 text-xs tracking-widest text-neutral-300 hover:border-neutral-600 hover:text-neutral-100"
          >
            {monitoring ? '✓ MARKED AS MONITORING' : 'MARK AS MONITORING'}
          </button>
          <Link href="/" className="ml-auto text-xs text-neutral-500 hover:text-neutral-300">
            ← Back to dashboard
          </Link>
        </div>
      </main>
    </div>
  )
}

function Row({ k, v, vClass }) {
  return (
    <div className="flex items-center justify-between py-2.5">
      <span className="text-xs uppercase tracking-widest text-neutral-500">{k}</span>
      <span className={`mono text-sm text-neutral-200 ${vClass || ''}`}>{v}</span>
    </div>
  )
}

function DCCard({ c, dest, source }) {
  const info = DC_INFO[c.code]
  let borderCls = 'border-neutral-900'
  let accent = null
  if (c.code === dest) {
    borderCls = 'border-[#EF4444]/50'
    accent = (
      <span className="mono inline-flex items-center gap-1 rounded-full bg-[#EF4444]/10 px-2 py-0.5 text-[10px] tracking-widest text-[#EF4444] ring-1 ring-inset ring-[#EF4444]/30">
        ● CRITICAL
      </span>
    )
  } else if (c.code === source) {
    borderCls = 'border-[#22C55E]/40'
    accent = (
      <span className="mono inline-flex items-center gap-1 rounded-full bg-[#22C55E]/10 px-2 py-0.5 text-[10px] tracking-widest text-[#22C55E] ring-1 ring-inset ring-[#22C55E]/30">
        SOURCE
      </span>
    )
  }
  return (
    <div className={`rounded-md border bg-[#0D0D0F] p-5 ${borderCls}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <DCBadge code={c.code} />
          <div className="text-sm text-neutral-300">— {info.city}</div>
        </div>
        {accent}
      </div>
      <div className="mono mt-4 text-4xl font-medium text-neutral-50">
        {fmtUnits(c.units)}
      </div>
      <div className="text-[11px] uppercase tracking-[0.18em] text-neutral-500">
        available units
      </div>
      <div className={`mono mt-3 text-sm ${supplyColor(c.days)}`}>
        {c.days} days of supply
      </div>
      <div className="mt-1 text-xs text-neutral-500">
        {c.po
          ? `PO arriving ${fmtDate(c.po.eta)} · ${fmtUnits(c.po.units)} units`
          : 'No inbound PO'}
      </div>
    </div>
  )
}

function unitsFor(ev, dc) {
  return dc === 'SF' ? ev.sf_units : dc === 'NJ' ? ev.nj_units : ev.la_units
}
function daysFor(ev, dc) {
  const u = unitsFor(ev, dc)
  const burn = dc === 'SF' ? ev.sf_daily_burn : dc === 'NJ' ? ev.nj_daily_burn : ev.la_daily_burn
  return Math.round(u / burn)
}
function poFor(ev, dc) {
  return ev.po_dc === dc ? { eta: ev.po_eta, units: ev.po_units } : null
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const p = payload[0].payload
  return (
    <div className="rounded-md border border-neutral-800 bg-[#0A0A0B] px-3 py-2 shadow-lg">
      <div className="mono text-[11px] text-neutral-400">Day {label} · {fmtDate(p.date)}</div>
      <div className="mono text-sm font-medium text-[#F59E0B]">{fmtUnits(p.units)} units</div>
    </div>
  )
}
