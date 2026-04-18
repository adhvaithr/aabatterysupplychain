'use client'

import { Fragment, useEffect, useMemo, useState } from 'react'
import Link from 'next/link'
import { Check, X, ArrowRight, Search } from 'lucide-react'
import Nav from '../../components/nav'
import { StateBadge, DCBadge } from '../../components/badges'
import { approveTransferRequest, getApprovalQueue, rejectTransferRequest } from '../../lib/api'
import { fmtMoney, fmtUnits } from '../../lib/format'

export default function ApprovalsPage() {
  const [data, setData] = useState([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState('')
  const [expandedId, setExpandedId] = useState(null)
  const [rejectionReason, setRejectionReason] = useState('')
  const [justApproved, setJustApproved] = useState(null)
  const [busyId, setBusyId] = useState(null)
  const [query, setQuery] = useState('')
  const [routeFilter, setRouteFilter] = useState('ALL')
  const [selectedIds, setSelectedIds] = useState([])
  const [batchBusy, setBatchBusy] = useState(false)
  const [batchRejectMode, setBatchRejectMode] = useState(false)
  const [batchReason, setBatchReason] = useState('')

  useEffect(() => {
    let cancelled = false

    async function load() {
      setIsLoading(true)
      setError('')
      try {
        const queue = await getApprovalQueue()
        if (!cancelled) setData(Array.isArray(queue) ? queue : [])
      } catch (err) {
        if (!cancelled) setError(err.message || 'Failed to load approval queue.')
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    }

    load()
    return () => {
      cancelled = true
    }
  }, [])

  const effective = data ?? []

  const pendingCount = useMemo(
    () => effective.filter((r) => r.state === 'PENDING_APPROVAL').length,
    [effective]
  )

  const filtered = useMemo(() => {
    const search = query.trim().toLowerCase()
    return effective.filter((row) => {
      const matchesSearch =
        !search ||
        [
          row.sku_id,
          row.event_id,
          row.source_dc,
          row.dest_dc,
          row.state,
        ]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(search))

      const route = `${row.source_dc}->${row.dest_dc}`
      const matchesRoute = routeFilter === 'ALL' || route === routeFilter
      return matchesSearch && matchesRoute
    })
  }, [effective, query, routeFilter])

  const routeOptions = useMemo(
    () => ['ALL', ...new Set(effective.map((row) => `${row.source_dc}->${row.dest_dc}`))],
    [effective]
  )

  const selectableIds = useMemo(
    () => filtered.filter((row) => row.state === 'PENDING_APPROVAL').map((row) => row.id),
    [filtered]
  )

  const allSelected = selectableIds.length > 0 && selectableIds.every((id) => selectedIds.includes(id))

  const approve = async (id) => {
    setBusyId(id)
    setError('')
    try {
      const updated = await approveTransferRequest(id)
      setData((prev) => prev.map((r) => (r.id === id ? { ...r, ...updated } : r)))
      setSelectedIds((prev) => prev.filter((item) => item !== id))
      setJustApproved(id)
      setTimeout(() => setJustApproved(null), 900)
    } catch (err) {
      setError(err.message || 'Approve failed.')
    } finally {
      setBusyId(null)
    }
  }

  const toggleSelected = (id) => {
    setSelectedIds((prev) => (prev.includes(id) ? prev.filter((item) => item !== id) : [...prev, id]))
  }

  const toggleSelectAll = () => {
    setSelectedIds((prev) => (allSelected ? prev.filter((id) => !selectableIds.includes(id)) : Array.from(new Set([...prev, ...selectableIds]))))
  }

  const batchApprove = async () => {
    if (!selectedIds.length) return
    setBatchBusy(true)
    setError('')
    const results = await Promise.allSettled(selectedIds.map((id) => approveTransferRequest(id)))
    const successes = new Map()
    let failed = 0
    results.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        successes.set(selectedIds[index], result.value)
      } else {
        failed += 1
      }
    })
    if (successes.size) {
      setData((prev) => prev.map((row) => (successes.has(row.id) ? { ...row, ...successes.get(row.id) } : row)))
      setJustApproved(selectedIds[0] || null)
      setTimeout(() => setJustApproved(null), 900)
    }
    setSelectedIds((prev) => prev.filter((id) => !successes.has(id)))
    if (failed) {
      setError(`${failed} batch approval request${failed === 1 ? '' : 's'} failed. The rest were updated.`)
    }
    setBatchBusy(false)
  }

  const batchReject = async () => {
    if (!selectedIds.length || !batchReason.trim()) return
    setBatchBusy(true)
    setError('')
    const results = await Promise.allSettled(selectedIds.map((id) => rejectTransferRequest(id, batchReason)))
    const successes = new Map()
    let failed = 0
    results.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        successes.set(selectedIds[index], result.value)
      } else {
        failed += 1
      }
    })
    if (successes.size) {
      setData((prev) => prev.map((row) => (successes.has(row.id) ? { ...row, ...successes.get(row.id) } : row)))
    }
    setSelectedIds((prev) => prev.filter((id) => !successes.has(id)))
    setBatchRejectMode(false)
    setBatchReason('')
    if (failed) {
      setError(`${failed} batch rejection request${failed === 1 ? '' : 's'} failed. The rest were updated.`)
    }
    setBatchBusy(false)
  }

  const openReject = (id) => {
    setExpandedId(id)
    setRejectionReason('')
  }

  const confirmReject = async (id) => {
    setBusyId(id)
    setError('')
    try {
      const updated = await rejectTransferRequest(id, rejectionReason)
      setData((prev) => prev.map((r) => (r.id === id ? { ...r, ...updated } : r)))
      setSelectedIds((prev) => prev.filter((item) => item !== id))
      setExpandedId(null)
      setRejectionReason('')
    } catch (err) {
      setError(err.message || 'Reject failed.')
    } finally {
      setBusyId(null)
    }
  }

  const cancelReject = () => {
    setExpandedId(null)
    setRejectionReason('')
  }

  return (
    <div className="min-h-screen">
      <Nav />
      <main className="page-fade mx-auto max-w-[1400px] px-6 py-8">
        <div className="mb-6 flex items-end justify-between">
          <div>
            <h1 className="text-2xl font-medium text-[hsl(var(--app-text-strong))]">Approval Queue</h1>
            <p className="mt-1 text-sm text-[hsl(var(--app-text-soft))]">
              Review pending transfers ranked by highest expected penalty cost first.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Pending</span>
            <span
              className={`mono inline-flex min-w-8 items-center justify-center rounded-full px-2 py-0.5 text-xs tracking-widest ring-1 ring-inset ${pendingCount > 0 ? 'bg-[#F59E0B]/10 text-[#F59E0B] ring-[#F59E0B]/30' : 'bg-secondary text-[hsl(var(--app-text-muted))] ring-border'}`}
            >
              {pendingCount}
            </span>
          </div>
        </div>

        <div className="mb-4 rounded-md border border-border bg-[hsl(var(--app-panel))] p-4 transition-colors">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <label className="relative block w-full md:max-w-md">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[hsl(var(--app-text-muted))]" />
              <input
                type="search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search SKU, event ID, route, or state"
                className="w-full rounded-md border border-border bg-background px-10 py-2.5 text-sm text-foreground placeholder:text-[hsl(var(--app-text-muted))] focus:border-[#F59E0B]/60 focus:outline-none"
              />
            </label>
            <div className="flex flex-wrap items-center gap-3">
              <select
                value={routeFilter}
                onChange={(e) => setRouteFilter(e.target.value)}
                className="rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground focus:border-[#F59E0B]/60 focus:outline-none"
              >
                {routeOptions.map((route) => (
                  <option key={route} value={route}>
                    {route === 'ALL' ? 'All routes' : route.replace('->', ' to ')}
                  </option>
                ))}
              </select>
              <div className="mono text-xs text-[hsl(var(--app-text-muted))]">
                Showing {filtered.length} of {effective.length}
              </div>
            </div>
          </div>

          <div className="mt-4 rounded-md border border-dashed border-border bg-[hsl(var(--app-panel-muted))] p-4 transition-colors">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <div className="mono text-[11px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">Batch actions</div>
                <div className="mt-1 text-sm text-[hsl(var(--app-text-soft))]">
                  {selectedIds.length ? `${selectedIds.length} request${selectedIds.length === 1 ? '' : 's'} selected.` : 'Select one or more pending requests to approve or reject in bulk.'}
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <button
                  onClick={toggleSelectAll}
                  disabled={!selectableIds.length}
                  className="mono rounded-md border border-border px-3 py-2 text-[11px] tracking-widest text-[hsl(var(--app-text-soft))] hover:text-[hsl(var(--app-text-strong))] disabled:opacity-40"
                >
                  {allSelected ? 'CLEAR PAGE' : 'SELECT PAGE'}
                </button>
                <button
                  onClick={batchApprove}
                  disabled={!selectedIds.length || batchBusy}
                  className="mono inline-flex items-center gap-1 rounded-md bg-[#22C55E]/15 px-3 py-2 text-[11px] font-medium tracking-widest text-[#22C55E] ring-1 ring-inset ring-[#22C55E]/30 hover:bg-[#22C55E]/25 disabled:opacity-40"
                >
                  <Check className="h-3 w-3" /> APPROVE SELECTED
                </button>
                <button
                  onClick={() => setBatchRejectMode((prev) => !prev)}
                  disabled={!selectedIds.length || batchBusy}
                  className="mono inline-flex items-center gap-1 rounded-md px-3 py-2 text-[11px] font-medium tracking-widest text-[#EF4444] hover:bg-[#EF4444]/10 disabled:opacity-40"
                >
                  <X className="h-3 w-3" /> REJECT SELECTED
                </button>
              </div>
            </div>
            {batchRejectMode && (
              <div className="mt-3 flex flex-col gap-3 md:flex-row md:items-center">
                <input
                  value={batchReason}
                  onChange={(e) => setBatchReason(e.target.value)}
                  placeholder="Shared rejection reason for selected requests"
                  className="mono flex-1 rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-[hsl(var(--app-text-muted))] focus:border-[#EF4444]/60 focus:outline-none"
                />
                <button
                  onClick={batchReject}
                  disabled={!batchReason.trim() || !selectedIds.length || batchBusy}
                  className="mono rounded-md bg-[#EF4444] px-4 py-2 text-[11px] font-medium tracking-widest text-neutral-950 hover:bg-[#EF4444]/90 disabled:opacity-40"
                >
                  CONFIRM BATCH REJECT
                </button>
              </div>
            )}
          </div>
        </div>

        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="shimmer h-16 w-full rounded-md" />
            ))}
          </div>
        ) : error ? (
          <div className="rounded-md border border-red-900/50 bg-red-950/20 px-4 py-3 text-sm text-red-300">
            {error}
          </div>
        ) : filtered.length === 0 ? (
          <EmptyState />
        ) : (
          <div className="overflow-hidden rounded-md border border-border bg-[hsl(var(--app-panel))] transition-colors">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border bg-[hsl(var(--app-panel-muted))] text-[10px] uppercase tracking-[0.18em] text-[hsl(var(--app-text-muted))]">
                  <th className="w-12 px-4 py-3 text-left">
                    <input
                      type="checkbox"
                      checked={allSelected}
                      onChange={toggleSelectAll}
                      aria-label="Select all visible pending requests"
                      className="h-4 w-4 rounded border-border bg-background accent-[#F59E0B]"
                    />
                  </th>
                  <th className="px-5 py-3 text-left">SKU / Event</th>
                  <th className="px-5 py-3 text-left">Route</th>
                  <th className="px-5 py-3 text-left">Qty</th>
                  <th className="px-5 py-3 text-left">Freight</th>
                  <th className="px-5 py-3 text-left">Penalty risk</th>
                  <th className="px-5 py-3 text-left">Submitted</th>
                  <th className="px-5 py-3 text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((r) => {
                  const isMuted = r.state !== 'PENDING_APPROVAL'
                  return (
                    <Fragment key={r.id}>
                      <tr
                        className={`border-b border-border transition ${isMuted ? 'opacity-55' : 'hover:bg-[hsl(var(--app-hover))]'} ${justApproved === r.id ? 'bg-emerald-500/5' : ''}`}
                      >
                        <td className="px-4 py-4">
                          <input
                            type="checkbox"
                            checked={selectedIds.includes(r.id)}
                            onChange={() => toggleSelected(r.id)}
                            disabled={r.state !== 'PENDING_APPROVAL' || batchBusy}
                            aria-label={`Select request ${r.id}`}
                            className="h-4 w-4 rounded border-border bg-background accent-[#F59E0B]"
                          />
                        </td>
                        <td className="px-5 py-4">
                          <Link href={`/events/${r.event_id}`} className="block">
                            <div className="mono text-sm font-medium text-[hsl(var(--app-text-strong))] hover:text-[#F59E0B]">{r.sku_id}</div>
                            <div className="text-xs text-[hsl(var(--app-text-muted))]">Event #{r.event_id}</div>
                          </Link>
                        </td>
                        <td className="px-5 py-4">
                          <div className="mono inline-flex items-center gap-2 text-sm text-[hsl(var(--app-text-soft))]">
                            <DCBadge code={r.source_dc} />
                            <ArrowRight className="h-3.5 w-3.5 text-[hsl(var(--app-text-muted))]" />
                            <DCBadge code={r.dest_dc} />
                          </div>
                        </td>
                        <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-strong))]">{fmtUnits(r.qty)}</td>
                        <td className="mono px-5 py-4 text-sm text-[hsl(var(--app-text-strong))]">{fmtMoney(r.estimated_cost)}</td>
                        <td className="mono px-5 py-4 text-sm text-[#22C55E]">
                          {fmtMoney(r.expected_penalty_cost)}
                        </td>
                        <td className="px-5 py-4 text-xs text-[hsl(var(--app-text-muted))]">
                          {r.created_at ? new Date(r.created_at).toLocaleString() : '—'}
                        </td>
                        <td className="px-5 py-4 text-right">
                          {r.state === 'PENDING_APPROVAL' ? (
                            <div className="flex justify-end gap-2">
                              <button
                                onClick={() => approve(r.id)}
                                disabled={busyId === r.id}
                                className="mono inline-flex items-center gap-1 rounded-full bg-[#22C55E]/15 px-3 py-1 text-[11px] font-medium tracking-widest text-[#22C55E] ring-1 ring-inset ring-[#22C55E]/30 hover:bg-[#22C55E]/25"
                              >
                                <Check className="h-3 w-3" /> APPROVE
                              </button>
                              <button
                                onClick={() => openReject(r.id)}
                                disabled={busyId === r.id}
                                className="mono inline-flex items-center gap-1 rounded-full px-3 py-1 text-[11px] font-medium tracking-widest text-[#EF4444] hover:bg-[#EF4444]/10"
                              >
                                <X className="h-3 w-3" /> REJECT
                              </button>
                            </div>
                          ) : (
                            <div className="flex justify-end">
                              <StateBadge state={r.state} />
                              {justApproved === r.id && (
                                <span className="ml-2 animate-pulse text-[#22C55E]">✓</span>
                              )}
                            </div>
                          )}
                        </td>
                      </tr>
                      {expandedId === r.id && (
                        <tr className="border-b border-border bg-red-950/10">
                          <td colSpan={8} className="px-5 py-4">
                            <div className="flex flex-wrap items-center gap-3">
                              <span className="text-[11px] uppercase tracking-[0.18em] text-[#EF4444]">Reject transfer</span>
                              <input
                                autoFocus
                                value={rejectionReason}
                                onChange={(e) => setRejectionReason(e.target.value)}
                                placeholder="Reason for rejection…"
                                className="mono min-w-64 flex-1 rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-[hsl(var(--app-text-muted))] focus:border-[#EF4444]/60 focus:outline-none"
                              />
                              <button
                                onClick={() => confirmReject(r.id)}
                                disabled={!rejectionReason.trim() || busyId === r.id}
                                className="mono rounded-md bg-[#EF4444] px-4 py-2 text-[11px] font-medium tracking-widest text-neutral-950 hover:bg-[#EF4444]/90 disabled:opacity-40"
                              >
                                CONFIRM REJECT
                              </button>
                              <button
                                onClick={cancelReject}
                                className="mono rounded-md border border-border px-4 py-2 text-[11px] tracking-widest text-[hsl(var(--app-text-soft))] hover:text-[hsl(var(--app-text-strong))]"
                              >
                                CANCEL
                              </button>
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </main>
    </div>
  )
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center rounded-md border border-border bg-[hsl(var(--app-panel))] py-24 transition-colors">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-[#22C55E]/10 ring-1 ring-inset ring-[#22C55E]/30">
        <Check className="h-5 w-5 text-[#22C55E]" />
      </div>
      <div className="mt-4 text-base text-[hsl(var(--app-text-strong))]">No matching transfers · All caught up</div>
      <div className="mt-1 text-xs text-[hsl(var(--app-text-muted))]">Pending requests that match your filters will appear here automatically</div>
    </div>
  )
}
