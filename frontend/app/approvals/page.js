'use client'

import { Fragment, useEffect, useMemo, useState } from 'react'
import { Check, X, ArrowRight } from 'lucide-react'
import Nav from '../../components/nav'
import { StateBadge, DCBadge } from '../../components/badges'
import { fmtMoney, fmtUnits } from '../../lib/format'
import { getApprovals } from '../../lib/mock-data'

export default function ApprovalsPage() {
  const [data, setData] = useState(() => getApprovals())
  const isLoading = false

  useEffect(() => {
    setData(getApprovals())
  }, [])

  const [rows, setRows] = useState(null)
  const [expandedId, setExpandedId] = useState(null)
  const [rejectionReason, setRejectionReason] = useState('')
  const [justApproved, setJustApproved] = useState(null)

  const effective = rows ?? data ?? []

  const pendingCount = useMemo(
    () => effective.filter((r) => r.state === 'PENDING_APPROVAL').length,
    [effective]
  )

  const ensureLocal = () => {
    if (!rows) setRows(data ? [...data] : [])
  }

  const approve = (id) => {
    ensureLocal()
    setRows((prev) => {
      const base = prev ?? [...(data || [])]
      return base.map((r) => (r.id === id ? { ...r, state: 'APPROVED' } : r))
    })
    setJustApproved(id)
    setTimeout(() => setJustApproved(null), 900)
  }

  const openReject = (id) => {
    setExpandedId(id)
    setRejectionReason('')
  }

  const confirmReject = (id) => {
    ensureLocal()
    setRows((prev) => {
      const base = prev ?? [...(data || [])]
      return base.map((r) =>
        r.id === id ? { ...r, state: 'REJECTED', rejection_reason: rejectionReason } : r
      )
    })
    setExpandedId(null)
    setRejectionReason('')
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
            <h1 className="text-2xl font-medium text-neutral-50">Approval Queue</h1>
            <p className="mt-1 text-sm text-neutral-500">
              Review and action proposed transfers. Optimistic updates — no page reload.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-[0.18em] text-neutral-500">Pending</span>
            <span
              className={`mono inline-flex min-w-8 items-center justify-center rounded-full px-2 py-0.5 text-xs tracking-widest ring-1 ring-inset ${pendingCount > 0 ? 'bg-[#F59E0B]/10 text-[#F59E0B] ring-[#F59E0B]/30' : 'bg-neutral-800 text-neutral-500 ring-neutral-700'}`}
            >
              {pendingCount}
            </span>
          </div>
        </div>

        {isLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="shimmer h-16 w-full rounded-md" />
            ))}
          </div>
        ) : effective.length === 0 ? (
          <EmptyState />
        ) : (
          <div className="overflow-hidden rounded-md border border-neutral-900 bg-[#0D0D0F]">
            <table className="w-full">
              <thead>
                <tr className="border-b border-neutral-900 bg-[#101013] text-[10px] uppercase tracking-[0.18em] text-neutral-500">
                  <th className="px-5 py-3 text-left">SKU / Product</th>
                  <th className="px-5 py-3 text-left">Route</th>
                  <th className="px-5 py-3 text-left">Qty</th>
                  <th className="px-5 py-3 text-left">Freight</th>
                  <th className="px-5 py-3 text-left">Penalty avoided</th>
                  <th className="px-5 py-3 text-left">Submitted</th>
                  <th className="px-5 py-3 text-right">Actions</th>
                </tr>
              </thead>
              <tbody>
                {effective.map((r) => {
                  const isMuted = r.state !== 'PENDING_APPROVAL'
                  return (
                    <Fragment key={r.id}>
                      <tr
                        className={`border-b border-neutral-900 transition ${isMuted ? 'opacity-55' : 'hover:bg-[#131316]'} ${justApproved === r.id ? 'bg-emerald-500/5' : ''}`}
                      >
                        <td className="px-5 py-4">
                          <div className="mono text-sm font-medium text-neutral-50">{r.sku}</div>
                          <div className="text-xs text-neutral-500">{r.product}</div>
                        </td>
                        <td className="px-5 py-4">
                          <div className="mono inline-flex items-center gap-2 text-sm text-neutral-300">
                            <DCBadge code={r.source_dc} />
                            <ArrowRight className="h-3.5 w-3.5 text-neutral-600" />
                            <DCBadge code={r.dest_dc} />
                          </div>
                        </td>
                        <td className="mono px-5 py-4 text-sm text-neutral-200">{fmtUnits(r.transfer_qty)}</td>
                        <td className="mono px-5 py-4 text-sm text-neutral-200">{fmtMoney(r.transfer_cost)}</td>
                        <td className="mono px-5 py-4 text-sm text-[#22C55E]">{fmtMoney(r.penalty_exposure)}</td>
                        <td className="px-5 py-4 text-xs text-neutral-500">{r.submitted_ago}</td>
                        <td className="px-5 py-4 text-right">
                          {r.state === 'PENDING_APPROVAL' ? (
                            <div className="flex justify-end gap-2">
                              <button
                                onClick={() => approve(r.id)}
                                className="mono inline-flex items-center gap-1 rounded-full bg-[#22C55E]/15 px-3 py-1 text-[11px] font-medium tracking-widest text-[#22C55E] ring-1 ring-inset ring-[#22C55E]/30 hover:bg-[#22C55E]/25"
                              >
                                <Check className="h-3 w-3" /> APPROVE
                              </button>
                              <button
                                onClick={() => openReject(r.id)}
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
                        <tr className="border-b border-neutral-900 bg-[#120F0F]">
                          <td colSpan={7} className="px-5 py-4">
                            <div className="flex flex-wrap items-center gap-3">
                              <span className="text-[11px] uppercase tracking-[0.18em] text-[#EF4444]">Reject transfer</span>
                              <input
                                autoFocus
                                value={rejectionReason}
                                onChange={(e) => setRejectionReason(e.target.value)}
                                placeholder="Reason for rejection…"
                                className="mono flex-1 min-w-64 rounded-md border border-neutral-800 bg-[#0A0A0B] px-3 py-2 text-sm text-neutral-200 placeholder:text-neutral-600 focus:border-[#EF4444]/60 focus:outline-none"
                              />
                              <button
                                onClick={() => confirmReject(r.id)}
                                disabled={!rejectionReason.trim()}
                                className="mono rounded-md bg-[#EF4444] px-4 py-2 text-[11px] font-medium tracking-widest text-neutral-950 hover:bg-[#EF4444]/90 disabled:opacity-40"
                              >
                                CONFIRM REJECT
                              </button>
                              <button
                                onClick={cancelReject}
                                className="mono rounded-md border border-neutral-800 px-4 py-2 text-[11px] tracking-widest text-neutral-400 hover:text-neutral-200"
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
    <div className="flex flex-col items-center justify-center rounded-md border border-neutral-900 bg-[#0D0D0F] py-24">
      <div className="flex h-12 w-12 items-center justify-center rounded-full bg-[#22C55E]/10 ring-1 ring-inset ring-[#22C55E]/30">
        <Check className="h-5 w-5 text-[#22C55E]" />
      </div>
      <div className="mt-4 text-base text-neutral-200">No pending transfers · All caught up</div>
      <div className="mt-1 text-xs text-neutral-500">New risk events will appear here automatically</div>
    </div>
  )
}
