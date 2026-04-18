export const STATE_STYLES = {
  DETECTED: { bg: 'bg-secondary', fg: 'text-[hsl(var(--app-text-soft))]', ring: 'ring-border' },
  ANALYZING: { bg: 'bg-blue-500/10', fg: 'text-blue-300', ring: 'ring-blue-500/30' },
  ACTION_PROPOSED: { bg: 'bg-amber-500/10', fg: 'text-amber-300', ring: 'ring-amber-500/30' },
  PENDING_APPROVAL: { bg: 'bg-orange-500/10', fg: 'text-orange-300', ring: 'ring-orange-500/30' },
  APPROVED: { bg: 'bg-emerald-500/10', fg: 'text-emerald-300', ring: 'ring-emerald-500/30' },
  REJECTED: { bg: 'bg-red-500/10', fg: 'text-red-300', ring: 'ring-red-500/30' },
  EXECUTED: { bg: 'bg-teal-500/10', fg: 'text-teal-300', ring: 'ring-teal-500/30' },
  RESOLVED: { bg: 'bg-secondary/80', fg: 'text-[hsl(var(--app-text-muted))]', ring: 'ring-border' },
}

export function StateBadge({ state, size = 'sm' }) {
  const s = STATE_STYLES[state] || STATE_STYLES.DETECTED
  const p = size === 'lg' ? 'px-3 py-1 text-xs' : 'px-2 py-0.5 text-[10px]'
  return (
    <span
      className={`mono inline-flex items-center rounded-full ring-1 ring-inset tracking-wider ${p} ${s.bg} ${s.fg} ${s.ring}`}
    >
      {state}
    </span>
  )
}

export const RISK_STYLES = {
  CRITICAL: 'bg-[#EF4444]/12 text-[#EF4444] ring-[#EF4444]/30',
  HIGH: 'bg-[#F59E0B]/12 text-[#F59E0B] ring-[#F59E0B]/30',
  MEDIUM: 'bg-yellow-500/10 text-yellow-300 ring-yellow-500/30',
  LOW: 'bg-[#22C55E]/10 text-[#22C55E] ring-[#22C55E]/30',
}

export function RiskPill({ risk }) {
  const cls = RISK_STYLES[risk] || RISK_STYLES.LOW
  return (
    <span
      className={`mono inline-flex items-center rounded-full px-2 py-0.5 text-[10px] tracking-widest ring-1 ring-inset ${cls}`}
    >
      {risk}
    </span>
  )
}

export const DC_PILL = {
  SF: 'bg-sky-500/8 text-sky-300 ring-sky-500/20',
  NJ: 'bg-purple-500/8 text-purple-300 ring-purple-500/20',
  LA: 'bg-rose-500/8 text-rose-300 ring-rose-500/20',
}

export function DCBadge({ code }) {
  return (
    <span
      className={`mono inline-flex items-center rounded-md px-2 py-0.5 text-[11px] tracking-wider ring-1 ring-inset ${DC_PILL[code] || 'bg-secondary text-[hsl(var(--app-text-soft))] ring-border'}`}
    >
      {code}
    </span>
  )
}

export function ActionText({ action }) {
  if (action === 'TRANSFER')
    return <span className="mono text-[11px] tracking-widest text-[#F59E0B]">TRANSFER</span>
  if (action === 'WAIT')
    return <span className="mono text-[11px] tracking-widest text-emerald-500 dark:text-emerald-400">WAIT</span>
  return <span className="mono text-[11px] tracking-widest text-[hsl(var(--app-text-muted))]">MONITOR</span>
}
