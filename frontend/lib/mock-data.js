// Mock data for PoP Sentinel dashboard

export const DC_INFO = {
  SF: { code: 'SF', name: 'SF', city: 'Livermore' },
  NJ: { code: 'NJ', name: 'NJ', city: 'Newark' },
  LA: { code: 'LA', name: 'LA', city: 'Los Angeles' },
}

export const EVENTS = [
  {
    id: 'evt_01',
    sku: 'AC-B3SLJ',
    product: 'Ginseng Slices',
    category: 'Herbal Supplements',
    dest_dc: 'NJ',
    source_dc: 'SF',
    third_dc: 'LA',
    days_of_supply: 8,
    stockout_date: '2026-04-26',
    risk: 'CRITICAL',
    action: 'TRANSFER',
    state: 'ACTION_PROPOSED',
    nj_units: 1240,
    sf_units: 8760,
    la_units: 3420,
    nj_daily_burn: 155,
    sf_daily_burn: 70,
    la_daily_burn: 95,
    po_eta: '2026-05-03',
    po_units: 2400,
    po_dc: 'NJ',
    transfer_qty: 1800,
    transfer_cost: 3186,
    penalty_exposure: 1020,
    transit_days: 3,
    confidence: 'HIGH',
    reasoning:
      'NJ DC will stock out 6 days before the inbound PO arrives on May 3. SF holds 8,760 units (125 days of supply) — well above buffer thresholds. A same-day transfer of 1,800 units from SF → NJ eliminates the stockout window and avoids the $1,020 contractual penalty. Freight cost ($3,186) is reasonable relative to the penalty plus margin loss from empty shelves at two major East Coast retailers.',
    submitted_ago: '2 hours ago',
  },
  {
    id: 'evt_02',
    sku: 'GC-M2R7T',
    product: 'Ginger Chews — Mango',
    category: 'Digestive Aids',
    dest_dc: 'SF',
    source_dc: 'LA',
    third_dc: 'NJ',
    days_of_supply: 5,
    stockout_date: '2026-04-23',
    risk: 'CRITICAL',
    action: 'TRANSFER',
    state: 'PENDING_APPROVAL',
    nj_units: 6100,
    sf_units: 560,
    la_units: 4820,
    nj_daily_burn: 80,
    sf_daily_burn: 112,
    la_daily_burn: 140,
    po_eta: '2026-05-08',
    po_units: 3000,
    po_dc: 'SF',
    transfer_qty: 1500,
    transfer_cost: 2640,
    penalty_exposure: 2150,
    transit_days: 2,
    confidence: 'HIGH',
    reasoning:
      'SF will deplete in 5 days. LA has surplus and is closest logistically. Transfer 1,500 units LA → SF via ground freight (2-day transit) to bridge the gap before the May 8 PO arrives. Critical SKU — top 20 revenue driver in the West region.',
    submitted_ago: '3 hours ago',
  },
  {
    id: 'evt_03',
    sku: 'TB-R4K8P',
    product: 'Tiger Balm — Red Extra Strength',
    category: 'Topical Analgesics',
    dest_dc: 'LA',
    source_dc: 'NJ',
    third_dc: 'SF',
    days_of_supply: 6,
    stockout_date: '2026-04-24',
    risk: 'CRITICAL',
    action: 'TRANSFER',
    state: 'PENDING_APPROVAL',
    nj_units: 5400,
    sf_units: 2100,
    la_units: 780,
    nj_daily_burn: 60,
    sf_daily_burn: 40,
    la_daily_burn: 130,
    po_eta: '2026-05-12',
    po_units: 2000,
    po_dc: 'LA',
    transfer_qty: 1200,
    transfer_cost: 4120,
    penalty_exposure: 1780,
    transit_days: 5,
    confidence: 'MEDIUM',
    reasoning:
      'LA has 6 days of cover; PO is 18 days out. Cross-country transfer is expensive ($4,120) but still cheaper than the $1,780 penalty plus downstream stockout risk at three anchor retailers. Consider air freight alternative if lead time becomes critical.',
    submitted_ago: '5 hours ago',
  },
  {
    id: 'evt_04',
    sku: 'GT-H5N9Q',
    product: 'Honey Ginseng Tea — 20ct',
    category: 'Functional Beverages',
    dest_dc: 'NJ',
    source_dc: 'SF',
    third_dc: 'LA',
    days_of_supply: 11,
    stockout_date: '2026-04-29',
    risk: 'HIGH',
    action: 'TRANSFER',
    state: 'ACTION_PROPOSED',
    nj_units: 2200,
    sf_units: 9400,
    la_units: 3100,
    nj_daily_burn: 200,
    sf_daily_burn: 85,
    la_daily_burn: 110,
    po_eta: '2026-05-10',
    po_units: 4000,
    po_dc: 'NJ',
    transfer_qty: 1600,
    transfer_cost: 2980,
    penalty_exposure: 850,
    transit_days: 3,
    confidence: 'HIGH',
    reasoning:
      'Borderline stockout — 11 days cover vs 11-day PO lead time. Small transfer recommended to create safety buffer and prevent any shelf gap during the Memorial Day promotional window.',
    submitted_ago: '6 hours ago',
  },
  {
    id: 'evt_05',
    sku: 'TB-W3X2L',
    product: 'Tiger Balm — White Regular',
    category: 'Topical Analgesics',
    dest_dc: 'SF',
    source_dc: 'NJ',
    third_dc: 'LA',
    days_of_supply: 13,
    stockout_date: '2026-05-01',
    risk: 'HIGH',
    action: 'TRANSFER',
    state: 'ANALYZING',
    nj_units: 7200,
    sf_units: 1820,
    la_units: 2400,
    nj_daily_burn: 55,
    sf_daily_burn: 140,
    la_daily_burn: 90,
    po_eta: '2026-05-18',
    po_units: 3200,
    po_dc: 'SF',
    transfer_qty: 1400,
    transfer_cost: 3820,
    penalty_exposure: 740,
    transit_days: 5,
    confidence: 'MEDIUM',
    reasoning:
      'Analyzing cross-country cost vs penalty tradeoff. Initial pass suggests transfer is marginally worth it; finalizing freight quotes.',
    submitted_ago: '8 hours ago',
  },
  {
    id: 'evt_06',
    sku: 'GC-L7Y4Z',
    product: 'Ginger Chews — Lychee',
    category: 'Digestive Aids',
    dest_dc: 'LA',
    source_dc: 'NJ',
    third_dc: 'SF',
    days_of_supply: 12,
    stockout_date: '2026-04-30',
    risk: 'HIGH',
    action: 'TRANSFER',
    state: 'DETECTED',
    nj_units: 5800,
    sf_units: 2700,
    la_units: 1560,
    nj_daily_burn: 50,
    sf_daily_burn: 60,
    la_daily_burn: 130,
    po_eta: '2026-05-14',
    po_units: 2800,
    po_dc: 'LA',
    transfer_qty: 1100,
    transfer_cost: 3420,
    penalty_exposure: 620,
    transit_days: 5,
    confidence: 'MEDIUM',
    reasoning:
      'Detected imbalance. Running full analysis — preliminary signal suggests transfer from NJ will avoid a thin stockout window before May 14 PO.',
    submitted_ago: '10 hours ago',
  },
  {
    id: 'evt_07',
    sku: 'AS-P6D1V',
    product: 'Ashwagandha Powder — 8oz',
    category: 'Herbal Supplements',
    dest_dc: 'NJ',
    source_dc: 'LA',
    third_dc: 'SF',
    days_of_supply: 18,
    stockout_date: '2026-05-06',
    risk: 'MEDIUM',
    action: 'MONITOR',
    state: 'DETECTED',
    nj_units: 2800,
    sf_units: 4100,
    la_units: 6200,
    nj_daily_burn: 155,
    sf_daily_burn: 70,
    la_daily_burn: 90,
    po_eta: '2026-05-05',
    po_units: 3500,
    po_dc: 'NJ',
    transfer_qty: 0,
    transfer_cost: 0,
    penalty_exposure: 320,
    transit_days: 3,
    confidence: 'MEDIUM',
    reasoning:
      'PO arrives 1 day before projected stockout. Monitor daily — if burn rate spikes >10%, escalate to TRANSFER.',
    submitted_ago: '1 day ago',
  },
  {
    id: 'evt_08',
    sku: 'TC-F9J3M',
    product: 'Turmeric Capsules — 120ct',
    category: 'Herbal Supplements',
    dest_dc: 'SF',
    source_dc: 'NJ',
    third_dc: 'LA',
    days_of_supply: 22,
    stockout_date: '2026-05-10',
    risk: 'MEDIUM',
    action: 'WAIT',
    state: 'RESOLVED',
    nj_units: 6400,
    sf_units: 3080,
    la_units: 4220,
    nj_daily_burn: 45,
    sf_daily_burn: 140,
    la_daily_burn: 95,
    po_eta: '2026-05-02',
    po_units: 4000,
    po_dc: 'SF',
    transfer_qty: 0,
    transfer_cost: 0,
    penalty_exposure: 0,
    transit_days: 0,
    confidence: 'HIGH',
    reasoning:
      'PO arrives May 2 — 8 days before projected stockout. Full coverage, no action needed.',
    submitted_ago: '1 day ago',
  },
]

export const HERO_STATS = {
  penalty_exposure: 47200,
  transfers_recommended: 8,
  critical: 3,
  high: 3,
  medium: 2,
}

export function getEvent(id) {
  return EVENTS.find((e) => e.id === id) || null
}

export function getApprovals() {
  return EVENTS.filter(
    (e) => e.state === 'PENDING_APPROVAL' || e.state === 'ACTION_PROPOSED'
  ).map((e) => ({
    id: e.id,
    sku: e.sku,
    product: e.product,
    source_dc: e.source_dc,
    dest_dc: e.dest_dc,
    transfer_qty: e.transfer_qty,
    transfer_cost: e.transfer_cost,
    penalty_exposure: e.penalty_exposure,
    submitted_ago: e.submitted_ago,
    state: 'PENDING_APPROVAL',
  }))
}

// Depletion series generator (60 days) including PO replenishment on po_eta_offset
export function buildDepletion(event) {
  const start = new Date()
  const stockoutOffset = Math.max(
    0,
    Math.round(
      (new Date(event.stockout_date).getTime() - start.getTime()) /
        (1000 * 60 * 60 * 24)
    )
  )
  const poOffset = Math.max(
    0,
    Math.round(
      (new Date(event.po_eta).getTime() - start.getTime()) /
        (1000 * 60 * 60 * 24)
    )
  )
  const burn = event.nj_daily_burn
  let units = event.nj_units
  const points = []
  for (let d = 0; d <= 60; d++) {
    if (d === poOffset) units += event.po_units
    points.push({
      day: d,
      units: Math.max(0, Math.round(units)),
      date: new Date(start.getTime() + d * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10),
    })
    units = Math.max(0, units - burn)
  }
  return { points, stockoutOffset, poOffset }
}
