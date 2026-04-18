import { NextResponse } from 'next/server'

// Placeholder API — the real API isn't built yet. All data is mocked client-side.
export async function GET() {
  return NextResponse.json({ ok: true, service: 'pop-sentinel', status: 'mocked' })
}

export async function POST() {
  return NextResponse.json({ ok: true })
}
