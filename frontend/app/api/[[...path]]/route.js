import { NextResponse } from 'next/server'

const API_BASE_URL = process.env.POP_API_BASE_URL || 'http://127.0.0.1:8000'

async function proxy(request, { params }) {
  const path = Array.isArray(params.path) ? params.path.join('/') : ''
  const target = new URL(`${API_BASE_URL.replace(/\/$/, '')}/${path}`)
  target.search = request.nextUrl.search

  const headers = new Headers()
  const contentType = request.headers.get('content-type')
  if (contentType) headers.set('content-type', contentType)
  headers.set('x-actor', request.headers.get('x-actor') || 'demo-user')

  const init = {
    method: request.method,
    headers,
    cache: 'no-store',
  }

  if (request.method !== 'GET' && request.method !== 'HEAD') {
    init.body = await request.text()
  }

  const response = await fetch(target, init)
  return new NextResponse(response.body, {
    status: response.status,
    headers: {
      'content-type': response.headers.get('content-type') || 'application/json',
    },
  })
}

export async function GET(request, context) {
  return proxy(request, context)
}

export async function POST(request, context) {
  return proxy(request, context)
}
