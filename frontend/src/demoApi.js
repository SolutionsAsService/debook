// In-browser stand-in for the Go API, used when the Load Lab is hosted without a backend
// (for example on Vercel). It mirrors the contract in docs/openapi.yaml closely enough for
// the fetch slots → book → cancel scenario: slots are locked on booking, idempotency keys
// are honoured and cancelled bookings release their slot.
export const DEMO_API = 'demo'

const slots = new Map()
const bookings = new Map()
const idempotency = new Map()
const seeded = new Set()
const wait = () => new Promise(resolve => setTimeout(resolve, 8 + Math.random() * 40))
const json = (status, body) => ({ ok: status < 400, status, json: async () => body })

function ensureSlots(region, resource) {
  const key = `${region}/${resource}`
  if (seeded.has(key)) return
  seeded.add(key)
  const start = new Date()
  start.setSeconds(0, 0)
  for (let i = 1; i <= 7 * 24 * 60; i++) {
    const startsAt = new Date(start.getTime() + i * 60_000)
    const id = crypto.randomUUID()
    slots.set(id, { id, key, region_id: region, resource_id: resource, status: 'AVAILABLE', starts_at: startsAt.toISOString(), ends_at: new Date(startsAt.getTime() + 60_000).toISOString() })
  }
}

export async function demoFetch(path, options = {}) {
  await wait()
  const url = new URL(path, 'http://demo')
  const method = (options.method || 'GET').toUpperCase()
  if (url.pathname === '/healthz' || url.pathname === '/readyz') return json(200, { status: 'ok' })
  if (method === 'GET' && url.pathname === '/api/v1/slots') {
    const region = url.searchParams.get('region_id'), resource = url.searchParams.get('resource_id') || ''
    const statuses = (url.searchParams.get('status') || '').split(',')
    if (!region || !statuses[0]) return json(400, { error: 'region_id and status are required' })
    ensureSlots(region, resource)
    const result = [...slots.values()].filter(slot => slot.region_id === region && (!resource || slot.resource_id === resource) && statuses.includes(slot.status)).slice(0, 500)
    return json(200, result.map(({ key, ...slot }) => slot))
  }
  if (method === 'POST' && url.pathname === '/api/v1/bookings') {
    const key = options.headers?.['Idempotency-Key']
    const req = JSON.parse(options.body || '{}')
    if (!req.slot_id || !req.customer_ref || !req.wallet_address || !key) return json(400, { error: 'slot_id, customer_ref, wallet_address and Idempotency-Key are required' })
    if (!/^0x[0-9a-fA-F]{40}$/.test(req.wallet_address)) return json(400, { error: 'wallet_address must be a valid EVM address' })
    if (idempotency.has(key)) return json(202, idempotency.get(key))
    const slot = slots.get(req.slot_id)
    if (!slot) return json(404, { error: 'slot not found' })
    if (slot.status !== 'AVAILABLE') return json(409, { error: 'slot is not available' })
    slot.status = 'RESERVED'
    const booking = { id: crypto.randomUUID(), slot_id: slot.id, status: 'PENDING_CONFIRMATION', customer_ref: req.customer_ref, wallet_address: req.wallet_address, expires_at: slot.ends_at }
    bookings.set(booking.id, booking)
    idempotency.set(key, booking)
    return json(202, booking)
  }
  const cancel = url.pathname.match(/^\/api\/v1\/bookings\/([^/]+)\/cancel$/)
  if (method === 'POST' && cancel) {
    const booking = bookings.get(cancel[1])
    if (!booking || !['PENDING_CONFIRMATION', 'CONFIRMED'].includes(booking.status)) return json(404, { error: 'active booking not found' })
    booking.status = 'CANCELLED'
    slots.get(booking.slot_id).status = 'AVAILABLE'
    return json(200, booking)
  }
  return json(404, { error: 'not found' })
}
