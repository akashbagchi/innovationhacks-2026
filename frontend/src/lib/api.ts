import type { PolicyRecord, ChangeEntry } from '../types/policy'
import type { ChatSource } from '../types/chat'

const BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

export interface LivePayer {
  payer: string
  drug_id: string
  mongo_id: string
  policy_record: PolicyRecord
}

export interface PolicySearchResult {
  _id: string
  status?: string
  drug_id?: string
  payer_canonical?: string
  filename?: string
  source?: string
  version?: number
  policy_record?: PolicyRecord | null
}

export async function fetchPoliciesForDrug(drugId: string): Promise<LivePayer[]> {
  const res = await fetch(`${BASE}/v1/compare?drug=${encodeURIComponent(drugId)}`)
  if (!res.ok) throw new Error(`compare API error: ${res.status}`)
  const data = await res.json()
  return (data.payers ?? []) as LivePayer[]
}

export async function fetchAllPolicies(): Promise<PolicySearchResult[]> {
  const res = await fetch(`${BASE}/v1/policies/search`)
  if (!res.ok) throw new Error(`policies search API error: ${res.status}`)
  const data = await res.json()
  return (data.results ?? []) as PolicySearchResult[]
}

export async function fetchChanges(drugId = '', severity = ''): Promise<ChangeEntry[]> {
  const params = new URLSearchParams()
  if (drugId) params.set('drug_id', drugId)
  if (severity) params.set('severity', severity)
  const res = await fetch(`${BASE}/v1/changes?${params}`)
  if (!res.ok) throw new Error(`changes API error: ${res.status}`)
  const data = await res.json()
  return (data.changes ?? []) as ChangeEntry[]
}

export async function sendChatMessage(
  message: string,
  history: { role: 'user' | 'assistant'; content: string }[],
): Promise<{ reply: string; sources: ChatSource[] }> {
  const res = await fetch(`${BASE}/v1/chat`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message, history }),
  })
  if (!res.ok) throw new Error(`chat API error: ${res.status}`)
  return res.json()
}
