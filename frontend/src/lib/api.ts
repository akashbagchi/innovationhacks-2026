import type { PolicyRecord, ChangeEntry } from '../types/policy'

const BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

export interface LivePayer {
  payer: string
  drug_id: string
  mongo_id: string
  policy_record: PolicyRecord
}

export async function fetchPoliciesForDrug(drugId: string): Promise<LivePayer[]> {
  const res = await fetch(`${BASE}/v1/compare?drug=${encodeURIComponent(drugId)}`)
  if (!res.ok) throw new Error(`compare API error: ${res.status}`)
  const data = await res.json()
  return (data.payers ?? []) as LivePayer[]
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
