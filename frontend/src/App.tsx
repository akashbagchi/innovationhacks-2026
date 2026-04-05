import { useState, useEffect } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { Sidebar } from './components/Sidebar'
import { PortfolioView } from './components/PortfolioView'
import { DrugDetailView } from './components/DrugDetailView'
import { ComparisonMatrix } from './components/ComparisonMatrix'
import { ChangeDigest } from './components/ChangeDigest'
import { mockChanges } from './data/mockChanges'
import { portfolio as mockPortfolio } from './data/mockPortfolio'
import type { DrugPortfolioEntry } from './data/mockPortfolio'
import type { ChangeEntry, InsightCard, PayerTrend, PolicyRecord } from './types/policy'
import { fetchAllPolicies, fetchChanges, type PolicySearchResult } from './lib/api'
import { ChatPanel } from './components/ChatPanel'
import { computeStringency } from './lib/stringency'
import { canonicalizePayerName } from './lib/formatters'

type NavView = 'portfolio' | 'compare' | 'digest'
const DEMO_MODE_KEY = 'antonrx-demo-mode'

function sourcePriority(source?: string) {
  if (source === 'pipeline' || source === 's3_scan') return 3
  if (source === 'ingest') return 2
  if (source === 'mock') return 1
  return 0
}

function statusPriority(status?: string) {
  if (status === 'normalized') return 3
  if (status === 'extracted') return 2
  return 1
}

function formatQuarter(date: Date) {
  const quarter = Math.floor(date.getMonth() / 3) + 1
  return `Q${quarter} ${date.getFullYear()}`
}

function latestPolicyDate(policies: PolicyRecord[]) {
  const timestamps = policies
    .map(policy => policy.payer.revision_date ?? policy.payer.effective_date)
    .filter(Boolean)
    .map(date => new Date(date as string))
    .filter(date => !Number.isNaN(date.getTime()))
    .map(date => date.getTime())

  if (timestamps.length === 0) return new Date()
  return new Date(Math.max(...timestamps))
}

function buildStableTrend(policies: PolicyRecord[]): PayerTrend[] {
  const currentQuarter = formatQuarter(new Date())
  return policies.map(policy => {
    const score = computeStringency(policy).score
    return {
      payerName: policy.payer.name,
      history: [
        { quarter: currentQuarter, score },
        { quarter: currentQuarter, score },
        { quarter: currentQuarter, score },
        { quarter: currentQuarter, score },
      ],
      delta: 0,
      direction: 'stable',
    }
  })
}

function buildInsightsForDrug(drugName: string, changes: ChangeEntry[]): InsightCard[] {
  const relevant = changes
    .filter(change =>
      (change.drug_id && change.drug_id.toLowerCase() === drugName.toLowerCase()) ||
      change.drug.toLowerCase().includes(drugName.toLowerCase()),
    )
    .slice(0, 3)

  if (relevant.length === 0) {
    return [{
      severity: 'low',
      text: 'No major payer-specific change signal is currently attached to this product in the live changelog.',
      action: 'Review the comparison and criteria tabs directly. New pipeline runs will automatically improve this view as more changelog entries are generated.',
    }]
  }

  return relevant.map(change => ({
    severity: change.severity === 'HIGH' ? 'high' : change.severity === 'MED' ? 'medium' : 'low',
    text: `${change.payer}: ${change.summary}`,
    action: change.after_text
      ? `Review the updated criteria details and compare them against the previous version before sharing guidance with field teams.`
      : `Use this change as the starting point for payer-specific review and escalation.`,
  }))
}

function preferLiveDocs(docs: PolicySearchResult[]) {
  return docs.filter(doc => doc.source !== 'mock')
}

function preferLiveChanges(changes: ChangeEntry[]) {
  return changes.filter(change => change.source !== 'mock')
}

function normalizePolicyRecord(input: PolicySearchResult['policy_record']): PolicyRecord | null {
  if (!input) return null

  const raw = input as Record<string, unknown>
  const payer = (raw.payer ?? {}) as Record<string, unknown>
  const drug = (raw.drug ?? {}) as Record<string, unknown>
  const indications = Array.isArray(raw.indications) ? raw.indications : null

  if (!payer.name || !drug || !indications) return null

  return {
    payer: {
      name: canonicalizePayerName(String(payer.name)),
      policy_id: typeof payer.policy_id === 'string' ? payer.policy_id : null,
      policy_title: String(payer.policy_title ?? 'Untitled policy'),
      effective_date: typeof payer.effective_date === 'string' ? payer.effective_date : undefined,
      revision_date: typeof payer.revision_date === 'string' ? payer.revision_date : undefined,
    },
    drug: {
      brand_name: String(
        drug.brand_name ??
        drug.display_name ??
        (Array.isArray(drug.brand_names) ? drug.brand_names[0] : '') ??
        drug.generic_name ??
        'Unknown drug',
      ),
      generic_name: String(drug.generic_name ?? drug.normalized_generic_name ?? 'unknown'),
      j_codes: Array.isArray(drug.j_codes) ? (drug.j_codes as string[]) : [],
      hcpcs_codes: Array.isArray(drug.hcpcs_codes) ? (drug.hcpcs_codes as string[]) : [],
      drug_class: typeof drug.drug_class === 'string' ? drug.drug_class : undefined,
      route_of_administration: typeof drug.route_of_administration === 'string' ? drug.route_of_administration : undefined,
      benefit_type: typeof drug.benefit_type === 'string' ? drug.benefit_type : undefined,
      limitations_of_use: typeof drug.limitations_of_use === 'string' ? drug.limitations_of_use : undefined,
    },
    indications: indications
      .map(indication => {
        const rawIndication = (indication ?? {}) as Record<string, unknown>
        const initial = (rawIndication.initial_authorization ?? {}) as Record<string, unknown>
        const reauth = rawIndication.reauthorization && typeof rawIndication.reauthorization === 'object'
          ? rawIndication.reauthorization as Record<string, unknown>
          : null

        return {
          name: String(rawIndication.name ?? 'Unknown indication'),
          description: typeof rawIndication.description === 'string' ? rawIndication.description : undefined,
          icd10_codes: Array.isArray(rawIndication.icd10_codes)
            ? rawIndication.icd10_codes as string[]
            : Array.isArray(rawIndication.icd10_codes_explicit)
              ? rawIndication.icd10_codes_explicit as string[]
              : [],
          pa_required: Boolean(rawIndication.pa_required),
          step_therapy_required: Boolean(rawIndication.step_therapy_required),
          initial_authorization: {
            criteria: Array.isArray(initial.criteria) ? initial.criteria as PolicyRecord['indications'][number]['initial_authorization']['criteria'] : [],
            authorization_duration_months: typeof initial.authorization_duration_months === 'number' ? initial.authorization_duration_months : null,
            required_prescriber_specialties: Array.isArray(initial.required_prescriber_specialties) ? initial.required_prescriber_specialties as string[] : [],
          },
          reauthorization: reauth
            ? {
                criteria: Array.isArray(reauth.criteria) ? reauth.criteria as PolicyRecord['indications'][number]['initial_authorization']['criteria'] : [],
                authorization_duration_months: typeof reauth.authorization_duration_months === 'number' ? reauth.authorization_duration_months : null,
                required_prescriber_specialties: Array.isArray(reauth.required_prescriber_specialties) ? reauth.required_prescriber_specialties as string[] : [],
              }
            : null,
        }
      })
      .filter(indication => indication.name),
    exclusions: Array.isArray(raw.exclusions) ? raw.exclusions as PolicyRecord['exclusions'] : [],
    confidence_scores: typeof raw.confidence_scores === 'object' && raw.confidence_scores !== null
      ? raw.confidence_scores as PolicyRecord['confidence_scores']
      : { overall: 0 },
  }
}

function dedupePoliciesByPayer(policies: PolicySearchResult[]) {
  const byPayer = new Map<string, PolicySearchResult>()
  for (const doc of policies) {
    const rawPayerKey = doc.payer_canonical ?? doc.policy_record?.payer.name ?? doc._id
    const payerKey = canonicalizePayerName(String(rawPayerKey))
    const existing = byPayer.get(payerKey)
    if (!existing) {
      byPayer.set(payerKey, doc)
      continue
    }

    const incomingRank = [
      sourcePriority(doc.source),
      statusPriority(doc.status),
      doc.version ?? 0,
    ]
    const existingRank = [
      sourcePriority(existing.source),
      statusPriority(existing.status),
      existing.version ?? 0,
    ]

    if (
      incomingRank[0] > existingRank[0] ||
      (incomingRank[0] === existingRank[0] && incomingRank[1] > existingRank[1]) ||
      (incomingRank[0] === existingRank[0] && incomingRank[1] === existingRank[1] && incomingRank[2] > existingRank[2])
    ) {
      byPayer.set(payerKey, doc)
    }
  }
  return Array.from(byPayer.values())
}

function buildLivePortfolio(
  docs: PolicySearchResult[],
  changes: ChangeEntry[],
): DrugPortfolioEntry[] {
  const liveDocs = preferLiveDocs(docs).filter(
    doc =>
      (doc.status === 'normalized' || doc.status === 'extracted' || doc.status == null) &&
      doc.policy_record != null &&
      doc.drug_id,
  )

  const grouped = new Map<string, PolicySearchResult[]>()
  for (const doc of liveDocs) {
    const key = doc.drug_id as string
    const current = grouped.get(key) ?? []
    current.push(doc)
    grouped.set(key, current)
  }

  return Array.from(grouped.entries())
    .map(([drugId, docsForDrug]) => {
      const deduped = dedupePoliciesByPayer(docsForDrug)
      const policies = deduped
        .map(doc => normalizePolicyRecord(doc.policy_record))
        .filter((policy): policy is PolicyRecord => policy != null)

      if (policies.length === 0) return null

      const sample = policies[0]
      const latestDate = latestPolicyDate(policies)
      const matchingChanges = changes.filter(change =>
        (change.drug_id && change.drug_id.toLowerCase() === drugId.toLowerCase()) ||
        change.drug.toLowerCase().includes((sample.drug.brand_name || '').toLowerCase()) ||
        change.drug.toLowerCase().includes((sample.drug.generic_name || '').toLowerCase()),
      )

      return {
        id: drugId,
        brandName: sample.drug.brand_name || sample.drug.generic_name || drugId,
        genericName: sample.drug.generic_name || drugId,
        drugClass: sample.drug.drug_class || 'Live imported policy',
        jCode: sample.drug.j_codes?.[0] || sample.drug.hcpcs_codes?.[0] || 'N/A',
        policies,
        insights: buildInsightsForDrug(drugId, changes),
        trends: buildStableTrend(policies),
        livesAtRisk: `${policies.length} payer policy${policies.length === 1 ? '' : 'ies'} live`,
        lastUpdated: latestDate.toISOString().slice(0, 10),
        changeCount: matchingChanges.length,
      } satisfies DrugPortfolioEntry
    })
    .filter((entry): entry is DrugPortfolioEntry => entry != null)
    .sort((a, b) => a.brandName.localeCompare(b.brandName))
}

export default function App() {
  const [activeNav, setActiveNav]         = useState<NavView>('portfolio')
  const [selectedDrug, setSelectedDrug]   = useState<string | null>(null)
  const [compareDrugId, setCompareDrugId] = useState<string | null>(null)
  const [livePortfolio, setLivePortfolio] = useState<DrugPortfolioEntry[]>([])
  const [liveChanges, setLiveChanges]     = useState<ChangeEntry[]>([])
  const [demoMode, setDemoMode]           = useState(false)
  const [chatOpen, setChatOpen]         = useState(false)

  useEffect(() => {
    if (typeof window === 'undefined') return
    setDemoMode(window.localStorage.getItem(DEMO_MODE_KEY) === 'true')
  }, [])

  useEffect(() => {
    Promise.allSettled([fetchAllPolicies(), fetchChanges()]).then(([policiesResult, changesResult]) => {
      const nextChanges = changesResult.status === 'fulfilled' ? preferLiveChanges(changesResult.value) : []
      setLiveChanges(nextChanges)

      if (policiesResult.status !== 'fulfilled') {
        setLivePortfolio([])
        return
      }

      const livePortfolio = buildLivePortfolio(policiesResult.value, nextChanges)
      setLivePortfolio(livePortfolio)
    })
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(DEMO_MODE_KEY, String(demoMode))
  }, [demoMode])

  const portfolio = demoMode ? mockPortfolio : livePortfolio
  const changes = demoMode ? mockChanges : liveChanges
  const drug = selectedDrug ? portfolio.find(d => d.id === selectedDrug) ?? null : null

  useEffect(() => {
    if (portfolio.length === 0) {
      setSelectedDrug(null)
      setCompareDrugId(null)
      return
    }

    if (selectedDrug && !portfolio.some(entry => entry.id === selectedDrug)) {
      setSelectedDrug(null)
    }

    if (!compareDrugId || !portfolio.some(entry => entry.id === compareDrugId)) {
      setCompareDrugId(portfolio[0].id)
    }
  }, [portfolio, selectedDrug, compareDrugId])

  function handleNavigate(id: string) {
    setActiveNav(id as NavView)
    setSelectedDrug(null)
  }

  return (
    <div className="flex min-h-screen" style={{ background: '#F0EFEB' }}>
      <Sidebar active={activeNav} onNavigate={handleNavigate} changes={changes} portfolio={portfolio} onChatToggle={() => setChatOpen(o => !o)} />

      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <AnimatePresence mode="wait">

          {activeNav === 'portfolio' && (
            <motion.div key={drug ? `drug-${drug.id}` : 'portfolio'}
              className="flex-1 flex flex-col"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}>
              {drug
                ? <DrugDetailView drug={drug} onBack={() => setSelectedDrug(null)} changes={changes} />
                : <PortfolioView portfolio={portfolio} onSelectDrug={setSelectedDrug} changes={changes} />}
            </motion.div>
          )}

          {activeNav === 'compare' && (
            <motion.div key="compare" className="flex-1 flex flex-col"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}>
              {/* Topbar */}
              <div style={{ background: '#FFFFFF', borderBottom: '1px solid #D8D4CC' }} className="px-7 py-4 flex-shrink-0">
                <p className="label-mono mb-1">Cross-Payer Intelligence</p>
                <h1 className="text-base font-bold" style={{ color: '#131210' }}>Coverage Matrix</h1>
                <p className="text-xs mt-1" style={{ color: '#4A4845' }}>
                  Select a drug below for a focused cross-payer analysis
                </p>
                <div className="flex gap-2 mt-3 flex-wrap">
                  {portfolio.map(d => (
                    <button key={d.id}
                      onClick={() => setCompareDrugId(d.id)}
                      style={{
                        background: compareDrugId === d.id ? '#131210' : '#F0EFEB',
                        border: `1px solid ${compareDrugId === d.id ? '#131210' : '#D8D4CC'}`,
                        borderRadius: '2px',
                        color: compareDrugId === d.id ? '#FFFFFF' : '#4A4845',
                        fontFamily: "'IBM Plex Mono', monospace",
                        fontSize: '10px', letterSpacing: '0.06em',
                        padding: '5px 10px', cursor: 'pointer',
                        transition: 'all 0.1s',
                      }}>
                      {d.brandName}
                      <span style={{ marginLeft: '8px', color: compareDrugId === d.id ? '#A8A5A0' : '#918D88' }}>{d.jCode}</span>
                    </button>
                  ))}
                </div>
              </div>
              <div className="flex-1 overflow-y-auto p-7">
                <ComparisonMatrix
                  policies={(portfolio.find(d => d.id === compareDrugId) ?? portfolio[0])?.policies ?? []}
                />
              </div>
            </motion.div>
          )}

          {activeNav === 'digest' && (
            <motion.div key="digest" className="flex-1 flex flex-col"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}>
              {/* Topbar */}
              <div style={{ background: '#FFFFFF', borderBottom: '1px solid #D8D4CC' }} className="px-7 py-4 flex-shrink-0">
                <p className="label-mono mb-1">Policy Surveillance</p>
                <h1 className="text-base font-bold" style={{ color: '#131210' }}>Change Digest</h1>
                <p className="text-xs mt-1" style={{ color: '#4A4845' }}>
                  All policy changes detected across your tracked formulary this quarter
                </p>
              </div>
              <div className="flex-1 overflow-y-auto p-7 max-w-4xl">
                <ChangeDigest changes={changes} />
              </div>
            </motion.div>
          )}

        </AnimatePresence>
      </div>
      <ChatPanel open={chatOpen} onClose={() => setChatOpen(false)} />

      <div
        style={{
          position: 'fixed',
          right: '18px',
          bottom: '12px',
          zIndex: 40,
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          padding: '6px 10px',
          borderRadius: '999px',
          background: 'rgba(255,255,255,0.84)',
          border: '1px solid rgba(216,212,204,0.9)',
          boxShadow: '0 2px 12px rgba(19,18,16,0.05)',
          backdropFilter: 'blur(8px)',
        }}
      >
        <span
          style={{
            fontFamily: "'IBM Plex Mono', monospace",
            fontSize: '9px',
            letterSpacing: '0.12em',
            textTransform: 'uppercase',
            color: demoMode ? '#8B6428' : '#918D88',
          }}
        >
          Demo mode
        </span>
        <button
          type="button"
          role="switch"
          aria-checked={demoMode}
          aria-label="Toggle demo mode"
          onClick={() => setDemoMode(current => !current)}
          style={{
            width: '32px',
            height: '18px',
            borderRadius: '999px',
            border: `1px solid ${demoMode ? 'rgba(139,100,40,0.35)' : '#D8D4CC'}`,
            background: demoMode ? '#F8EDDC' : '#F0EFEB',
            padding: '1px',
            position: 'relative',
            transition: 'all 0.15s ease',
            cursor: 'pointer',
          }}
        >
          <span
            style={{
              display: 'block',
              width: '14px',
              height: '14px',
              borderRadius: '50%',
              background: demoMode ? '#8B6428' : '#FFFFFF',
              boxShadow: '0 1px 3px rgba(19,18,16,0.16)',
              transform: demoMode ? 'translateX(14px)' : 'translateX(0)',
              transition: 'transform 0.15s ease, background 0.15s ease',
            }}
          />
        </button>
      </div>
    </div>
  )
}
