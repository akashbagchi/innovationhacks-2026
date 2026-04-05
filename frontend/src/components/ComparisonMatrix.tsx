import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { ChevronRight } from 'lucide-react'
import type { PolicyRecord } from '../types/policy'
import { computeStringency } from '../lib/stringency'
import { ProvenanceChip } from './ProvenanceChip'

interface ComparisonMatrixProps {
  policies: PolicyRecord[]
}

const criterionColors: Record<string, { bg: string; text: string }> = {
  combination_restriction: { bg: '#EDE9FE', text: '#5B21B6' },
  step_therapy:            { bg: '#FEF3C7', text: '#92400E' },
  prior_therapy:           { bg: '#FEF9C3', text: '#713F12' },
  line_of_therapy:         { bg: '#EBF4FA', text: '#2D6A90' },
  disease_severity:        { bg: '#FEE2E2', text: '#B91C1C' },
  lab_value:               { bg: '#D0F4F1', text: '#0A6B62' },
  diagnosis:               { bg: '#F1F5F9', text: '#475569' },
  prescriber:              { bg: '#DCFCE7', text: '#166534' },
  clinical_response:       { bg: '#CCFBF1', text: '#0F766E' },
  other:                   { bg: '#F8FAFC', text: '#64748B' },
}

export function ComparisonMatrix({ policies }: ComparisonMatrixProps) {
  const [expandedIndication, setExpandedIndication] = useState<string | null>(null)

  const allIndications = Array.from(new Set(policies.flatMap(p => p.indications.map(i => i.name))))
  const sorted = [...policies].sort((a, b) => computeStringency(b).score - computeStringency(a).score)

  // Summary rows for the scaffold
  const summaryRows = sorted.map(p => {
    const { score, breakdown } = computeStringency(p)
    const paCount    = p.indications.filter(i => i.pa_required).length
    const stepCount  = p.indications.filter(i => i.step_therapy_required).length
    const totalInd   = p.indications.length
    const avgCriteria = p.indications.length > 0
      ? (p.indications.reduce((s, i) => s + i.initial_authorization.criteria.length, 0) / p.indications.length).toFixed(1)
      : '—'
    const authDurations = p.indications
      .map(i => i.initial_authorization.authorization_duration_months)
      .filter(Boolean) as number[]
    const authDuration = authDurations.length > 0
      ? `${Math.min(...authDurations)}–${Math.max(...authDurations)}mo`
      : '—'
    const scoreColor = score >= 70 ? '#DC2626' : score >= 40 ? '#D97706' : '#10A090'
    return { p, score, breakdown, paCount, stepCount, totalInd, avgCriteria, authDuration, scoreColor }
  })

  const summaryDimensions = [
    { label: 'Indications covered',  render: (r: typeof summaryRows[0]) => `${r.totalInd}` },
    { label: 'PA required',          render: (r: typeof summaryRows[0]) => `${r.paCount}/${r.totalInd} (${Math.round(r.paCount/r.totalInd*100)}%)` },
    { label: 'Step therapy',         render: (r: typeof summaryRows[0]) => r.stepCount > 0 ? `${r.stepCount} indication${r.stepCount > 1 ? 's' : ''}` : 'None' },
    { label: 'Auth duration',        render: (r: typeof summaryRows[0]) => r.authDuration },
    { label: 'Avg criteria/ind.',    render: (r: typeof summaryRows[0]) => r.avgCriteria as string },
    { label: 'Exclusions',           render: (r: typeof summaryRows[0]) => `${r.p.exclusions?.length ?? 0}` },
    { label: 'Stringency score',     render: (r: typeof summaryRows[0]) => r.score.toString(), isScore: true },
  ]

  return (
    <div className="space-y-5">
      {/* Unified scaffold */}
      <div className="rounded-xl overflow-hidden" style={{ background: '#fff', border: '1px solid #E2E7EF' }}>

        {/* Column headers */}
        <div className="grid border-b" style={{
          gridTemplateColumns: `200px repeat(${sorted.length}, 1fr)`,
          borderBottomColor: '#E2E7EF',
          background: '#F5F6F8',
        }}>
          <div className="px-4 py-3">
            <p className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: '#9AA3AF' }}>
              Payer Comparison
            </p>
          </div>
          {summaryRows.map(({ p, score, scoreColor }) => (
            <div key={p.payer.name} className="px-4 py-3 border-l" style={{ borderLeftColor: '#E2E7EF' }}>
              <p className="text-sm font-semibold" style={{ color: '#0E1117' }}>{p.payer.name}</p>
              {p.payer.policy_id && (
                <p className="text-[10px] font-mono mt-0.5" style={{ color: '#9AA3AF' }}>{p.payer.policy_id}</p>
              )}
              {/* Compact stringency bar */}
              <div className="mt-2 h-1 rounded-full w-full" style={{ background: '#E2E7EF' }}>
                <div className="h-full rounded-full" style={{ width: `${score}%`, background: scoreColor }} />
              </div>
            </div>
          ))}
        </div>

        {/* Summary dimension rows */}
        {summaryDimensions.map((dim, ri) => (
          <div key={dim.label} className="grid border-b last:border-b-0"
            style={{ gridTemplateColumns: `200px repeat(${sorted.length}, 1fr)`, borderBottomColor: '#E2E7EF' }}>
            <div className="px-4 py-2.5 flex items-center" style={{ background: ri % 2 === 1 ? '#FAFBFC' : '#fff' }}>
              <p className="text-xs" style={{ color: '#6B7583' }}>{dim.label}</p>
            </div>
            {summaryRows.map((row) => {
              const value = dim.render(row)
              const scoreColor = (dim as { isScore?: boolean }).isScore ? row.scoreColor : undefined
              return (
                <div key={row.p.payer.name} className="px-4 py-2.5 border-l flex items-center"
                  style={{ background: ri % 2 === 1 ? '#FAFBFC' : '#fff', borderLeftColor: '#E2E7EF' }}>
                  <p className="text-xs font-semibold tabular-nums"
                    style={{ color: scoreColor ?? (value === 'None' || value === '0' ? '#10A090' : '#0E1117') }}>
                    {value}
                  </p>
                </div>
              )
            })}
          </div>
        ))}
      </div>

      {/* Indication rows — expandable */}
      <div className="rounded-xl overflow-hidden" style={{ background: '#fff', border: '1px solid #E2E7EF' }}>
        {/* Header */}
        <div className="grid border-b px-0" style={{
          gridTemplateColumns: `200px repeat(${sorted.length}, 1fr)`,
          borderBottomColor: '#E2E7EF',
          background: '#F5F6F8',
        }}>
          <div className="px-4 py-3">
            <p className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: '#9AA3AF' }}>
              Indications
            </p>
            <p className="text-[10px] mt-0.5" style={{ color: '#C0CDD9' }}>click to expand criteria</p>
          </div>
          {sorted.map(p => (
            <div key={p.payer.name} className="px-4 py-3 border-l" style={{ borderLeftColor: '#E2E7EF' }}>
              <div className="flex items-center gap-2">
                <span className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: '#10A090' }} />
                <span className="text-[10px]" style={{ color: '#9AA3AF' }}>No PA</span>
                <span className="w-2.5 h-2.5 rounded-full flex-shrink-0 ml-2" style={{ background: '#F59E0B' }} />
                <span className="text-[10px]" style={{ color: '#9AA3AF' }}>PA req.</span>
              </div>
            </div>
          ))}
        </div>

        {allIndications.map((indName, ri) => {
          const isExpanded = expandedIndication === indName
          return (
            <div key={indName}>
              {/* Indication row */}
              <button
                onClick={() => setExpandedIndication(isExpanded ? null : indName)}
                className="w-full grid text-left hover:bg-[#F5F6F8] transition-colors border-b"
                style={{
                  gridTemplateColumns: `200px repeat(${sorted.length}, 1fr)`,
                  borderBottomColor: '#E2E7EF',
                  background: isExpanded ? '#EFF4FB' : ri % 2 === 1 ? '#FAFBFC' : '#fff',
                }}
              >
                <div className="px-4 py-3 flex items-center gap-2">
                  <ChevronRight size={12} style={{
                    color: '#9AA3AF',
                    transform: isExpanded ? 'rotate(90deg)' : 'none',
                    transition: 'transform 0.12s',
                    flexShrink: 0,
                  }} />
                  <span className="text-xs" style={{ color: '#0E1117' }}>{indName}</span>
                </div>
                {sorted.map(p => {
                  const ind = p.indications.find(i => i.name === indName)
                  return (
                    <div key={p.payer.name} className="px-4 py-3 border-l flex items-center gap-3"
                      style={{ borderLeftColor: '#E2E7EF' }}>
                      {ind ? (
                        <>
                          <div className="w-3 h-3 rounded-full flex-shrink-0"
                            style={{ background: ind.pa_required ? '#F59E0B' : '#10A090' }}
                            title={ind.pa_required ? 'PA required' : 'No PA'} />
                          {ind.step_therapy_required && (
                            <span className="text-[10px] px-1.5 py-0.5 rounded font-medium"
                              style={{ background: '#FEE2E2', color: '#B91C1C' }}>Step Rx</span>
                          )}
                          <span className="text-[10px]" style={{ color: '#9AA3AF' }}>
                            {ind.initial_authorization.criteria.length} criteria
                          </span>
                        </>
                      ) : (
                        <div className="w-3 h-3 rounded-full" style={{ background: '#E2E7EF' }} title="Not covered" />
                      )}
                    </div>
                  )
                })}
              </button>

              {/* Expanded criteria per payer */}
              <AnimatePresence>
                {isExpanded && (
                  <motion.div
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: 'auto' }}
                    exit={{ opacity: 0, height: 0 }}
                    transition={{ duration: 0.15, ease: 'easeInOut' }}
                    className="overflow-hidden border-b"
                    style={{ borderBottomColor: '#E2E7EF', background: '#F8FCFF' }}
                  >
                    <div className="grid px-0"
                      style={{ gridTemplateColumns: `200px repeat(${sorted.length}, 1fr)` }}>
                      {/* empty first cell */}
                      <div className="px-4 py-3" />
                      {sorted.map(p => {
                        const ind = p.indications.find(i => i.name === indName)
                        return (
                          <div key={p.payer.name} className="px-4 py-3 border-l"
                            style={{ borderLeftColor: '#D0D9E4' }}>
                            {ind ? (
                              <div className="space-y-2">
                                {/* Auth duration */}
                                {ind.initial_authorization.authorization_duration_months && (
                                  <p className="text-[11px]" style={{ color: '#6B7583' }}>
                                    Auth: <span className="font-semibold">{ind.initial_authorization.authorization_duration_months}mo</span>
                                  </p>
                                )}
                                {/* Criteria list */}
                                {ind.initial_authorization.criteria.length > 0 ? (
                                  <ul className="space-y-1.5">
                                    {ind.initial_authorization.criteria.map((c, j) => {
                                      const colors = criterionColors[c.criterion_type] ?? criterionColors.other
                                      return (
                                        <li key={j} className="flex items-start gap-1.5 text-[11px]">
                                          <span className="px-1.5 py-0.5 rounded text-[10px] font-medium flex-shrink-0"
                                            style={{ background: colors.bg, color: colors.text }}>
                                            {c.criterion_type.replace(/_/g, ' ')}
                                          </span>
                                          <span style={{ color: '#475569' }}>{c.description}</span>
                                        </li>
                                      )
                                    })}
                                  </ul>
                                ) : (
                                  <p className="text-[11px]" style={{ color: '#9AA3AF' }}>No specific criteria</p>
                                )}
                                <ProvenanceChip payer={p.payer.name} policyTitle={p.payer.policy_title} />
                              </div>
                            ) : (
                              <p className="text-[11px]" style={{ color: '#C0CDD9' }}>Not covered</p>
                            )}
                          </div>
                        )
                      })}
                    </div>
                    {/* Exclusions if any */}
                    {sorted.some(p => p.exclusions && p.exclusions.length > 0) && (
                      <div className="grid px-0 border-t"
                        style={{ gridTemplateColumns: `200px repeat(${sorted.length}, 1fr)`, borderTopColor: '#D0D9E4' }}>
                        <div className="px-4 py-2 flex items-center">
                          <p className="text-[10px] font-semibold uppercase" style={{ color: '#B91C1C' }}>Exclusions</p>
                        </div>
                        {sorted.map(p => (
                          <div key={p.payer.name} className="px-4 py-2 border-l" style={{ borderLeftColor: '#D0D9E4' }}>
                            {p.exclusions && p.exclusions.length > 0 ? (
                              p.exclusions.map((e, i) => (
                                <p key={i} className="text-[11px]" style={{ color: '#991B1B' }}>• {e.description}</p>
                              ))
                            ) : (
                              <p className="text-[11px]" style={{ color: '#D0D9E4' }}>None</p>
                            )}
                          </div>
                        ))}
                      </div>
                    )}
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          )
        })}
      </div>
    </div>
  )
}
