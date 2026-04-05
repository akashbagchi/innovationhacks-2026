import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { ChevronRight } from 'lucide-react'
import type { PolicyRecord } from '../types/policy'
import { computeStringency } from '../lib/stringency'
import { ProvenanceChip } from './ProvenanceChip'

interface ComparisonMatrixProps {
  policies: PolicyRecord[]
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

const criterionColors: Record<string, { bg: string; text: string; border: string }> = {
  combination_restriction: { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  step_therapy:            { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  prior_therapy:           { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  line_of_therapy:         { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  disease_severity:        { bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  lab_value:               { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  diagnosis:               { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  prescriber:              { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  clinical_response:       { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  other:                   { bg: '#F0EFEB', text: '#918D88', border: '#D8D4CC' },
}

export function ComparisonMatrix({ policies }: ComparisonMatrixProps) {
  const [expandedIndication, setExpandedIndication] = useState<string | null>(null)

  const allIndications = Array.from(new Set(policies.flatMap(p => p.indications.map(i => i.name))))
  const sorted         = [...policies].sort((a, b) => computeStringency(b).score - computeStringency(a).score)

  const summaryRows = sorted.map(p => {
    const { score } = computeStringency(p)
    const paCount   = p.indications.filter(i => i.pa_required).length
    const stepCount = p.indications.filter(i => i.step_therapy_required).length
    const totalInd  = p.indications.length
    const avgCriteria = totalInd > 0
      ? (p.indications.reduce((s, i) => s + i.initial_authorization.criteria.length, 0) / totalInd).toFixed(1)
      : '—'
    const authDurations = p.indications.map(i => i.initial_authorization.authorization_duration_months).filter(Boolean) as number[]
    const authDuration  = authDurations.length > 0 ? `${Math.min(...authDurations)}–${Math.max(...authDurations)}mo` : '—'
    const scoreColor    = score >= 70 ? '#B81C1C' : score >= 40 ? '#8B6428' : '#1A7840'
    return { p, score, paCount, stepCount, totalInd, avgCriteria, authDuration, scoreColor }
  })

  const summaryDimensions = [
    { label: 'Indications covered', render: (r: typeof summaryRows[0]) => `${r.totalInd}` },
    { label: 'PA required',         render: (r: typeof summaryRows[0]) => `${r.paCount}/${r.totalInd} (${Math.round(r.paCount / r.totalInd * 100)}%)` },
    { label: 'Step therapy',        render: (r: typeof summaryRows[0]) => r.stepCount > 0 ? `${r.stepCount} indication${r.stepCount > 1 ? 's' : ''}` : 'None' },
    { label: 'Auth duration',       render: (r: typeof summaryRows[0]) => r.authDuration },
    { label: 'Avg criteria/ind.',   render: (r: typeof summaryRows[0]) => r.avgCriteria as string },
    { label: 'Exclusions',          render: (r: typeof summaryRows[0]) => `${r.p.exclusions?.length ?? 0}` },
    { label: 'Stringency score',    render: (r: typeof summaryRows[0]) => r.score.toString(), isScore: true },
  ]

  const colTemplate = `160px repeat(${sorted.length}, 1fr)`
  const cellBase: React.CSSProperties = { padding: '8px 12px', borderLeft: '1px solid #EBEBEB', display: 'flex', alignItems: 'center' }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
      {/* Summary scaffold */}
      <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', overflow: 'hidden' }}>
        {/* Column headers */}
        <div style={{ display: 'grid', gridTemplateColumns: colTemplate, borderBottom: '1px solid #D8D4CC', background: '#F0EFEB' }}>
          <div style={{ padding: '10px 12px' }}>
            <p style={{ ...LABEL }}>Payer Comparison</p>
          </div>
          {summaryRows.map(({ p, score, scoreColor }) => (
            <div key={p.payer.name} style={{ ...cellBase }}>
              <div style={{ flex: 1 }}>
                <p style={{ fontSize: '13px', fontWeight: 600, color: '#131210' }}>{p.payer.name}</p>
                {p.payer.policy_id && <p style={{ ...mono, fontSize: '9px', color: '#918D88', marginTop: '1px' }}>{p.payer.policy_id}</p>}
                <div style={{ marginTop: '6px', height: '3px', background: '#EBEBEB', borderRadius: '1px', overflow: 'hidden' }}>
                  <div style={{ height: '100%', width: `${score}%`, background: scoreColor, borderRadius: '1px' }} />
                </div>
              </div>
            </div>
          ))}
        </div>

        {/* Dimension rows */}
        {summaryDimensions.map((dim, ri) => (
          <div key={dim.label} style={{ display: 'grid', gridTemplateColumns: colTemplate, borderBottom: ri < summaryDimensions.length - 1 ? '1px solid #EBEBEB' : 'none' }}>
            <div style={{ padding: '7px 12px', display: 'flex', alignItems: 'center', background: ri % 2 === 1 ? '#F7F5F0' : '#FFFFFF' }}>
              <p style={{ fontSize: '12px', color: '#4A4845' }}>{dim.label}</p>
            </div>
            {summaryRows.map(row => {
              const value      = dim.render(row)
              const scoreColor = (dim as { isScore?: boolean }).isScore ? row.scoreColor : undefined
              return (
                <div key={row.p.payer.name} style={{ ...cellBase, background: ri % 2 === 1 ? '#F7F5F0' : '#FFFFFF' }}>
                  <p style={{ ...mono, fontSize: '11px', fontWeight: 600, color: scoreColor ?? (value === 'None' || value === '0' ? '#1A7840' : '#131210') }}>
                    {value}
                  </p>
                </div>
              )
            })}
          </div>
        ))}
      </div>

      {/* Indication rows */}
      <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', overflow: 'hidden' }}>
        {/* Header */}
        <div style={{ display: 'grid', gridTemplateColumns: colTemplate, borderBottom: '1px solid #D8D4CC', background: '#F0EFEB' }}>
          <div style={{ padding: '10px 12px' }}>
            <p style={{ ...LABEL }}>Indications</p>
            <p style={{ ...LABEL, color: '#D8D4CC', marginTop: '2px' }}>click to expand criteria</p>
          </div>
          {sorted.map(p => (
            <div key={p.payer.name} style={{ ...cellBase, gap: '6px' }}>
              <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#1A7840', flexShrink: 0 }} />
              <span style={{ ...LABEL }}>No PA</span>
              <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#8B6428', flexShrink: 0, marginLeft: '8px' }} />
              <span style={{ ...LABEL }}>PA req.</span>
            </div>
          ))}
        </div>

        {allIndications.map((indName, ri) => {
          const isExpanded = expandedIndication === indName
          return (
            <div key={indName}>
              <button
                onClick={() => setExpandedIndication(isExpanded ? null : indName)}
                style={{ width: '100%', display: 'grid', gridTemplateColumns: colTemplate, textAlign: 'left', background: isExpanded ? '#F8EDDC' : ri % 2 === 1 ? '#F7F5F0' : '#FFFFFF', borderBottom: '1px solid #EBEBEB', cursor: 'pointer', border: 'none' }}
              >
                <div style={{ padding: '8px 12px', display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <ChevronRight size={11} style={{ color: '#918D88', transform: isExpanded ? 'rotate(90deg)' : 'none', transition: 'transform 0.12s', flexShrink: 0 }} />
                  <span style={{ fontSize: '12px', color: '#131210' }}>{indName}</span>
                </div>
                {sorted.map(p => {
                  const ind = p.indications.find(i => i.name === indName)
                  return (
                    <div key={p.payer.name} style={{ ...cellBase, gap: '8px' }}>
                      {ind ? (
                        <>
                          <div style={{ width: '10px', height: '10px', borderRadius: '50%', flexShrink: 0, background: ind.pa_required ? '#8B6428' : '#1A7840' }} />
                          {ind.step_therapy_required && (
                            <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: '#FBEAEA', border: '1px solid rgba(184,28,28,0.2)', color: '#B81C1C' }}>Step Rx</span>
                          )}
                          <span style={{ ...mono, fontSize: '9px', color: '#918D88' }}>{ind.initial_authorization.criteria.length} criteria</span>
                        </>
                      ) : (
                        <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#EBEBEB' }} />
                      )}
                    </div>
                  )
                })}
              </button>

              {/* Expanded criteria */}
              <AnimatePresence>
                {isExpanded && (
                  <motion.div initial={{ opacity: 0, height: 0 }} animate={{ opacity: 1, height: 'auto' }}
                    exit={{ opacity: 0, height: 0 }} transition={{ duration: 0.13 }}
                    style={{ overflow: 'hidden', borderBottom: '1px solid #D8D4CC', background: '#FDF8F2' }}>
                    <div style={{ display: 'grid', gridTemplateColumns: colTemplate }}>
                      <div style={{ padding: '10px 12px' }} />
                      {sorted.map(p => {
                        const ind = p.indications.find(i => i.name === indName)
                        return (
                          <div key={p.payer.name} style={{ padding: '10px 12px', borderLeft: '1px solid #EBEBEB' }}>
                            {ind ? (
                              <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                                {ind.initial_authorization.authorization_duration_months != null && (
                                  <p style={{ fontSize: '11px', color: '#4A4845' }}>
                                    Initial auth: <span style={{ fontWeight: 600, color: '#131210' }}>{ind.initial_authorization.authorization_duration_months}mo</span>
                                  </p>
                                )}
                                {ind.initial_authorization.required_prescriber_specialties?.length > 0 && (
                                  <p style={{ fontSize: '11px', color: '#4A4845' }}>
                                    Prescriber: <span style={{ fontWeight: 600, color: '#131210' }}>{ind.initial_authorization.required_prescriber_specialties.join(', ')}</span>
                                  </p>
                                )}
                                {ind.initial_authorization.criteria.length > 0 ? (
                                  <ul style={{ display: 'flex', flexDirection: 'column', gap: '5px', listStyle: 'none', padding: 0, margin: 0 }}>
                                    {ind.initial_authorization.criteria.map((c, j) => {
                                      const colors = criterionColors[c.criterion_type] ?? criterionColors.other
                                      return (
                                        <li key={j} style={{ display: 'flex', alignItems: 'flex-start', gap: '5px', fontSize: '11px' }}>
                                          <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: colors.bg, border: `1px solid ${colors.border}`, color: colors.text, flexShrink: 0 }}>
                                            {c.criterion_type.replace(/_/g, ' ')}
                                          </span>
                                          <span style={{ color: '#4A4845', lineHeight: 1.4 }}>{c.description}</span>
                                        </li>
                                      )
                                    })}
                                  </ul>
                                ) : (
                                  <p style={{ fontSize: '11px', color: '#918D88' }}>No specific criteria</p>
                                )}

                                {ind.reauthorization && (
                                  <div style={{ marginTop: '6px', paddingTop: '6px', borderTop: '1px dashed #D8D4CC' }}>
                                    <p style={{ ...LABEL, color: '#91bfeb', marginBottom: '5px' }}>
                                      Reauthorization{ind.reauthorization.authorization_duration_months != null && ` · ${ind.reauthorization.authorization_duration_months}mo`}
                                    </p>
                                    {ind.reauthorization.criteria.length > 0 ? (
                                      <ul style={{ display: 'flex', flexDirection: 'column', gap: '5px', listStyle: 'none', padding: 0, margin: 0 }}>
                                        {ind.reauthorization.criteria.map((c, j) => {
                                          const colors = criterionColors[c.criterion_type] ?? criterionColors.other
                                          return (
                                            <li key={j} style={{ display: 'flex', alignItems: 'flex-start', gap: '5px', fontSize: '11px' }}>
                                              <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: colors.bg, border: `1px solid ${colors.border}`, color: colors.text, flexShrink: 0 }}>
                                                {c.criterion_type.replace(/_/g, ' ')}
                                              </span>
                                              <span style={{ color: '#4A4845', lineHeight: 1.4 }}>{c.description}</span>
                                            </li>
                                          )
                                        })}
                                      </ul>
                                    ) : (
                                      <p style={{ fontSize: '11px', color: '#918D88' }}>No separate reauth criteria</p>
                                    )}
                                  </div>
                                )}

                                <ProvenanceChip payer={p.payer.name} policyTitle={p.payer.policy_title} />
                              </div>
                            ) : (
                              <p style={{ fontSize: '11px', color: '#D8D4CC' }}>Not covered</p>
                            )}
                          </div>
                        )
                      })}
                    </div>

                    {/* Exclusions */}
                    {sorted.some(p => p.exclusions && p.exclusions.length > 0) && (
                      <div style={{ display: 'grid', gridTemplateColumns: colTemplate, borderTop: '1px dashed #D8D4CC' }}>
                        <div style={{ padding: '8px 12px' }}>
                          <p style={{ ...LABEL, color: '#B81C1C' }}>Exclusions</p>
                        </div>
                        {sorted.map(p => (
                          <div key={p.payer.name} style={{ padding: '8px 12px', borderLeft: '1px solid #EBEBEB' }}>
                            {p.exclusions && p.exclusions.length > 0
                              ? p.exclusions.map((e, i) => <p key={i} style={{ fontSize: '11px', color: '#B81C1C' }}>• {e.description}</p>)
                              : <p style={{ fontSize: '11px', color: '#D8D4CC' }}>None</p>}
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
