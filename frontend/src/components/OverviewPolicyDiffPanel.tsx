import { useMemo, useState } from 'react'
import type { ChangeEntry } from '../types/policy'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'
import { CHANGE_TYPE_LABELS } from '../lib/formatters'

interface OverviewPolicyDiffPanelProps {
  portfolio: DrugPortfolioEntry[]
  changes: ChangeEntry[]
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

const severityPalette = {
  HIGH: { bg: '#FBEAEA', text: '#B81C1C', rail: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  MED:  { bg: '#F8EDDC', text: '#8B6428', rail: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  LOW:  { bg: '#E0F2E8', text: '#1A7840', rail: '#1A7840', border: 'rgba(26,120,64,0.2)' },
}


function formatDate(date: string) {
  return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(new Date(date))
}

function buildPolicyPairs(drug: DrugPortfolioEntry) {
  return drug.policies.map(policy => {
    const updateCount = drug.policies.length > 1
      ? Math.max(1, Math.round(policy.indications.length / 2))
      : 1
    return {
      id: policy.payer.name,
      label: `${policy.payer.name} latest revision`,
      caption: `${updateCount} material change${updateCount === 1 ? '' : 's'}`,
    }
  })
}

function getGraphicMix(changes: ChangeEntry[]) {
  const totals = { added: 0, changed: 0, removed: 0 }
  changes.forEach(change => {
    if (change.change_type.startsWith('ADDED'))   { totals.added   += 1; return }
    if (change.change_type.startsWith('REMOVED')) { totals.removed += 1; return }
    totals.changed += 1
  })
  const totalCount = changes.length || 1
  return [
    { label: 'Added',   value: totals.added,   color: '#1A7840', width: `${(totals.added   / totalCount) * 100}%` },
    { label: 'Changed', value: totals.changed,  color: '#8B6428', width: `${(totals.changed / totalCount) * 100}%` },
    { label: 'Removed', value: totals.removed,  color: '#B81C1C', width: `${(totals.removed / totalCount) * 100}%` },
  ]
}

function derivePolicyChangeScope(selectedDrug: DrugPortfolioEntry, allChanges: ChangeEntry[], selectedPair: string) {
  const matching = allChanges.filter(change =>
    change.drug.toLowerCase().includes(selectedDrug.brandName.toLowerCase()) ||
    change.drug.toLowerCase().includes(selectedDrug.genericName.toLowerCase())
  )
  const payerFiltered = matching.filter(change => change.payer === selectedPair)
  return payerFiltered.length > 0 ? payerFiltered : matching
}

export function OverviewPolicyDiffPanel({ portfolio, changes }: OverviewPolicyDiffPanelProps) {
  if (portfolio.length === 0) {
    return (
      <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '16px', display: 'flex', flexDirection: 'column', gap: '10px', minHeight: '280px', justifyContent: 'center' }}>
        <p style={{ ...LABEL, color: '#91bfeb' }}>Policy Diff</p>
        <h3 style={{ fontSize: '18px', fontWeight: 700, color: '#131210' }}>No live diff data yet</h3>
        <p style={{ fontSize: '12px', color: '#4A4845', lineHeight: 1.6 }}>
          This panel only uses live portfolio entries and live changelog records. Once real policies are available, payer diffs will appear here automatically.
        </p>
      </div>
    )
  }

  const [selectedDrugId, setSelectedDrugId] = useState(portfolio[1]?.id ?? portfolio[0]?.id ?? '')
  const selectedDrug = portfolio.find(entry => entry.id === selectedDrugId) ?? portfolio[0]
  const policyPairs  = useMemo(() => buildPolicyPairs(selectedDrug), [selectedDrug])
  const [selectedPair, setSelectedPair] = useState(policyPairs[0]?.id ?? '')

  const visibleChanges = useMemo(
    () => derivePolicyChangeScope(selectedDrug, changes, selectedPair || policyPairs[0]?.id || ''),
    [selectedDrug, changes, selectedPair, policyPairs]
  )

  const mix           = getGraphicMix(visibleChanges)
  const materialCount = visibleChanges.filter(change => change.severity !== 'LOW').length

  return (
    <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '16px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '12px' }}>
        <div>
          <p style={{ ...LABEL, color: '#91bfeb', marginBottom: '4px' }}>Policy Diff</p>
          <h3 style={{ fontSize: '16px', fontWeight: 700, color: '#131210', lineHeight: 1.2 }}>
            {selectedPair || policyPairs[0]?.label || 'Revision snapshot'}
          </h3>
        </div>
        <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '3px 8px', background: '#F8EDDC', border: '1px solid rgba(139,100,40,0.2)', color: '#8B6428', borderRadius: '1px', flexShrink: 0, marginTop: '2px' }}>
          {materialCount} material
        </span>
      </div>

      {/* Selects */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
        {[
          {
            label: 'Drug',
            value: selectedDrugId,
            options: portfolio.map(d => ({ value: d.id, label: d.brandName })),
            onChange: (v: string) => {
              const nextDrug  = portfolio.find(e => e.id === v) ?? portfolio[0]
              const nextPairs = buildPolicyPairs(nextDrug)
              setSelectedDrugId(v)
              setSelectedPair(nextPairs[0]?.id ?? '')
            },
          },
          {
            label: 'Compare focus',
            value: selectedPair || policyPairs[0]?.id || '',
            options: policyPairs.map(p => ({ value: p.id, label: p.label })),
            onChange: (v: string) => setSelectedPair(v),
          },
        ].map(({ label, value, options, onChange }) => (
          <label key={label} style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            <span style={{ ...LABEL }}>{label}</span>
            <select value={value} onChange={e => onChange(e.target.value)}
              style={{ background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px', color: '#131210', fontFamily: "'IBM Plex Sans',system-ui,sans-serif", fontSize: '13px', padding: '7px 10px', outline: 'none' }}>
              {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </label>
        ))}
      </div>

      {/* Change mix */}
      <div style={{ background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '12px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
          <p style={{ ...LABEL }}>Change mix</p>
          <p style={{ ...mono, fontSize: '10px', color: '#918D88' }}>{visibleChanges.length} updates</p>
        </div>

        {/* Mix bar */}
        <div style={{ height: '4px', background: '#D8D4CC', borderRadius: '0', display: 'flex', overflow: 'hidden', marginBottom: '10px' }}>
          {mix.map(segment => segment.value > 0 && (
            <div key={segment.label} title={`${segment.label}: ${segment.value}`}
              style={{ width: segment.width, background: segment.color }} />
          ))}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '6px' }}>
          {mix.map(segment => (
            <div key={segment.label} style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '8px 10px' }}>
              <p style={{ ...LABEL, marginBottom: '4px' }}>{segment.label}</p>
              <p style={{ ...mono, fontSize: '18px', fontWeight: 600, color: segment.color, lineHeight: 1 }}>{segment.value}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Change entries */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
        {visibleChanges.map(change => {
          const style = severityPalette[change.severity]
          return (
            <div key={`${change.drug}-${change.payer}-${change.change_type}-${change.date}`}
              style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderLeft: `3px solid ${style.rail}`, borderRadius: '2px', padding: '10px 12px' }}>
              <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '6px', marginBottom: '6px' }}>
                <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: style.bg, border: `1px solid ${style.border}`, color: style.text, letterSpacing: '0.08em', textTransform: 'uppercase' as const }}>
                  {change.change_type.startsWith('ADDED') ? 'Added' : change.change_type.startsWith('REMOVED') ? 'Removed' : 'Changed'}
                </span>
                <span style={{ fontSize: '12px', fontWeight: 600, color: '#131210' }}>
                  {CHANGE_TYPE_LABELS[change.change_type] ?? change.change_type}
                </span>
                <span style={{ ...mono, fontSize: '9px', color: '#918D88', marginLeft: 'auto' }}>{formatDate(change.date)}</span>
              </div>
              <p style={{ fontSize: '12px', lineHeight: 1.6, color: '#4A4845' }}>{change.summary}</p>
            </div>
          )
        })}
      </div>
    </div>
  )
}
