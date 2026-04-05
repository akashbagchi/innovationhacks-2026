import { useMemo, useState } from 'react'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from './ui/dialog'

interface OverviewMatrixPanelProps {
  portfolio: DrugPortfolioEntry[]
}

type MatrixColumn = 'coverage' | 'pa' | 'stepTherapy' | 'criterionTypes' | 'duration' | 'confidence' | 'updated'

interface MatrixRow {
  payer: string
  coverage: string
  pa: string
  stepTherapy: string
  criterionTypes: string[]
  duration: string
  confidence: string
  updated: string
}

const DEFAULT_COLUMNS: MatrixColumn[] = ['coverage', 'pa', 'stepTherapy', 'criterionTypes', 'duration', 'confidence', 'updated']

const columnLabels: Record<MatrixColumn, string> = {
  coverage: 'Coverage', pa: 'PA', stepTherapy: 'Step Therapy',
  criterionTypes: 'Criterion Types', duration: 'Auth Duration',
  confidence: 'Confidence', updated: 'Updated',
}

const coverageStyles: Record<string, { bg: string; text: string; border: string }> = {
  'PA Required': { bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  'No PA':       { bg: '#E0F2E8', text: '#1A7840', border: 'rgba(26,120,64,0.2)' },
  'No Policy':   { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
}

const criterionChipColors: Record<string, { bg: string; text: string; border: string }> = {
  step_therapy:            { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  combination_restriction: { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  prior_therapy:           { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  line_of_therapy:         { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  disease_severity:        { bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  lab_value:               { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  diagnosis:               { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  prescriber:              { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  clinical_response:       { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  other:                   { bg: '#F0EFEB', text: '#918D88', border: '#D8D4CC' },
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

function formatDate(date?: string) {
  if (!date) return 'Unknown'
  return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(new Date(date))
}

function normalizeConfidence(score: number) {
  return score >= 0.9 ? 'High' : score >= 0.75 ? 'Medium' : 'Low'
}

function buildMatrixRows(drug: DrugPortfolioEntry, indicationFilter: string): MatrixRow[] {
  return drug.policies.map(policy => {
    const indication = policy.indications.find(item => item.name === indicationFilter)
    if (!indication) {
      return {
        payer: policy.payer.name, coverage: 'No Policy', pa: 'Unknown', stepTherapy: 'Unknown',
        criterionTypes: [], duration: '—', confidence: normalizeConfidence(policy.confidence_scores.overall),
        updated: formatDate(policy.payer.revision_date ?? policy.payer.effective_date),
      }
    }
    const uniqueTypes = Array.from(new Set(indication.initial_authorization.criteria.map(c => c.criterion_type)))
    const dur         = indication.initial_authorization.authorization_duration_months
    return {
      payer: policy.payer.name,
      coverage: indication.pa_required ? 'PA Required' : 'No PA',
      pa: indication.pa_required ? 'Required' : 'Not required',
      stepTherapy: indication.step_therapy_required ? 'Yes' : 'No',
      criterionTypes: uniqueTypes, duration: dur != null ? `${dur}mo` : '—',
      confidence: normalizeConfidence(policy.confidence_scores.overall),
      updated: formatDate(policy.payer.revision_date ?? policy.payer.effective_date),
    }
  })
}

function MatrixTable({ rows, visibleColumns, previewCount }: { rows: MatrixRow[]; visibleColumns: MatrixColumn[]; previewCount?: number }) {
  const displayedRows = typeof previewCount === 'number' ? rows.slice(0, previewCount) : rows

  return (
    <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', overflow: 'hidden' }}>
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #D8D4CC', background: '#F0EFEB' }}>
              <th style={{ ...LABEL, padding: '8px 12px' }}>Payer</th>
              {visibleColumns.map(col => (
                <th key={col} style={{ ...LABEL, padding: '8px 10px' }}>{columnLabels[col]}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayedRows.map((row, ri) => (
              <tr key={row.payer} style={{ borderBottom: ri < displayedRows.length - 1 ? '1px solid #EBEBEB' : 'none', background: ri % 2 === 1 ? '#F7F5F0' : '#FFFFFF' }}>
                <td style={{ padding: '8px 12px', fontSize: '12px', fontWeight: 600, color: '#131210' }}>{row.payer}</td>
                {visibleColumns.map(col => {
                  if (col === 'coverage') {
                    const s = coverageStyles[row.coverage] ?? coverageStyles['No Policy']
                    return (
                      <td key={col} style={{ padding: '8px 10px' }}>
                        <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '2px 7px', borderRadius: '1px', background: s.bg, color: s.text, border: `1px solid ${s.border}` }}>
                          {row.coverage}
                        </span>
                      </td>
                    )
                  }
                  if (col === 'stepTherapy') {
                    const isYes = row.stepTherapy === 'Yes'
                    return (
                      <td key={col} style={{ padding: '8px 10px' }}>
                        <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '2px 7px', borderRadius: '1px', background: isYes ? '#FBEAEA' : '#F0EFEB', color: isYes ? '#B81C1C' : '#918D88', border: `1px solid ${isYes ? 'rgba(184,28,28,0.2)' : '#D8D4CC'}` }}>
                          {row.stepTherapy}
                        </span>
                      </td>
                    )
                  }
                  if (col === 'criterionTypes') {
                    return (
                      <td key={col} style={{ padding: '8px 10px' }}>
                        {row.criterionTypes.length > 0 ? (
                          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '3px' }}>
                            {row.criterionTypes.map(type => {
                              const chip = criterionChipColors[type] ?? criterionChipColors.other
                              return (
                                <span key={type} style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: chip.bg, border: `1px solid ${chip.border}`, color: chip.text, whiteSpace: 'nowrap' }}>
                                  {type.replace(/_/g, ' ')}
                                </span>
                              )
                            })}
                          </div>
                        ) : <span style={{ ...LABEL }}>—</span>}
                      </td>
                    )
                  }
                  const value = row[col as 'pa' | 'duration' | 'confidence' | 'updated']
                  return <td key={col} style={{ padding: '8px 10px', fontSize: '12px', color: '#4A4845' }}>{value}</td>
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function OverviewMatrixPanel({ portfolio }: OverviewMatrixPanelProps) {
  const [selectedDrugId, setSelectedDrugId]     = useState(portfolio[1]?.id ?? portfolio[0]?.id ?? '')
  const selectedDrug                             = portfolio.find(e => e.id === selectedDrugId) ?? portfolio[0]
  const indicationOptions                        = useMemo(() => Array.from(new Set(selectedDrug.policies.flatMap(p => p.indications.map(i => i.name)))), [selectedDrug])
  const [selectedIndication, setSelectedIndication] = useState(indicationOptions[0] ?? '')
  const [visibleColumns, setVisibleColumns]      = useState<MatrixColumn[]>(DEFAULT_COLUMNS)
  const activeIndication                         = selectedIndication || indicationOptions[0] || ''
  const matrixRows                               = useMemo(() => buildMatrixRows(selectedDrug, activeIndication), [selectedDrug, activeIndication])

  function toggleColumn(col: MatrixColumn) {
    setVisibleColumns(cur => cur.includes(col) ? cur.filter(c => c !== col) : [...cur, col])
  }

  return (
    <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '16px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
      <div>
        <p style={{ ...LABEL, color: '#EF5050', marginBottom: '4px' }}>Cross-Payer Matrix</p>
        <h3 style={{ fontSize: '18px', fontWeight: 700, color: '#131210', marginBottom: '4px' }}>{selectedDrug.brandName} coverage snapshot</h3>
        <p style={{ fontSize: '12px', color: '#4A4845', lineHeight: 1.6 }}>Choose the drug, narrow to an indication, and select which columns to show.</p>
      </div>

      {/* Selects */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
        {[
          { label: 'Drug', value: selectedDrugId, options: portfolio.map(d => ({ value: d.id, label: d.brandName })),
            onChange: (v: string) => { const nd = portfolio.find(e => e.id === v); const ni = Array.from(new Set(nd?.policies.flatMap(p => p.indications.map(i => i.name)) ?? [])); setSelectedDrugId(v); setSelectedIndication(ni[0] ?? '') } },
          { label: 'Indication', value: activeIndication, options: indicationOptions.map(i => ({ value: i, label: i })),
            onChange: (v: string) => setSelectedIndication(v) },
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

      {/* Column toggles */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '5px' }}>
        {DEFAULT_COLUMNS.map(col => {
          const active = visibleColumns.includes(col)
          return (
            <button key={col} type="button" onClick={() => toggleColumn(col)}
              style={{ ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', padding: '3px 9px', borderRadius: '1px', border: '1px solid', cursor: 'pointer', transition: 'all 0.1s', background: active ? '#131210' : '#F0EFEB', color: active ? '#FFFFFF' : '#4A4845', borderColor: active ? '#131210' : '#D8D4CC' }}>
              {columnLabels[col]}
            </button>
          )
        })}
      </div>

      <MatrixTable rows={matrixRows} visibleColumns={visibleColumns} previewCount={4} />

      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', paddingTop: '12px', borderTop: '1px solid #EBEBEB' }}>
        <p style={{ fontSize: '12px', color: '#4A4845' }}>Showing first {Math.min(4, matrixRows.length)} payer rows.</p>
        <Dialog>
          <DialogTrigger asChild>
            <button type="button" style={{ background: '#131210', color: '#FFFFFF', border: 'none', borderRadius: '2px', padding: '7px 14px', fontSize: '12px', fontWeight: 600, cursor: 'pointer' }}>
              View full matrix
            </button>
          </DialogTrigger>
          <DialogContent style={{ maxWidth: '1100px', borderRadius: '2px', border: '1px solid #D8D4CC', background: '#FFFFFF', padding: 0, color: '#131210' }}>
            <div style={{ borderBottom: '1px solid #D8D4CC', padding: '20px 24px' }}>
              <DialogHeader className="gap-2">
                <p style={{ ...LABEL, color: '#EF5050' }}>Cross-Payer Matrix</p>
                <DialogTitle style={{ fontSize: '18px', fontWeight: 700, color: '#131210' }}>Full {selectedDrug.brandName} matrix for {activeIndication}</DialogTitle>
                <DialogDescription style={{ fontSize: '13px', color: '#4A4845' }}>Expanded view with current column and indication selections.</DialogDescription>
              </DialogHeader>
            </div>
            <div style={{ maxHeight: '70vh', overflowY: 'auto', padding: '20px 24px' }}>
              <MatrixTable rows={matrixRows} visibleColumns={visibleColumns} />
            </div>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  )
}
