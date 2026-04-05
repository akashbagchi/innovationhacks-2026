import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts'
import type { PolicyRecord } from '../types/policy'
import { computeStringency } from '../lib/stringency'

interface CriteriaBreakdownProps {
  policies: PolicyRecord[]
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

const PAYER_COLORS = ['#91bfeb', '#2870A8', '#CD944F']

const CRITERION_COLORS: Record<string, string> = {
  combination_restriction: '#4A4845',
  prior_therapy:           '#CD944F',
  line_of_therapy:         '#2870A8',
  disease_severity:        '#B81C1C',
  lab_value:               '#2870A8',
  diagnosis:               '#918D88',
  step_therapy:            '#CD944F',
  prescriber:              '#2870A8',
  clinical_response:       '#2870A8',
  other:                   '#918D88',
}

const CRITERION_LABELS: Record<string, string> = {
  combination_restriction: 'Combination restriction',
  prior_therapy:           'Prior therapy failure',
  line_of_therapy:         'Line of therapy',
  disease_severity:        'Disease severity',
  lab_value:               'Lab value / biomarker',
  diagnosis:               'Diagnosis confirmation',
  step_therapy:            'Step therapy',
  prescriber:              'Prescriber specialty',
  clinical_response:       'Clinical response',
  other:                   'Other',
}

export function CriteriaBreakdown({ policies }: CriteriaBreakdownProps) {
  const barData = [
    { label: 'PA Burden',    key: 'pa' },
    { label: 'Criteria',     key: 'criteriaType' },
    { label: 'Auth Duration', key: 'duration' },
    { label: 'Exclusions',   key: 'exclusions' },
  ].map(dim => {
    const entry: Record<string, string | number> = { label: dim.label }
    policies.forEach(p => {
      const { breakdown } = computeStringency(p)
      entry[p.payer.name] = breakdown[dim.key as keyof typeof breakdown]
    })
    return entry
  })

  const barrierCounts: Record<string, number> = {}
  policies.forEach(p => {
    p.indications.forEach(ind => {
      ind.initial_authorization.criteria.forEach(c => {
        barrierCounts[c.criterion_type] = (barrierCounts[c.criterion_type] ?? 0) + 1
      })
    })
  })
  const totalCriteria  = Object.values(barrierCounts).reduce((a, b) => a + b, 0)
  const rankedBarriers = Object.entries(barrierCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([type, count]) => ({ type, count, pct: Math.round((count / totalCriteria) * 100) }))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Header */}
      <div>
        <p style={{ ...LABEL, color: '#91bfeb', marginBottom: '4px' }}>Criteria Analysis</p>
        <p style={{ fontSize: '14px', fontWeight: 700, color: '#131210' }}>Restriction Profile &amp; Access Barrier Composition</p>
      </div>

      {/* Bar chart */}
      <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '16px' }}>
        <p style={{ fontSize: '12px', fontWeight: 600, color: '#131210', marginBottom: '2px' }}>Restriction Dimensions by Payer</p>
        <p style={{ ...LABEL, marginBottom: '16px' }}>{policies.length} payer{policies.length !== 1 ? 's' : ''} · 4 dimensions</p>
        <ResponsiveContainer width="100%" height={220}>
          <BarChart data={barData} barGap={2} barCategoryGap="30%">
            <CartesianGrid vertical={false} stroke="#EBEBEB" />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 11, fill: '#4A4845', fontFamily: "'IBM Plex Sans', system-ui, sans-serif" }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 10, fill: '#918D88', fontFamily: "'IBM Plex Mono', monospace" }}
              axisLine={false}
              tickLine={false}
              width={28}
            />
            <Tooltip
              cursor={{ fill: 'rgba(239,80,80,0.04)' }}
              contentStyle={{
                background: '#131210',
                border: 'none',
                borderRadius: '2px',
                fontSize: '11px',
                color: '#FFFFFF',
                fontFamily: "'IBM Plex Mono', monospace",
              }}
              labelStyle={{ color: '#918D88', marginBottom: '4px' }}
            />
            <Legend wrapperStyle={{ fontSize: '11px', color: '#4A4845', paddingTop: '12px', fontFamily: "'IBM Plex Sans', system-ui, sans-serif" }} />
            {policies.map((p, i) => (
              <Bar key={p.payer.name} dataKey={p.payer.name} fill={PAYER_COLORS[i]} radius={[0, 0, 0, 0]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Ranked barriers */}
      <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '16px' }}>
        <p style={{ fontSize: '12px', fontWeight: 600, color: '#131210', marginBottom: '2px' }}>Ranked Access Barriers</p>
        <p style={{ ...LABEL, marginBottom: '16px' }}>By frequency across all payer × indication combinations</p>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
          {rankedBarriers.map(({ type, count, pct }, i) => {
            const color = CRITERION_COLORS[type] ?? '#918D88'
            const label = CRITERION_LABELS[type] ?? type.replace(/_/g, ' ')
            return (
              <div key={type} style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                <span style={{ ...mono, fontSize: '10px', fontWeight: 700, color: '#D8D4CC', width: '14px', flexShrink: 0, textAlign: 'right' }}>
                  {i + 1}
                </span>
                <span style={{ fontSize: '11px', color: '#131210', width: '160px', flexShrink: 0 }}>{label}</span>
                <div style={{ flex: 1, height: '3px', background: '#EBEBEB' }}>
                  <div style={{ height: '100%', width: `${pct}%`, background: color, transition: 'width 0.2s' }} />
                </div>
                <span style={{ ...mono, fontSize: '10px', color: '#4A4845', width: '60px', textAlign: 'right' }}>
                  {count} <span style={{ color: '#918D88' }}>({pct}%)</span>
                </span>
              </div>
            )
          })}
        </div>
        <p style={{ ...LABEL, marginTop: '14px', paddingTop: '10px', borderTop: '1px solid #EBEBEB' }}>
          Count = payer × indication combinations containing this criterion type
        </p>
      </div>
    </div>
  )
}
