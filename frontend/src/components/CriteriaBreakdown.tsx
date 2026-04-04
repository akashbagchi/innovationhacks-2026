import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell,
} from 'recharts'
import type { PolicyRecord } from '../types/policy'
import { computeStringency } from '../lib/stringency'

interface CriteriaBreakdownProps {
  policies: PolicyRecord[]
}

const PAYER_COLORS = ['#2D6A90', '#10A090', '#D97706']

const CRITERION_COLORS: Record<string, string> = {
  combination_restriction: '#7C3AED',
  prior_therapy:           '#D97706',
  line_of_therapy:         '#2D6A90',
  disease_severity:        '#DC2626',
  lab_value:               '#10A090',
  diagnosis:               '#6B7583',
  step_therapy:            '#EA580C',
  prescriber:              '#16A34A',
  clinical_response:       '#0891B2',
  other:                   '#9AA3AF',
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
  // Grouped bar chart data — 4 dimensions × 3 payers
  const barData = [
    { label: 'PA Burden', key: 'pa' },
    { label: 'Step Therapy', key: 'stepTherapy' },
    { label: 'Criteria Volume', key: 'criteriaVolume' },
    { label: 'Exclusions', key: 'exclusions' },
  ].map(dim => {
    const entry: Record<string, string | number> = { label: dim.label }
    policies.forEach(p => {
      const { breakdown } = computeStringency(p)
      entry[p.payer.name] = breakdown[dim.key as keyof typeof breakdown]
    })
    return entry
  })

  // Ranked barrier analysis — criterion type × (payer × indication) count
  const barrierCounts: Record<string, number> = {}
  policies.forEach(p => {
    p.indications.forEach(ind => {
      ind.initial_authorization.criteria.forEach(c => {
        barrierCounts[c.criterion_type] = (barrierCounts[c.criterion_type] ?? 0) + 1
      })
    })
  })
  const totalCriteria = Object.values(barrierCounts).reduce((a, b) => a + b, 0)
  const rankedBarriers = Object.entries(barrierCounts)
    .sort((a, b) => b[1] - a[1])
    .map(([type, count]) => ({ type, count, pct: Math.round((count / totalCriteria) * 100) }))

  return (
    <div className="space-y-6">
      {/* Section header */}
      <div>
        <p className="text-[10px] font-semibold uppercase tracking-widest mb-1" style={{ color: '#2D6A90' }}>
          Criteria Analysis
        </p>
        <p className="text-sm font-semibold" style={{ color: '#0E1117' }}>Restriction Profile &amp; Access Barrier Composition</p>
      </div>

      {/* Grouped bar chart */}
      <div className="rounded-xl p-5" style={{ background: '#fff', border: '1px solid #E2E7EF' }}>
        <p className="text-xs font-semibold mb-0.5" style={{ color: '#0E1117' }}>Restriction Dimensions by Payer</p>
        <p className="text-[11px] mb-4" style={{ color: '#6B7583' }}>
          4 dimensions × {policies.length} payers — compare directly where each payer restricts access
        </p>
        <ResponsiveContainer width="100%" height={240}>
          <BarChart data={barData} barGap={2} barCategoryGap="30%">
            <CartesianGrid vertical={false} stroke="#E2E7EF" />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 11, fill: '#6B7583' }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 10, fill: '#9AA3AF' }}
              axisLine={false}
              tickLine={false}
              width={28}
            />
            <Tooltip
              cursor={{ fill: 'rgba(10,77,140,0.04)' }}
              contentStyle={{
                background: '#0E1117',
                border: 'none',
                borderRadius: '8px',
                fontSize: '11px',
                color: '#fff',
              }}
              labelStyle={{ color: '#9AA3AF', marginBottom: '4px' }}
            />
            <Legend wrapperStyle={{ fontSize: '11px', color: '#6B7583', paddingTop: '12px' }} />
            {policies.map((p, i) => (
              <Bar key={p.payer.name} dataKey={p.payer.name} fill={PAYER_COLORS[i]} radius={[3, 3, 0, 0]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Ranked barrier analysis */}
      <div className="rounded-xl p-5" style={{ background: '#fff', border: '1px solid #E2E7EF' }}>
        <p className="text-xs font-semibold mb-0.5" style={{ color: '#0E1117' }}>Ranked Access Barriers</p>
        <p className="text-[11px] mb-5" style={{ color: '#6B7583' }}>
          Criterion types ranked by frequency across all payer × indication combinations
        </p>
        <div className="space-y-3">
          {rankedBarriers.map(({ type, count, pct }, i) => {
            const color = CRITERION_COLORS[type] ?? '#9AA3AF'
            const label = CRITERION_LABELS[type] ?? type.replace(/_/g, ' ')
            return (
              <div key={type} className="flex items-center gap-3">
                {/* Rank */}
                <span className="text-[11px] font-bold tabular-nums w-4 flex-shrink-0 text-right"
                  style={{ color: '#C0CDD9' }}>
                  {i + 1}
                </span>
                {/* Label */}
                <span className="text-xs w-44 flex-shrink-0" style={{ color: '#334155' }}>{label}</span>
                {/* Bar */}
                <div className="flex-1 h-2 rounded-full" style={{ background: '#E2E7EF' }}>
                  <div className="h-full rounded-full transition-all"
                    style={{ width: `${pct}%`, background: color }} />
                </div>
                {/* Count + pct */}
                <span className="text-[11px] tabular-nums w-16 text-right" style={{ color: '#6B7583' }}>
                  {count} <span style={{ color: '#C0CDD9' }}>({pct}%)</span>
                </span>
              </div>
            )
          })}
        </div>
        <p className="text-[10px] mt-4 pt-3" style={{ borderTop: '1px solid #E2E7EF', color: '#9AA3AF' }}>
          Count = number of (payer × indication) combinations containing this criterion type
        </p>
      </div>
    </div>
  )
}
