import { useState, type ReactNode } from 'react'
import { motion } from 'framer-motion'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'
import { computeStringency } from '../lib/stringency'
import type { ChangeEntry } from '../types/policy'
import { SparkLine } from './SparkLine'
import { CoverageHeatmap } from './CoverageHeatmap'
import { RecentChangeFeed } from './RecentChangeFeed'
import { OverviewMatrixPanel } from './OverviewMatrixPanel'
import { OverviewPolicyDiffPanel } from './OverviewPolicyDiffPanel'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from './ui/dialog'

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

interface PortfolioViewProps {
  portfolio: DrugPortfolioEntry[]
  onSelectDrug: (id: string) => void
  changes: ChangeEntry[]
}

interface StatCardData {
  label: string
  value: number
  sub: string
  accentLeft?: string
  viz?: { label: string; segments: Array<{ value: number; color: string; tooltip: string }> }
  details?: {
    eyebrow: string; title: string; description: string; emptyMessage: string
    items: Array<{ kind: string; title: string; badge: string; description: string; supporting: string }>
  }
}

function shortPayer(name: string) {
  if (name === 'Blue Cross NC') return 'BCNC'
  if (name === 'UnitedHealth') return 'UHC'
  return name
}

function formatChangeDate(date: string) {
  return new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric' }).format(new Date(date))
}

function summarizeDirection(change: ChangeEntry) {
  const text = `${change.change_type} ${change.summary}`.toLowerCase()
  if (/(removed|waive|loosen|expanded|broader|without pa|no clinical criteria changes)/.test(text)) return 'Loosening'
  if (/(added|stricter|reduced|must|required|step therapy|threshold)/.test(text)) return 'Tightening'
  return 'Material update'
}

function buildStats(portfolio: DrugPortfolioEntry[], changes: ChangeEntry[]): StatCardData[] {
  const policiesTracked = portfolio.length * 3
  const highImpact      = changes.filter(c => c.severity === 'HIGH').length
  const tighteningCount = portfolio.flatMap(d => d.trends).filter(t => t.direction === 'tightening').length
  const looseningCount  = portfolio.flatMap(d => d.trends).filter(t => t.direction === 'loosening').length
  const outlierPolicies = portfolio.flatMap(d => d.trends).filter(t => Math.abs(t.delta) > 8).length

  const highImpactItems = changes.filter(c => c.severity === 'HIGH').map(c => ({
    kind: 'high-impact', title: `${c.payer} changed ${c.drug}`,
    badge: summarizeDirection(c), description: c.summary,
    supporting: `${c.change_type.replaceAll('_', ' ')} | ${formatChangeDate(c.date)}`,
  }))

  const outlierItems = portfolio.flatMap(drug =>
    drug.trends.filter(t => Math.abs(t.delta) > 8).map(t => ({
      kind: 'outlier', title: `${t.payerName} is an outlier for ${drug.brandName}`,
      badge: t.direction === 'loosening' ? 'Looser than peers' : 'Stricter than peers',
      description: `${t.payerName} is trending ${t.direction === 'loosening' ? 'looser' : 'stricter'} than the rest of the market this quarter.`,
      supporting: `${Math.abs(t.delta)} points ${t.direction === 'loosening' ? 'below' : 'above'} its prior baseline`,
    }))
  )

  return [
    {
      label: 'Policies Tracked', value: policiesTracked, sub: '+0 this quarter',
      viz: { label: 'Portfolio mix', segments: portfolio.map((d, i) => ({ value: d.policies.length, color: ['#1A7840', '#8B6428', '#7BA8C4'][i % 3], tooltip: `${d.brandName}: ${d.policies.length} policies` })) },
    },
    {
      label: 'Payers Monitored', value: 3, sub: 'Commercial + Medicare LCDs',
      viz: { label: 'Coverage source mix', segments: [{ value: 2, color: '#1A7840', tooltip: 'Commercial payers' }, { value: 1, color: '#A8D4C8', tooltip: 'Medicare / LCD' }] },
    },
    {
      label: 'High-Impact Changes', value: highImpact, sub: `${tighteningCount} tightening, ${looseningCount} loosening`,
      accentLeft: '#B81C1C',
      viz: { label: 'Direction', segments: [{ value: tighteningCount, color: '#B81C1C', tooltip: `${tighteningCount} tightening` }, { value: looseningCount, color: '#1A7840', tooltip: `${looseningCount} loosening` }] },
      details: { eyebrow: 'Quarterly change audit', title: 'What is driving the high-impact count', description: 'These are the specific payer edits behind the summary.', emptyMessage: 'No high-impact changes detected.', items: highImpactItems },
    },
    {
      label: 'Outlier Policies', value: outlierPolicies, sub: 'Unique restrictions flagged',
      accentLeft: '#8B6428',
      viz: { label: 'Outlier direction', segments: [{ value: outlierItems.filter(i => i.badge.includes('Stricter')).length, color: '#8B6428', tooltip: 'Stricter than peers' }, { value: outlierItems.filter(i => i.badge.includes('Looser')).length, color: '#7BA8C4', tooltip: 'Looser than peers' }] },
      details: { eyebrow: 'Cross-payer anomaly review', title: 'Why these policies are considered outliers', description: 'Each outlier compares one payer against peers for the same drug.', emptyMessage: 'No outlier patterns flagged.', items: outlierItems },
    },
  ]
}

function StatCard({ stat, index }: { stat: StatCardData; index: number }) {
  const [hoveredViz, setHoveredViz] = useState<string | null>(null)
  const valueColor = stat.accentLeft === '#B81C1C' ? '#B81C1C' : stat.accentLeft === '#8B6428' ? '#8B6428' : '#131210'

  const cardBody = (
    <div className="h-full p-4 text-left" style={{
      background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px',
      borderLeft: stat.accentLeft ? `3px solid ${stat.accentLeft}` : '1px solid #D8D4CC',
    }}>
      <div className="flex flex-col h-full">
        <p style={{ ...LABEL, marginBottom: '7px' }}>{stat.label}</p>
        <p style={{ ...mono, fontSize: '34px', fontWeight: 600, lineHeight: 1, letterSpacing: '-0.02em', color: valueColor, marginBottom: '5px' }}>{stat.value}</p>
        <p style={{ fontSize: '11px', color: '#4A4845' }}>{stat.sub}</p>

        {stat.viz && stat.viz.segments.some(s => s.value > 0) && (
          <div className="mt-3">
            <p style={{ ...LABEL, marginBottom: '5px' }}>{stat.viz.label}</p>
            <div className="relative">
              <div style={{ display: 'flex', height: '3px', background: '#EBEBEB', borderRadius: '1px', gap: '1px', overflow: 'hidden' }}>
                {stat.viz.segments.map(seg => {
                  const total = stat.viz!.segments.reduce((s, i) => s + i.value, 0) || 1
                  if (seg.value <= 0) return null
                  return <div key={seg.tooltip}
                    onMouseEnter={() => setHoveredViz(seg.tooltip)}
                    onMouseLeave={() => setHoveredViz(null)}
                    style={{ width: `${(seg.value / total) * 100}%`, background: seg.color, height: '100%' }} />
                })}
              </div>
              {hoveredViz && (
                <div className="absolute left-0 top-4 z-10 pointer-events-none px-2 py-1"
                  style={{ background: '#131210', color: '#FFFFFF', borderRadius: '2px', fontSize: '11px' }}>
                  {hoveredViz}
                </div>
              )}
            </div>
          </div>
        )}

        {stat.details && <p style={{ ...LABEL, color: '#EF5050', marginTop: '12px', letterSpacing: '0.12em' }}>View details →</p>}
      </div>
    </div>
  )

  const wrapped = (children: ReactNode) => (
    <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05, duration: 0.15 }} className="h-full">
      {children}
    </motion.div>
  )

  if (!stat.details) return wrapped(cardBody)

  return wrapped(
    <Dialog>
      <DialogTrigger asChild>
        <button type="button" className="h-full w-full text-left focus:outline-none" style={{ borderRadius: '2px' }}>
          {cardBody}
        </button>
      </DialogTrigger>
      <DialogContent style={{ maxWidth: '720px', borderRadius: '2px', border: '1px solid #D8D4CC', background: '#FFFFFF', padding: 0, color: '#131210', boxShadow: '0 4px 32px rgba(26,23,20,0.12)' }}>
        <div style={{ borderBottom: '1px solid #D8D4CC', padding: '20px 24px' }}>
          <DialogHeader className="gap-2">
            <p style={{ ...LABEL, color: '#EF5050' }}>{stat.details.eyebrow}</p>
            <DialogTitle style={{ fontSize: '18px', fontWeight: 700, color: '#131210' }}>{stat.details.title}</DialogTitle>
            <DialogDescription style={{ fontSize: '13px', lineHeight: 1.6, color: '#4A4845' }}>{stat.details.description}</DialogDescription>
          </DialogHeader>
        </div>
        <div className="overflow-y-auto space-y-2" style={{ maxHeight: '60vh', padding: '20px 24px' }}>
          {stat.details.items.length === 0
            ? <p style={{ fontSize: '13px', color: '#918D88', textAlign: 'center', padding: '24px' }}>{stat.details.emptyMessage}</p>
            : stat.details.items.map(item => (
              <div key={`${item.kind}-${item.title}`}
                style={{ background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '14px' }}>
                <div className="flex items-start justify-between gap-3 mb-2">
                  <p style={{ fontSize: '13px', fontWeight: 600, color: '#131210' }}>{item.title}</p>
                  <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase' as const, padding: '2px 7px', borderRadius: '1px', flexShrink: 0, background: item.kind === 'high-impact' ? '#FBEAEA' : '#F8EDDC', color: item.kind === 'high-impact' ? '#B81C1C' : '#8B6428', border: `1px solid ${item.kind === 'high-impact' ? 'rgba(184,28,28,0.2)' : 'rgba(139,100,40,0.2)'}` }}>
                    {item.badge}
                  </span>
                </div>
                <p style={{ fontSize: '12px', lineHeight: 1.6, color: '#4A4845', marginBottom: '8px' }}>{item.description}</p>
                <p style={{ ...LABEL }}>{item.supporting}</p>
              </div>
            ))
          }
        </div>
      </DialogContent>
    </Dialog>
  )
}

function DrugCard({ drug, onSelect }: { drug: DrugPortfolioEntry; onSelect: () => void }) {
  const scores   = drug.policies.map(p => computeStringency(p))
  const maxScore = Math.max(...scores.map(s => s.score))
  const maxColor = maxScore >= 70 ? '#B81C1C' : maxScore >= 40 ? '#8B6428' : '#1A7840'

  return (
    <motion.button onClick={onSelect} className="w-full text-left"
      style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '13px' }}
      whileHover={{ borderColor: '#EF5050' }}>

      <div className="flex items-start justify-between mb-2">
        <div>
          <p style={{ fontSize: '14px', fontWeight: 700, color: '#131210', lineHeight: 1.2 }}>{drug.brandName}</p>
          <p style={{ ...mono, fontSize: '10px', color: '#918D88', marginTop: '2px' }}>{drug.genericName} · {drug.jCode}</p>
        </div>
        {drug.changeCount > 0 && (
          <div className="text-right flex-shrink-0">
            <p style={{ ...mono, fontSize: '20px', fontWeight: 600, color: '#B81C1C', lineHeight: 1 }}>{drug.changeCount}</p>
            <p style={{ ...mono, fontSize: '9px', color: '#918D88', textTransform: 'uppercase', letterSpacing: '0.08em' }}>changes</p>
          </div>
        )}
      </div>

      <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.08em', textTransform: 'uppercase' as const, padding: '2px 7px', background: '#F0EFEB', border: '1px solid #D8D4CC', color: '#4A4845', borderRadius: '1px', display: 'inline-block', marginBottom: '10px' }}>
        {drug.drugClass}
      </span>

      <div className="mb-3">
        <p style={{ ...LABEL, marginBottom: '5px' }}>PA Burden</p>
        {drug.policies.map(policy => {
          const paCount = policy.indications.filter(i => i.pa_required).length
          const pct     = Math.round((paCount / policy.indications.length) * 100)
          const barColor = pct > 70 ? '#B81C1C' : pct > 40 ? '#8B6428' : '#1A7840'
          return (
            <div key={policy.payer.name} className="flex items-center gap-2 mb-1">
              <span style={{ ...mono, fontSize: '9px', color: '#918D88', width: '34px', flexShrink: 0 }}>{shortPayer(policy.payer.name)}</span>
              <div style={{ flex: 1, height: '3px', background: '#EBEBEB', borderRadius: '1px', overflow: 'hidden' }}>
                <motion.div style={{ height: '100%', background: barColor, borderRadius: '1px' }}
                  initial={{ width: 0 }} animate={{ width: `${pct}%` }} transition={{ duration: 0.35, ease: 'easeOut' }} />
              </div>
              <span style={{ ...mono, fontSize: '9px', color: '#4A4845', width: '26px', textAlign: 'right' }}>{pct}%</span>
            </div>
          )
        })}
      </div>

      <div className="mb-2 pt-2" style={{ borderTop: '1px solid #EBEBEB' }}>
        <p style={{ ...LABEL, marginBottom: '4px' }}>Trend (4Q)</p>
        <div className="flex flex-wrap gap-3">
          {drug.trends.map(trend => {
            const tc = trend.direction === 'tightening' ? '#B81C1C' : trend.direction === 'loosening' ? '#1A7840' : '#918D88'
            const ar = trend.direction === 'tightening' ? '↑' : trend.direction === 'loosening' ? '↓' : '→'
            return (
              <div key={trend.payerName} className="flex items-center gap-1.5">
                <span style={{ ...mono, fontSize: '9px', color: '#918D88' }}>{shortPayer(trend.payerName)}</span>
                <SparkLine data={trend.history.map(p => p.score)} color={tc} width={40} height={16} />
                <span style={{ ...mono, fontSize: '9px', fontWeight: 600, color: tc }}>{ar}{Math.abs(trend.delta)}</span>
              </div>
            )
          })}
        </div>
      </div>

      <div className="flex items-center justify-between pt-2" style={{ borderTop: '1px solid #EBEBEB' }}>
        <p style={{ ...mono, fontSize: '9px', color: '#918D88' }}>{drug.livesAtRisk}</p>
        <div className="flex items-center gap-1.5">
          <div style={{ width: '5px', height: '5px', borderRadius: '50%', background: maxColor }} />
          <span style={{ ...mono, fontSize: '10px', color: '#4A4845' }}>Max <span style={{ fontWeight: 600, color: maxColor }}>{maxScore}</span></span>
        </div>
      </div>
    </motion.button>
  )
}

export function PortfolioView({ portfolio, onSelectDrug, changes }: PortfolioViewProps) {
  const [search, setSearch] = useState('')
  const stats    = buildStats(portfolio, changes)
  const filtered = portfolio.filter(d =>
    d.brandName.toLowerCase().includes(search.toLowerCase()) ||
    d.genericName.toLowerCase().includes(search.toLowerCase()) ||
    d.drugClass.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div style={{ background: '#F0EFEB', minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Topbar */}
      <div style={{ background: '#FFFFFF', borderBottom: '1px solid #D8D4CC', padding: '13px 26px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexShrink: 0 }}>
        <div>
          <p style={{ ...LABEL, marginBottom: '2px' }}>Medical Benefit Drug Policy Tracker</p>
          <h1 style={{ fontSize: '16px', fontWeight: 700, color: '#131210' }}>Portfolio Overview</h1>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '7px', background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px', padding: '6px 10px', width: '200px' }}>
          <span style={{ color: '#918D88', fontSize: '12px' }}>⌕</span>
          <input
            style={{ background: 'none', border: 'none', outline: 'none', color: '#131210', fontFamily: "'IBM Plex Sans',system-ui,sans-serif", fontSize: '12px', width: '100%' }}
            placeholder="Search drugs, payers..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div style={{ padding: '22px 26px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
        {/* Stat grid */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: '10px' }}>
          {stats.map((stat, i) => <StatCard key={stat.label} stat={stat} index={i} />)}
        </div>

        {/* Heatmap + Feed */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 300px', gap: '10px' }}>
          <CoverageHeatmap portfolio={portfolio} onSelectDrug={onSelectDrug} />
          <RecentChangeFeed changes={changes} />
        </div>

        {/* Drug portfolio */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '10px' }}>
            <div>
              <p style={{ ...LABEL, marginBottom: '2px' }}>Drug Portfolio</p>
              <p style={{ fontSize: '13px', fontWeight: 600, color: '#131210' }}>{portfolio.length} products tracked</p>
            </div>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: '10px' }}>
            {filtered.map(drug => <DrugCard key={drug.id} drug={drug} onSelect={() => onSelectDrug(drug.id)} />)}
            <div style={{ background: 'transparent', border: '1px dashed #D8D4CC', borderRadius: '2px', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', minHeight: '180px', gap: '5px' }}>
              <p style={{ fontSize: '18px', color: '#918D88', fontWeight: 300 }}>+</p>
              <p style={{ ...mono, fontSize: '10px', letterSpacing: '0.1em', textTransform: 'uppercase', color: '#918D88' }}>Add Product</p>
              <p style={{ fontSize: '10px', color: '#918D88', textAlign: 'center', maxWidth: '120px', lineHeight: 1.4 }}>Upload a policy PDF to begin tracking</p>
            </div>
          </div>
        </div>

        {/* Overview panels */}
        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1.65fr) minmax(360px,0.95fr)', gap: '16px' }}>
          <OverviewMatrixPanel portfolio={portfolio} />
          <OverviewPolicyDiffPanel portfolio={portfolio} changes={changes} />
        </div>
      </div>

      {/* Status bar */}
      <div style={{ background: '#FFFFFF', borderTop: '1px solid #D8D4CC', padding: '5px 26px', display: 'flex', alignItems: 'center', gap: '18px', marginTop: 'auto' }}>
        {[{ dot: '#1A7840', label: 'API Connected' }, { dot: '#8B6428', label: '4 alerts pending' }].map(({ dot, label }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: '5px', ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', color: '#918D88' }}>
            <div style={{ width: '5px', height: '5px', borderRadius: '50%', background: dot }} /> {label}
          </div>
        ))}
        <div style={{ marginLeft: 'auto', ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', color: '#918D88' }}>
          Q1 2026
        </div>
      </div>
    </div>
  )
}
