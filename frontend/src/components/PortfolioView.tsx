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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from './ui/dialog'

interface PortfolioViewProps {
  portfolio: DrugPortfolioEntry[]
  onSelectDrug: (id: string) => void
  changes: ChangeEntry[]
}

interface HighImpactDetail {
  kind: 'high-impact'
  title: string
  badge: string
  description: string
  supporting: string
}

interface OutlierDetail {
  kind: 'outlier'
  title: string
  badge: string
  description: string
  supporting: string
}

interface StatCardData {
  label: string
  value: number
  sub: string
  highlight?: boolean
  details?: {
    eyebrow: string
    title: string
    description: string
    emptyMessage: string
    items: Array<HighImpactDetail | OutlierDetail>
  }
}

function shortPayer(name: string) {
  if (name === 'Blue Cross NC') return 'BCNC'
  if (name === 'UnitedHealth') return 'UHC'
  return name
}

function formatChangeDate(date: string) {
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  }).format(new Date(date))
}

function summarizeDirection(change: ChangeEntry) {
  const text = `${change.change_type} ${change.summary}`.toLowerCase()

  if (/(removed|waive|loosen|expanded|broader|without pa|no clinical criteria changes)/.test(text)) {
    return 'Loosening'
  }

  if (/(added|stricter|reduced|must|required|step therapy|threshold)/.test(text)) {
    return 'Tightening'
  }

  return 'Material update'
}

function buildHighImpactDetails(changes: ChangeEntry[]): HighImpactDetail[] {
  return changes
    .filter(change => change.severity === 'HIGH')
    .map(change => ({
      kind: 'high-impact',
      title: `${change.payer} changed ${change.drug}`,
      badge: summarizeDirection(change),
      description: change.summary,
      supporting: `${change.change_type.replaceAll('_', ' ')} | ${formatChangeDate(change.date)}`,
    }))
}

function buildOutlierDetails(portfolio: DrugPortfolioEntry[]): OutlierDetail[] {
  return portfolio.flatMap(drug => {
    const scores = drug.policies.map(policy => ({
      payer: policy.payer.name,
      score: computeStringency(policy).score,
      stepTherapyCount: policy.indications.filter(indication => indication.step_therapy_required).length,
      paCount: policy.indications.filter(indication => indication.pa_required).length,
      exclusions: policy.exclusions?.length ?? 0,
    }))

    const averageScore = scores.reduce((sum, item) => sum + item.score, 0) / scores.length

    return drug.trends
      .filter(trend => Math.abs(trend.delta) > 8)
      .map(trend => {
        const matchingScore = scores.find(item => item.payer === trend.payerName)
        const directionLabel = trend.direction === 'loosening' ? 'looser' : 'stricter'
        const deltaLabel = trend.direction === 'loosening'
          ? `${Math.abs(trend.delta)} points below its prior baseline`
          : `${Math.abs(trend.delta)} points above its prior baseline`

        return {
          kind: 'outlier' as const,
          title: `${trend.payerName} is an outlier for ${drug.brandName}`,
          badge: trend.direction === 'loosening' ? 'Looser than peers' : 'Stricter than peers',
          description: matchingScore
            ? `Stringency score ${matchingScore.score} versus peer average ${averageScore.toFixed(0)}. ${trend.payerName} is trending ${directionLabel} than the rest of the market this quarter.`
            : `${trend.payerName} is trending ${directionLabel} than peer policies this quarter.`,
          supporting: matchingScore
            ? `${deltaLabel} | ${matchingScore.paCount} PA indications, ${matchingScore.stepTherapyCount} step therapy rules, ${matchingScore.exclusions} exclusions`
            : deltaLabel,
        }
      })
  })
}

function buildStats(portfolio: DrugPortfolioEntry[], changes: ChangeEntry[]): StatCardData[] {
  const policiesTracked = portfolio.length * 3
  const payersMonitored = 3
  const highImpactDetails = buildHighImpactDetails(changes)
  const outlierDetails = buildOutlierDetails(portfolio)
  const highImpact = highImpactDetails.length
  const tighteningCount = portfolio.flatMap(d => d.trends).filter(t => t.direction === 'tightening').length
  const looseningCount = portfolio.flatMap(d => d.trends).filter(t => t.direction === 'loosening').length
  const outlierPolicies = outlierDetails.length

  return [
    {
      label: 'Policies Tracked',
      value: policiesTracked,
      sub: '+0 this quarter',
    },
    {
      label: 'Payers Monitored',
      value: payersMonitored,
      sub: 'Commercial + Medicare LCDs',
    },
    {
      label: 'High-Impact Changes',
      value: highImpact,
      sub: `${tighteningCount} tightening, ${looseningCount} loosening`,
      highlight: highImpact > 0,
      details: {
        eyebrow: 'Quarterly change audit',
        title: 'What is driving the high-impact count',
        description: 'These are the specific payer edits behind the summary, including the direction of the change and when it landed.',
        emptyMessage: 'No high-impact changes detected in the current mock feed.',
        items: highImpactDetails,
      },
    },
    {
      label: 'Outlier Policies',
      value: outlierPolicies,
      sub: 'Unique restrictions flagged',
      details: {
        eyebrow: 'Cross-payer anomaly review',
        title: 'Why these policies are considered outliers',
        description: 'Each outlier compares one payer against peers for the same drug so the analyst can quickly see what is different and why it matters.',
        emptyMessage: 'No outlier policy patterns are flagged in the current mock portfolio.',
        items: outlierDetails,
      },
    },
  ]
}

const stagger = {
  container: { transition: { staggerChildren: 0.05 } },
  item: {
    initial: { opacity: 0, y: 10 },
    animate: { opacity: 1, y: 0 },
    transition: { duration: 0.18, ease: 'easeOut' },
  },
}

function StatCard({ stat, index }: { stat: StatCardData; index: number }) {
  const cardBody = (
    <div
      className="h-full rounded-[26px] border p-5 text-left transition-all duration-200 shadow-[0_18px_48px_rgba(18,52,51,0.1)] backdrop-blur-[18px]"
      style={{ background: 'rgba(255, 252, 245, 0.82)', borderColor: 'rgba(53, 76, 72, 0.14)' }}
    >
      <div className="flex h-full flex-col">
        <p className="mb-3 text-xs" style={{ color: '#9CA3AF' }}>{stat.label}</p>
        <p
          className="mb-1.5 text-3xl font-bold leading-none tabular-nums"
          style={{ color: stat.highlight ? '#DC2626' : '#0E1117' }}
        >
          {stat.value}
        </p>
        <p className="text-[11px]" style={{ color: '#9CA3AF' }}>{stat.sub}</p>
        {stat.details && (
          <p className="mt-4 text-[11px] font-semibold uppercase tracking-[0.18em]" style={{ color: '#2D6A90' }}>
            View details
          </p>
        )}
      </div>
    </div>
  )

  const animatedWrapper = (children: ReactNode) => (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.06, duration: 0.18 }}
      className="h-full"
    >
      {children}
    </motion.div>
  )

  if (!stat.details) {
    return animatedWrapper(cardBody)
  }

  return animatedWrapper(
    <Dialog>
      <DialogTrigger asChild>
        <button
          type="button"
          className="h-full w-full rounded-[24px] text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-[#7BA8C4] focus-visible:ring-offset-2"
        >
          {cardBody}
        </button>
      </DialogTrigger>
      <DialogContent className="max-w-3xl rounded-[28px] border border-[#E5E2DC] bg-[#FCFBF8] p-0 text-[#0E1117] shadow-[0_24px_80px_rgba(14,17,23,0.14)]">
        <div className="border-b border-[#ECE7DE] px-7 py-6">
          <DialogHeader className="gap-3">
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-[#2D6A90]">
              {stat.details.eyebrow}
            </p>
            <DialogTitle className="text-2xl font-semibold tracking-tight text-[#0E1117]">
              {stat.details.title}
            </DialogTitle>
            <DialogDescription className="max-w-2xl text-sm leading-6 text-[#6B7280]">
              {stat.details.description}
            </DialogDescription>
          </DialogHeader>
        </div>

        <div className="max-h-[65vh] space-y-3 overflow-y-auto px-7 py-6">
          {stat.details.items.length === 0 ? (
            <div className="rounded-2xl border border-dashed border-[#D9D4CB] bg-white px-5 py-8 text-center text-sm text-[#6B7280]">
              {stat.details.emptyMessage}
            </div>
          ) : (
            stat.details.items.map(item => (
              <div
                key={`${item.kind}-${item.title}-${item.badge}`}
                className="rounded-2xl border border-[#E5E2DC] bg-white p-5 shadow-[0_8px_24px_rgba(14,17,23,0.04)]"
              >
                <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
                  <h3 className="text-base font-semibold text-[#0E1117]">{item.title}</h3>
                  <span
                    className="inline-flex rounded-full px-3 py-1 text-[11px] font-semibold"
                    style={{
                      background: item.kind === 'high-impact' ? '#EBF4FA' : '#FEF3C7',
                      color: item.kind === 'high-impact' ? '#2D6A90' : '#92400E',
                    }}
                  >
                    {item.badge}
                  </span>
                </div>
                <p className="text-sm leading-6 text-[#334155]">{item.description}</p>
                <p className="mt-3 text-xs font-medium uppercase tracking-[0.16em] text-[#9CA3AF]">
                  {item.supporting}
                </p>
              </div>
            ))
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function DrugCard({ drug, onSelect }: { drug: DrugPortfolioEntry; onSelect: () => void }) {
  const scores = drug.policies.map(p => computeStringency(p))
  const maxScore = Math.max(...scores.map(s => s.score))
  const maxColor = maxScore >= 70 ? '#DC2626' : maxScore >= 40 ? '#D97706' : '#10A090'

  return (
    <motion.button
      variants={stagger.item}
      onClick={onSelect}
      className="group w-full rounded-[26px] border p-5 text-left transition-all duration-200 backdrop-blur-[18px]"
      style={{ background: 'rgba(255, 252, 245, 0.82)', borderColor: 'rgba(53, 76, 72, 0.14)' }}
      whileHover={{ y: -2, boxShadow: '0 18px 48px rgba(18,52,51,0.1)', borderColor: '#7BA8C4' }}
    >
      <div className="mb-1 flex items-start justify-between">
        <div>
          <p
            className="text-base font-bold leading-tight transition-colors group-hover:text-[#2D6A90]"
            style={{ color: '#0E1117' }}
          >
            {drug.brandName}
          </p>
          <p className="mt-0.5 text-xs font-mono" style={{ color: '#9CA3AF' }}>
            {drug.genericName} | {drug.jCode}
          </p>
        </div>
        {drug.changeCount > 0 && (
          <div className="text-right">
            <p className="text-2xl font-bold leading-none tabular-nums" style={{ color: '#DC2626' }}>
              {drug.changeCount}
            </p>
            <p className="mt-0.5 text-[10px]" style={{ color: '#9CA3AF' }}>changes</p>
          </div>
        )}
      </div>

      <span
        className="mt-2 mb-4 inline-flex items-center gap-1 rounded-full px-2.5 py-1 text-[11px] font-medium"
        style={{ background: '#EBF4FA', color: '#2D6A90' }}
      >
        <span className="h-1 w-1 rounded-full" style={{ background: '#7BA8C4' }} />
        {drug.drugClass}
      </span>

      <div className="mb-4 space-y-1.5">
        <p className="text-[10px] font-semibold uppercase tracking-wider" style={{ color: '#C4C9D1' }}>PA Burden</p>
        {drug.policies.map(policy => {
          const paCount = policy.indications.filter(indication => indication.pa_required).length
          const pct = Math.round((paCount / policy.indications.length) * 100)
          const barColor = pct > 70 ? '#DC2626' : pct > 40 ? '#D97706' : '#10A090'

          return (
            <div key={policy.payer.name} className="flex items-center gap-2">
              <span className="w-9 flex-shrink-0 text-[10px] font-mono" style={{ color: '#9CA3AF' }}>
                {shortPayer(policy.payer.name)}
              </span>
              <div className="h-1.5 flex-1 overflow-hidden rounded-full" style={{ background: '#EEE9E1' }}>
                <motion.div
                  className="h-full rounded-full"
                  style={{ background: barColor }}
                  initial={{ width: 0 }}
                  animate={{ width: `${pct}%` }}
                  transition={{ duration: 0.4, ease: 'easeOut' }}
                />
              </div>
              <span className="w-7 text-right text-[10px] tabular-nums" style={{ color: '#6B7280' }}>{pct}%</span>
            </div>
          )
        })}
      </div>

      <div className="mb-3 pt-3" style={{ borderTop: '1px solid #F0EDE7' }}>
        <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider" style={{ color: '#C4C9D1' }}>
          Trend (4Q)
        </p>
        <div className="flex flex-wrap gap-3">
          {drug.trends.map(trend => {
            const trendColor = trend.direction === 'tightening'
              ? '#DC2626'
              : trend.direction === 'loosening'
                ? '#10A090'
                : '#9CA3AF'
            const arrow = trend.direction === 'tightening' ? '↑' : trend.direction === 'loosening' ? '↓' : '→'

            return (
              <div key={trend.payerName} className="flex items-center gap-1.5">
                <span className="text-[10px] font-mono" style={{ color: '#9CA3AF' }}>{shortPayer(trend.payerName)}</span>
                <SparkLine data={trend.history.map(point => point.score)} color={trendColor} width={44} height={18} />
                <span className="text-[10px] font-bold" style={{ color: trendColor }}>{arrow}{Math.abs(trend.delta)}</span>
              </div>
            )
          })}
        </div>
      </div>

      <div className="flex items-center justify-between pt-2.5" style={{ borderTop: '1px solid #F0EDE7' }}>
        <p className="text-[10px]" style={{ color: '#9CA3AF' }}>{drug.livesAtRisk}</p>
        <div className="flex items-center gap-1.5">
          <div className="h-1.5 w-1.5 rounded-full" style={{ background: maxColor }} />
          <span className="text-xs" style={{ color: '#6B7280' }}>
            Max <span className="font-bold tabular-nums" style={{ color: maxColor }}>{maxScore}</span>
          </span>
        </div>
      </div>
    </motion.button>
  )
}

export function PortfolioView({ portfolio, onSelectDrug, changes }: PortfolioViewProps) {
  const [search, setSearch] = useState('')
  const stats = buildStats(portfolio, changes)

  const filtered = portfolio.filter(drug =>
    drug.brandName.toLowerCase().includes(search.toLowerCase()) ||
    drug.genericName.toLowerCase().includes(search.toLowerCase()) ||
    drug.drugClass.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div className="flex min-h-screen flex-1 flex-col" style={{ background: '#F2EFE9' }}>
      <div className="mx-auto w-full max-w-[1520px] px-6 py-6 xl:px-10">
        <div className="space-y-6">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 2xl:grid-cols-4">
            {stats.map((stat, index) => (
              <StatCard key={stat.label} stat={stat} index={index} />
            ))}
          </div>

          <div className="grid gap-4 2xl:grid-cols-[minmax(0,1.7fr)_380px]">
            <CoverageHeatmap portfolio={portfolio} onSelectDrug={onSelectDrug} />
            <RecentChangeFeed changes={changes} />
          </div>

          <div>
            <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <p className="mb-0.5 text-[10px] font-bold uppercase tracking-widest" style={{ color: '#9CA3AF' }}>
                  Drug Portfolio
                </p>
                <p className="text-sm font-semibold" style={{ color: '#0E1117' }}>
                  {portfolio.length} products tracked
                </p>
              </div>

              <div
                className="brand-focus flex items-center gap-2 rounded-lg border px-3 py-2"
                style={{ borderColor: '#E5E2DC', background: '#fff' }}
              >
                <span aria-hidden="true" style={{ color: '#C4C9D1' }}>⌕</span>
                <input
                  className="w-48 bg-transparent text-sm outline-none"
                  style={{ color: '#0E1117' }}
                  placeholder="Search..."
                  value={search}
                  onChange={event => setSearch(event.target.value)}
                />
              </div>
            </div>

            <motion.div
              className="grid grid-cols-1 gap-4 lg:grid-cols-2 2xl:grid-cols-3"
              variants={stagger.container}
              initial="initial"
              animate="animate"
            >
              {filtered.map(drug => (
                <DrugCard key={drug.id} drug={drug} onSelect={() => onSelectDrug(drug.id)} />
              ))}
              <motion.div
                variants={stagger.item}
                className="flex min-h-[200px] flex-col items-center justify-center rounded-[26px] border-2 border-dashed p-8"
                style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 252, 245, 0.56)' }}
              >
                <p className="mb-2 text-2xl" style={{ color: '#D1CEC8' }}>+</p>
                <p className="text-sm font-medium" style={{ color: '#9CA3AF' }}>Add product</p>
                <p className="mt-1 text-center text-xs" style={{ color: '#C4C9D1' }}>
                  Upload a policy PDF to begin tracking
                </p>
              </motion.div>
            </motion.div>
          </div>

          <div className="grid gap-5 xl:grid-cols-[minmax(0,1.65fr)_minmax(360px,0.95fr)]">
            <OverviewMatrixPanel portfolio={portfolio} />
            <OverviewPolicyDiffPanel portfolio={portfolio} changes={changes} />
          </div>
        </div>
      </div>
    </div>
  )
}
