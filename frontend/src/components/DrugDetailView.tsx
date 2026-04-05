import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { ArrowLeft } from 'lucide-react'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'
import { ComparisonMatrix } from './ComparisonMatrix'
import { InsightPanel } from './InsightPanel'
import { CriteriaBreakdown } from './CriteriaBreakdown'
import { ChangeDigest } from './ChangeDigest'
import { SparkLine } from './SparkLine'
import type { ChangeEntry } from '../types/policy'

interface DrugDetailViewProps {
  drug: DrugPortfolioEntry
  onBack: () => void
  changes: ChangeEntry[]
}

type Tab = 'comparison' | 'criteria' | 'digest'

const tabs: { id: Tab; label: string }[] = [
  { id: 'comparison', label: 'Cross-Payer Comparison' },
  { id: 'criteria',   label: 'Criteria Analysis' },
  { id: 'digest',     label: 'Change Digest' },
]

export function DrugDetailView({ drug, onBack, changes }: DrugDetailViewProps) {
  const [activeTab, setActiveTab] = useState<Tab>('comparison')

  const totalPA  = drug.policies.reduce((s, p) => s + p.indications.filter(i => i.pa_required).length, 0)
  const totalInd = drug.policies.reduce((s, p) => s + p.indications.length, 0)

  const drugChanges = changes.filter(c =>
    c.drug.toLowerCase().includes(drug.brandName.toLowerCase()) ||
    c.drug.toLowerCase().includes(drug.genericName.toLowerCase())
  )

  return (
    <div className="flex-1 flex flex-col min-h-screen" style={{ background: '#F2EFE9' }}>
      {/* Drug header */}
      <div className="px-8 pt-6 pb-0" style={{ background: '#fff', borderBottom: '1px solid #E2E7EF' }}>
        <div className="max-w-6xl">
          {/* Back */}
          <button onClick={onBack}
            className="flex items-center gap-1.5 text-xs mb-5 group"
            style={{ color: '#9AA3AF' }}>
            <ArrowLeft size={13} className="group-hover:-translate-x-0.5 transition-transform" />
            Back to portfolio
          </button>

          <div className="flex items-start justify-between pb-5">
            {/* Identity */}
            <div className="flex items-start gap-4">
              <div className="w-11 h-11 rounded-xl flex items-center justify-center flex-shrink-0"
                style={{ background: 'linear-gradient(135deg, #2D6A90, #0A8F7C)' }}>
                <span className="text-white text-base font-bold">{drug.brandName.charAt(0)}</span>
              </div>
              <div>
                <div className="flex items-center gap-2.5 flex-wrap">
                  <h1 className="text-xl font-semibold" style={{ color: '#0E1117' }}>{drug.brandName}</h1>
                  <span style={{ color: '#E2E7EF' }}>·</span>
                  <span className="text-sm font-mono" style={{ color: '#6B7583' }}>{drug.genericName}</span>
                  <span className="text-[10px] font-mono px-2 py-0.5 rounded"
                    style={{ background: '#EEF1F6', color: '#6B7583' }}>
                    {drug.jCode}
                  </span>
                </div>
                <div className="flex items-center gap-2 mt-1.5">
                  <span className="inline-flex items-center gap-1 text-[11px] font-medium px-2.5 py-1 rounded-full"
                    style={{ background: '#EBF4FA', color: '#2D6A90' }}>
                    {drug.drugClass}
                  </span>
                  {drug.changeCount > 0 && (
                    <span className="inline-flex items-center gap-1 text-[11px] font-medium px-2.5 py-1 rounded-full"
                      style={{ background: '#FEE2E2', color: '#DC2626' }}>
                      {drug.changeCount} changes this quarter
                    </span>
                  )}
                </div>
              </div>
            </div>

            {/* Stats — clinical language, not stringency hero */}
            <div className="flex gap-8 text-right">
              {[
                { label: 'PA burden',       value: `${totalPA}/${totalInd}`, color: totalPA > totalInd * 0.6 ? '#DC2626' : '#10A090' },
                { label: 'Lives at risk',   value: drug.livesAtRisk, color: '#0E1117' },
                { label: 'Payers tracked',  value: `${drug.policies.length}`, color: '#0E1117' },
              ].map((stat, i) => (
                <motion.div key={i} initial={{ opacity: 0, y: 5 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: i * 0.06 }}>
                  <p className="text-xl font-bold tabular-nums" style={{ color: stat.color }}>{stat.value}</p>
                  <p className="text-[10px] mt-0.5" style={{ color: '#9AA3AF' }}>{stat.label}</p>
                </motion.div>
              ))}
            </div>
          </div>

          {/* Trend strip — always visible, above tabs */}
          <div className="flex gap-6 pb-5 border-b mb-0" style={{ borderBottomColor: '#E2E7EF' }}>
            {drug.trends.map(t => {
              const trendColor = t.direction === 'tightening' ? '#DC2626' : t.direction === 'loosening' ? '#10A090' : '#9AA3AF'
              const dirLabel   = t.direction === 'tightening' ? '↑ tightening' : t.direction === 'loosening' ? '↓ loosening' : '→ stable'
              const shortName  = t.payerName === 'Blue Cross NC' ? 'BCNC' : t.payerName === 'UnitedHealth' ? 'UHC' : t.payerName
              const lastScore  = t.history[t.history.length - 1].score
              return (
                <div key={t.payerName} className="flex items-center gap-3 px-4 py-2.5 rounded-lg"
                  style={{ background: '#F2EFE9', border: '1px solid #E2E7EF' }}>
                  <div>
                    <p className="text-[10px] font-semibold" style={{ color: '#6B7583' }}>{shortName}</p>
                    <p className="text-xs font-semibold tabular-nums mt-0.5" style={{ color: trendColor }}>
                      {dirLabel} <span className="font-bold">({t.delta > 0 ? '+' : ''}{t.delta} pts)</span>
                    </p>
                  </div>
                  <SparkLine data={t.history.map(h => h.score)} color={trendColor} width={72} height={32} />
                  <p className="text-lg font-bold tabular-nums" style={{ color: trendColor }}>{lastScore}</p>
                </div>
              )
            })}
            <div className="flex items-center ml-auto">
              <p className="text-[10px]" style={{ color: '#C0CDD9' }}>Stringency over 4 quarters · higher = more restrictive</p>
            </div>
          </div>

          {/* Tab bar */}
          <div className="flex gap-0 -mb-px">
            {tabs.map(tab => {
              const isActive = activeTab === tab.id
              return (
                <button key={tab.id} onClick={() => setActiveTab(tab.id)}
                  className="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-all duration-100"
                  style={{
                    borderBottomColor: isActive ? '#2D6A90' : 'transparent',
                    color: isActive ? '#2D6A90' : '#6B7583',
                  }}>
                  {tab.label}
                  {tab.id === 'digest' && drugChanges.length > 0 && (
                    <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded-full ml-1"
                      style={{ background: '#FEE2E2', color: '#DC2626' }}>
                      {drugChanges.length}
                    </span>
                  )}
                </button>
              )
            })}
          </div>
        </div>
      </div>

      {/* Tab content */}
      <div className="flex-1 px-8 py-6 max-w-6xl w-full">
        <AnimatePresence mode="wait">
          <motion.div
            key={activeTab}
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -4 }}
            transition={{ duration: 0.12, ease: 'easeOut' }}
          >
            {activeTab === 'comparison' && (
              <div className="grid gap-6" style={{ gridTemplateColumns: '1fr 300px' }}>
                <ComparisonMatrix policies={drug.policies} />
                <InsightPanel insights={drug.insights} drugName={drug.brandName} />
              </div>
            )}
            {activeTab === 'criteria' && (
              <CriteriaBreakdown policies={drug.policies} />
            )}
            {activeTab === 'digest' && (
              <div className="max-w-3xl">
                <ChangeDigest changes={drugChanges.length > 0 ? drugChanges : changes} />
              </div>
            )}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  )
}
