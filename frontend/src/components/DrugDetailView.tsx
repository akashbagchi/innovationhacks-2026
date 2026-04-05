import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { ArrowLeft, ChevronDown } from 'lucide-react'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'
import { ComparisonMatrix } from './ComparisonMatrix'
import { InsightPanel } from './InsightPanel'
import { CriteriaBreakdown } from './CriteriaBreakdown'
import { ChangeDigest } from './ChangeDigest'
import { SparkLine } from './SparkLine'
import type { ChangeEntry } from '../types/policy'
import { formatPayerName } from '../lib/formatters'

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

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

export function DrugDetailView({ drug, onBack, changes }: DrugDetailViewProps) {
  const [activeTab, setActiveTab]         = useState<Tab>('comparison')
  const [limitationsOpen, setLimitationsOpen] = useState(false)

  const benefitType      = drug.policies.find(p => p.drug.benefit_type)?.drug.benefit_type
  const limitationsOfUse = drug.policies.find(p => p.drug.limitations_of_use)?.drug.limitations_of_use

  const totalPA  = drug.policies.reduce((s, p) => s + p.indications.filter(i => i.pa_required).length, 0)
  const totalInd = drug.policies.reduce((s, p) => s + p.indications.length, 0)

  const drugChanges = changes.filter(c =>
    c.drug.toLowerCase().includes(drug.brandName.toLowerCase()) ||
    c.drug.toLowerCase().includes(drug.genericName.toLowerCase())
  )

  return (
    <div style={{ background: '#F0EFEB', minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      {/* Drug header */}
      <div style={{ background: '#FFFFFF', borderBottom: '1px solid #D8D4CC', padding: '16px 26px 0', flexShrink: 0 }}>
        {/* Back */}
        <button onClick={onBack}
          className="flex items-center gap-1.5 mb-4 group"
          style={{ color: '#918D88', fontSize: '12px', background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
          <ArrowLeft size={12} className="group-hover:-translate-x-0.5 transition-transform" />
          Back to portfolio
        </button>

        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', paddingBottom: '14px', borderBottom: '1px solid #EBEBEB' }}>
          {/* Identity */}
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap', marginBottom: '6px' }}>
              <h1 style={{ fontSize: '18px', fontWeight: 700, color: '#131210' }}>{drug.brandName}</h1>
              <span style={{ color: '#D8D4CC' }}>·</span>
              <span style={{ ...mono, fontSize: '12px', color: '#4A4845' }}>{drug.genericName}</span>
              <span style={{ ...mono, fontSize: '10px', padding: '2px 7px', borderRadius: '1px', background: '#F0EFEB', border: '1px solid #D8D4CC', color: '#4A4845' }}>{drug.jCode}</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap' }}>
              <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '2px 7px', background: '#F0EFEB', border: '1px solid #D8D4CC', color: '#4A4845', borderRadius: '1px' }}>
                {drug.drugClass}
              </span>
              {benefitType && (
                <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '2px 7px', background: '#F8EDDC', border: '1px solid rgba(139,100,40,0.2)', color: '#8B6428', borderRadius: '1px' }}>
                  {benefitType} benefit
                </span>
              )}
              {drug.changeCount > 0 && (
                <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.08em', textTransform: 'uppercase', padding: '2px 7px', background: '#FBEAEA', border: '1px solid rgba(184,28,28,0.2)', color: '#B81C1C', borderRadius: '1px' }}>
                  {drug.changeCount} changes this quarter
                </span>
              )}
            </div>
          </div>

          {/* Stats */}
          <div style={{ display: 'flex', gap: '28px', textAlign: 'right' }}>
            {[
              { label: 'PA burden',      value: `${totalPA}/${totalInd}`, color: totalPA > totalInd * 0.6 ? '#B81C1C' : '#1A7840' },
              { label: 'Lives at risk',  value: drug.livesAtRisk, color: '#131210' },
              { label: 'Payers tracked', value: `${drug.policies.length}`, color: '#131210' },
            ].map((stat, i) => (
              <motion.div key={i} initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: i * 0.05 }}>
                <p style={{ ...mono, fontSize: '18px', fontWeight: 600, color: stat.color, lineHeight: 1 }}>{stat.value}</p>
                <p style={{ ...LABEL, marginTop: '3px' }}>{stat.label}</p>
              </motion.div>
            ))}
          </div>
        </div>

        {/* Limitations of use */}
        {limitationsOfUse && (
          <div style={{ margin: '10px 0', background: '#F8EDDC', border: '1px solid rgba(139,100,40,0.25)', borderRadius: '2px', overflow: 'hidden' }}>
            <button onClick={() => setLimitationsOpen(o => !o)}
              style={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '8px 12px', background: 'none', border: 'none', cursor: 'pointer' }}>
              <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.12em', textTransform: 'uppercase', color: '#8B6428' }}>Limitations of Use</span>
              <ChevronDown size={12} style={{ color: '#8B6428', transform: limitationsOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.12s', flexShrink: 0 }} />
            </button>
            {limitationsOpen && (
              <p style={{ padding: '0 12px 10px', fontSize: '12px', lineHeight: 1.6, color: '#4A4845' }}>{limitationsOfUse}</p>
            )}
          </div>
        )}

        {/* Trend strip */}
        <div style={{ display: 'flex', gap: '10px', paddingBottom: '14px', flexWrap: 'wrap' }}>
          {drug.trends.map(t => {
            const tc        = t.direction === 'tightening' ? '#B81C1C' : t.direction === 'loosening' ? '#1A7840' : '#918D88'
            const dirLabel  = t.direction === 'tightening' ? '↑ tightening' : t.direction === 'loosening' ? '↓ loosening' : '→ stable'
            const shortName = formatPayerName(t.payerName)
            const lastScore = t.history[t.history.length - 1].score
            return (
              <div key={t.payerName} style={{ display: 'flex', alignItems: 'center', gap: '10px', padding: '8px 12px', background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px' }}>
                <div>
                  <p style={{ ...LABEL, marginBottom: '2px' }}>{shortName}</p>
                  <p style={{ ...mono, fontSize: '10px', fontWeight: 600, color: tc }}>{dirLabel} <span style={{ fontWeight: 700 }}>({t.delta > 0 ? '+' : ''}{t.delta} pts)</span></p>
                </div>
                <SparkLine data={t.history.map(h => h.score)} color={tc} width={64} height={28} />
                <p style={{ ...mono, fontSize: '16px', fontWeight: 600, color: tc }}>{lastScore}</p>
              </div>
            )
          })}
          <p style={{ ...LABEL, alignSelf: 'center', marginLeft: 'auto' }}>Stringency over 4 quarters · higher = more restrictive</p>
        </div>

        {/* Tab bar */}
        <div style={{ display: 'flex', gap: 0, marginBottom: '-1px' }}>
          {tabs.map(tab => {
            const isActive = activeTab === tab.id
            return (
              <button key={tab.id} onClick={() => setActiveTab(tab.id)}
                style={{
                  display: 'flex', alignItems: 'center', gap: '6px',
                  padding: '9px 14px', fontSize: '13px', fontWeight: isActive ? 600 : 400,
                  background: 'none', border: 'none', borderBottom: isActive ? '2px solid #91bfeb' : '2px solid transparent',
                  color: isActive ? '#91bfeb' : '#4A4845', cursor: 'pointer', transition: 'all 0.1s',
                }}>
                {tab.label}
                {tab.id === 'digest' && drugChanges.length > 0 && (
                  <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: '#FBEAEA', color: '#B81C1C' }}>
                    {drugChanges.length}
                  </span>
                )}
              </button>
            )
          })}
        </div>
      </div>

      {/* Tab content */}
      <div style={{ flex: 1, padding: '22px 26px' }}>
        <AnimatePresence mode="wait">
          <motion.div key={activeTab}
            initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            transition={{ duration: 0.1 }}>
            {activeTab === 'comparison' && (
              <div style={{ display: 'grid', gap: '16px', gridTemplateColumns: '1fr 300px' }}>
                <ComparisonMatrix policies={drug.policies} />
                <InsightPanel insights={drug.insights} drugName={drug.brandName} />
              </div>
            )}
            {activeTab === 'criteria' && <CriteriaBreakdown policies={drug.policies} />}
            {activeTab === 'digest' && (
              <div style={{ maxWidth: '800px' }}>
                <ChangeDigest changes={drugChanges.length > 0 ? drugChanges : changes} />
              </div>
            )}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  )
}
