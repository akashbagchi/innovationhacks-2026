import { useState, useEffect } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { Sidebar } from './components/Sidebar'
import { PortfolioView } from './components/PortfolioView'
import { DrugDetailView } from './components/DrugDetailView'
import { ComparisonMatrix } from './components/ComparisonMatrix'
import { ChangeDigest } from './components/ChangeDigest'
import { mockChanges } from './data/mockChanges'
import { portfolio as mockPortfolio } from './data/mockPortfolio'
import type { DrugPortfolioEntry } from './data/mockPortfolio'
import type { ChangeEntry } from './types/policy'
import { fetchPoliciesForDrug, fetchChanges } from './lib/api'

type NavView = 'portfolio' | 'compare' | 'digest'

export default function App() {
  const [activeNav, setActiveNav]       = useState<NavView>('portfolio')
  const [selectedDrug, setSelectedDrug] = useState<string | null>(null)
  const [portfolio, setPortfolio]       = useState<DrugPortfolioEntry[]>(mockPortfolio)
  const [changes, setChanges]           = useState<ChangeEntry[]>(mockChanges)

  // Hydrate portfolio policies from MongoDB via API.
  // trends/insights/livesAtRisk remain from mock (require historical pipeline).
  useEffect(() => {
    Promise.all(
      mockPortfolio.map(async entry => {
        try {
          const livePayers = await fetchPoliciesForDrug(entry.id)
          if (livePayers.length === 0) return entry
          return {
            ...entry,
            policies: livePayers
              .filter(p => p.policy_record != null)
              .map(p => p.policy_record),
          }
        } catch {
          return entry // fallback to mock if API unreachable
        }
      })
    ).then(setPortfolio)
  }, [])

  // Hydrate change log from MongoDB.
  useEffect(() => {
    fetchChanges()
      .then(setChanges)
      .catch(() => {/* keep mockChanges */})
  }, [])

  const drug = selectedDrug ? portfolio.find(d => d.id === selectedDrug) ?? null : null

  function handleNavigate(id: string) {
    setActiveNav(id as NavView)
    setSelectedDrug(null)
  }

  return (
    <div className="flex min-h-screen gap-6 p-6" style={{ background: 'transparent' }}>
      <Sidebar active={activeNav} onNavigate={handleNavigate} changes={changes} portfolio={portfolio} />

      <div className="flex-1 flex flex-col min-w-0 gap-6">
        <AnimatePresence mode="wait">

          {activeNav === 'portfolio' && (
            <motion.div
              key={drug ? `drug-${drug.id}` : 'portfolio'}
              className="flex-1 flex flex-col"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.12 }}
            >
              {drug ? (
                <DrugDetailView drug={drug} onBack={() => setSelectedDrug(null)} changes={changes} />
              ) : (
                <PortfolioView portfolio={portfolio} onSelectDrug={setSelectedDrug} changes={changes} />
              )}
            </motion.div>
          )}

          {activeNav === 'compare' && (
            <motion.div
              key="compare"
              className="flex-1 flex flex-col"
              style={{ background: 'transparent' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.12 }}
            >
              <div className="glass-hero rounded-[30px] px-8 pt-7 pb-5">
                <div className="max-w-6xl">
                  <p className="text-[10px] font-semibold uppercase tracking-widest mb-1" style={{ color: '#0A4D8C' }}>
                    Cross-Payer Intelligence
                  </p>
                  <h1 className="text-2xl font-semibold" style={{ color: '#0D1B2A' }}>Compare</h1>
                  <p className="text-sm mt-1" style={{ color: '#5A6E8A' }}>
                    Select a drug below for a focused cross-payer analysis
                  </p>
                  <div className="flex gap-2 mt-4 flex-wrap">
                    {portfolio.map(d => (
                      <button key={d.id}
                        onClick={() => { setSelectedDrug(d.id); setActiveNav('portfolio') }}
                        className="px-3 py-1.5 rounded-lg text-xs font-medium border transition-all"
                        style={{ borderColor: '#E2E7EF', color: '#5A6E8A', background: '#fff' }}
                      >
                        {d.brandName}
                        <span className="ml-1.5 text-[10px] font-mono" style={{ color: '#94A3B8' }}>{d.jCode}</span>
                      </button>
                    ))}
                  </div>
                </div>
              </div>
              <div className="mx-auto w-full max-w-[1520px] px-6 xl:px-10">
                <ComparisonMatrix policies={portfolio[0].policies} />
              </div>
            </motion.div>
          )}

          {activeNav === 'digest' && (
            <motion.div
              key="digest"
              className="flex-1 flex flex-col"
              style={{ background: 'transparent' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.12 }}
            >
              <div className="glass-hero rounded-[30px] px-8 pt-7 pb-5">
                <div className="max-w-6xl">
                  <p className="text-[10px] font-semibold uppercase tracking-widest mb-1" style={{ color: '#0A4D8C' }}>
                    Policy Surveillance
                  </p>
                  <h1 className="text-2xl font-semibold" style={{ color: '#0D1B2A' }}>Change Digest</h1>
                  <p className="text-sm mt-1" style={{ color: '#5A6E8A' }}>
                    All policy changes detected across your tracked formulary this quarter
                  </p>
                </div>
              </div>
              <div className="mx-auto w-full max-w-[980px] px-6 xl:px-10">
                <ChangeDigest changes={changes} />
              </div>
            </motion.div>
          )}

        </AnimatePresence>
      </div>
    </div>
  )
}
