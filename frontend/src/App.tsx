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
        } catch { return entry }
      })
    ).then(setPortfolio)
  }, [])

  useEffect(() => {
    fetchChanges().then(setChanges).catch(() => {})
  }, [])

  const drug = selectedDrug ? portfolio.find(d => d.id === selectedDrug) ?? null : null

  function handleNavigate(id: string) {
    setActiveNav(id as NavView)
    setSelectedDrug(null)
  }

  return (
    <div className="flex min-h-screen" style={{ background: '#F0EFEB' }}>
      <Sidebar active={activeNav} onNavigate={handleNavigate} changes={changes} portfolio={portfolio} />

      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        <AnimatePresence mode="wait">

          {activeNav === 'portfolio' && (
            <motion.div key={drug ? `drug-${drug.id}` : 'portfolio'}
              className="flex-1 flex flex-col"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}>
              {drug
                ? <DrugDetailView drug={drug} onBack={() => setSelectedDrug(null)} changes={changes} />
                : <PortfolioView portfolio={portfolio} onSelectDrug={setSelectedDrug} changes={changes} />}
            </motion.div>
          )}

          {activeNav === 'compare' && (
            <motion.div key="compare" className="flex-1 flex flex-col"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}>
              {/* Topbar */}
              <div style={{ background: '#FFFFFF', borderBottom: '1px solid #D8D4CC' }} className="px-7 py-4 flex-shrink-0">
                <p className="label-mono mb-1">Cross-Payer Intelligence</p>
                <h1 className="text-base font-bold" style={{ color: '#131210' }}>Coverage Matrix</h1>
                <p className="text-xs mt-1" style={{ color: '#4A4845' }}>
                  Select a drug below for a focused cross-payer analysis
                </p>
                <div className="flex gap-2 mt-3 flex-wrap">
                  {portfolio.map(d => (
                    <button key={d.id}
                      onClick={() => { setSelectedDrug(d.id); setActiveNav('portfolio') }}
                      style={{
                        background: '#F0EFEB', border: '1px solid #D8D4CC',
                        borderRadius: '2px', color: '#4A4845',
                        fontFamily: "'IBM Plex Mono', monospace",
                        fontSize: '10px', letterSpacing: '0.06em',
                        padding: '5px 10px', cursor: 'pointer',
                      }}>
                      {d.brandName}
                      <span style={{ marginLeft: '8px', color: '#918D88' }}>{d.jCode}</span>
                    </button>
                  ))}
                </div>
              </div>
              <div className="flex-1 overflow-y-auto p-7">
                <ComparisonMatrix policies={portfolio[0].policies} />
              </div>
            </motion.div>
          )}

          {activeNav === 'digest' && (
            <motion.div key="digest" className="flex-1 flex flex-col"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
              transition={{ duration: 0.1 }}>
              {/* Topbar */}
              <div style={{ background: '#FFFFFF', borderBottom: '1px solid #D8D4CC' }} className="px-7 py-4 flex-shrink-0">
                <p className="label-mono mb-1">Policy Surveillance</p>
                <h1 className="text-base font-bold" style={{ color: '#131210' }}>Change Digest</h1>
                <p className="text-xs mt-1" style={{ color: '#4A4845' }}>
                  All policy changes detected across your tracked formulary this quarter
                </p>
              </div>
              <div className="flex-1 overflow-y-auto p-7 max-w-4xl">
                <ChangeDigest changes={changes} />
              </div>
            </motion.div>
          )}

        </AnimatePresence>
      </div>
    </div>
  )
}
