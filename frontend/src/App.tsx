import { useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { Sidebar } from './components/Sidebar'
import { PortfolioView } from './components/PortfolioView'
import { DrugDetailView } from './components/DrugDetailView'
import { ComparisonMatrix } from './components/ComparisonMatrix'
import { ChangeDigest } from './components/ChangeDigest'
import { mockChanges } from './data/mockChanges'
import { portfolio } from './data/mockPortfolio'

type NavView = 'portfolio' | 'compare' | 'digest'

export default function App() {
  const [activeNav, setActiveNav]       = useState<NavView>('portfolio')
  const [selectedDrug, setSelectedDrug] = useState<string | null>(null)

  const drug = selectedDrug ? portfolio.find(d => d.id === selectedDrug) ?? null : null

  function handleNavigate(id: string) {
    setActiveNav(id as NavView)
    setSelectedDrug(null)
  }

  return (
    <div className="flex min-h-screen" style={{ background: '#F2EFE9' }}>
      <Sidebar active={activeNav} onNavigate={handleNavigate} />

      <div className="flex-1 flex flex-col min-w-0">
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
                <DrugDetailView drug={drug} onBack={() => setSelectedDrug(null)} />
              ) : (
                <PortfolioView portfolio={portfolio} onSelectDrug={setSelectedDrug} />
              )}
            </motion.div>
          )}

          {activeNav === 'compare' && (
            <motion.div
              key="compare"
              className="flex-1 flex flex-col min-h-screen"
              style={{ background: '#F2EFE9' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.12 }}
            >
              <div className="px-8 pt-7 pb-5" style={{ background: '#fff', borderBottom: '1px solid #E2E7EF' }}>
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
              <div className="px-8 py-6 max-w-6xl w-full">
                <ComparisonMatrix policies={portfolio[0].policies} />
              </div>
            </motion.div>
          )}

          {activeNav === 'digest' && (
            <motion.div
              key="digest"
              className="flex-1 flex flex-col min-h-screen"
              style={{ background: '#F2EFE9' }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.12 }}
            >
              <div className="px-8 pt-7 pb-5" style={{ background: '#fff', borderBottom: '1px solid #E2E7EF' }}>
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
              <div className="px-8 py-6 max-w-3xl w-full">
                <ChangeDigest changes={mockChanges} />
              </div>
            </motion.div>
          )}

        </AnimatePresence>
      </div>
    </div>
  )
}
