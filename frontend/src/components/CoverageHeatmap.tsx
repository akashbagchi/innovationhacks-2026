import { motion } from 'framer-motion'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'

interface CoverageHeatmapProps {
  portfolio: DrugPortfolioEntry[]
  onSelectDrug: (id: string) => void
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

const statusConfig = {
  tightening: { label: 'Tightened', bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  loosening:  { label: 'Loosened',  bg: '#E0F2E8', text: '#1A7840', border: 'rgba(26,120,64,0.2)' },
  stable:     { label: 'Stable',    bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
}

export function CoverageHeatmap({ portfolio, onSelectDrug }: CoverageHeatmapProps) {
  const payers = portfolio[0]?.trends.map(t => t.payerName) ?? []

  return (
    <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', overflow: 'hidden' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', background: '#F0EFEB', borderBottom: '1px solid #D8D4CC' }}>
        <p style={{ ...LABEL }}>Coverage Change Heatmap — Q1 2026</p>
        <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', color: '#918D88' }}>
          Click drug to drill down →
        </span>
      </div>

      <div style={{ padding: '12px 14px' }}>
        {/* Column headers */}
        <div className="grid items-center gap-3 mb-2"
          style={{ gridTemplateColumns: `140px repeat(${payers.length}, minmax(0, 1fr))` }}>
          <span />
          {payers.map(payer => (
            <span key={payer} style={{ ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', color: '#918D88', textAlign: 'center', display: 'block' }}>
              {payer === 'Blue Cross NC' ? 'BCNC' : payer === 'UnitedHealth' ? 'UHC' : payer}
            </span>
          ))}
        </div>

        {/* Drug rows */}
        {portfolio.map((drug, index) => (
          <motion.button key={drug.id} type="button" onClick={() => onSelectDrug(drug.id)}
            initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.04, duration: 0.15 }}
            className="grid items-center gap-3 w-full text-left py-2"
            style={{ gridTemplateColumns: `140px repeat(${payers.length}, minmax(0, 1fr))`, borderTop: index > 0 ? '1px solid #EBEBEB' : 'none' }}>
            <div>
              <p style={{ fontSize: '13px', fontWeight: 600, color: '#131210' }}>{drug.brandName}</p>
              <p style={{ ...mono, fontSize: '9px', color: '#918D88', marginTop: '1px' }}>{drug.jCode}</p>
            </div>
            {payers.map(payerName => {
              const trend = drug.trends.find(t => t.payerName === payerName)
              if (!trend) return <span key={payerName} />
              const cfg = statusConfig[trend.direction]
              return (
                <div key={payerName} className="flex flex-col items-center gap-1">
                  <span style={{ display: 'block', width: '100%', textAlign: 'center', padding: '5px 8px', borderRadius: '2px', background: cfg.bg, color: cfg.text, border: `1px solid ${cfg.border}`, ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', fontWeight: 600 }}>
                    {cfg.label}
                  </span>
                  <span style={{ ...mono, fontSize: '9px', color: cfg.text }}>
                    {trend.delta > 0 ? '+' : ''}{trend.delta} pts
                  </span>
                </div>
              )
            })}
          </motion.button>
        ))}

        {/* Legend */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '14px', paddingTop: '10px', marginTop: '8px', borderTop: '1px solid #EBEBEB' }}>
          {Object.entries(statusConfig).map(([key, cfg]) => (
            <div key={key} style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
              <div style={{ width: '8px', height: '8px', borderRadius: '1px', background: cfg.bg, border: `1px solid ${cfg.border}` }} />
              <span style={{ ...LABEL }}>{cfg.label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
