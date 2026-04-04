import { motion } from 'framer-motion'
import { ChevronRight } from 'lucide-react'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'

interface CoverageHeatmapProps {
  portfolio: DrugPortfolioEntry[]
  onSelectDrug: (id: string) => void
}

const statusConfig = {
  tightening: { label: 'Tightening', bg: '#FEF3C7', text: '#92400E', dot: '#D97706' },
  loosening:  { label: 'Loosening',  bg: '#D1FAE5', text: '#065F46', dot: '#10A090' },
  stable:     { label: 'Stable',     bg: '#F3F4F6', text: '#6B7280', dot: '#9CA3AF' },
}

export function CoverageHeatmap({ portfolio, onSelectDrug }: CoverageHeatmapProps) {
  const payers = portfolio[0]?.trends.map(t => t.payerName) ?? []

  function shortPayer(name: string) {
    if (name === 'Blue Cross NC') return 'Blue Cross NC'
    if (name === 'UnitedHealth')  return 'UnitedHealth'
    return name
  }

  return (
    <div className="rounded-xl overflow-hidden" style={{ background: '#fff', border: '1px solid #E5E2DC' }}>
      {/* Section header */}
      <div className="px-5 py-4 flex items-start justify-between" style={{ borderBottom: '1px solid #F0EDE7' }}>
        <div>
          <p className="text-[10px] font-bold uppercase tracking-widest mb-1" style={{ color: '#9CA3AF' }}>
            Coverage Change Heatmap
          </p>
          <p className="text-sm font-semibold" style={{ color: '#0E1117' }}>
            Where payer policies shifted this quarter
          </p>
        </div>
        <span className="text-[11px] font-mono px-2 py-1 rounded"
          style={{ background: '#F9F8F5', color: '#9CA3AF', border: '1px solid #E5E2DC' }}>
          Q1 2026
        </span>
      </div>

      {/* Table */}
      <div className="px-5 py-4">
        {/* Payer header row */}
        <div className="grid mb-3" style={{ gridTemplateColumns: `160px repeat(${payers.length}, 1fr)` }}>
          <div /> {/* empty drug name column */}
          {payers.map(p => (
            <div key={p} className="text-center">
              <p className="text-xs font-medium" style={{ color: '#6B7280' }}>{shortPayer(p)}</p>
            </div>
          ))}
        </div>

        {/* Drug rows */}
        <div className="space-y-2">
          {portfolio.map((drug, di) => (
            <motion.div
              key={drug.id}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: di * 0.06, duration: 0.2 }}
              className="grid items-center group cursor-pointer rounded-lg transition-colors"
              style={{ gridTemplateColumns: `160px repeat(${payers.length}, 1fr)` }}
              onClick={() => onSelectDrug(drug.id)}
            >
              {/* Drug name */}
              <div className="flex items-center gap-1.5 py-2 pr-3">
                <p className="text-sm font-semibold group-hover:text-[#2D6A90] transition-colors truncate"
                  style={{ color: '#0E1117' }}>
                  {drug.brandName}
                </p>
                <ChevronRight size={12} style={{ color: '#C4C9D1', flexShrink: 0 }}
                  className="group-hover:translate-x-0.5 group-hover:text-[#7BA8C4] transition-all opacity-0 group-hover:opacity-100" />
              </div>

              {/* Payer cells */}
              {payers.map((payerName, pi) => {
                const trend = drug.trends.find(t => t.payerName === payerName)
                if (!trend) return <div key={payerName} />
                const cfg = statusConfig[trend.direction]
                return (
                  <motion.div
                    key={payerName}
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{ delay: di * 0.06 + pi * 0.04, duration: 0.15 }}
                    className="px-2 py-2"
                  >
                    <div className="flex flex-col items-center gap-0.5">
                      <span className="text-[11px] font-semibold px-3 py-1 rounded-full w-full text-center"
                        style={{ background: cfg.bg, color: cfg.text }}>
                        {cfg.label}
                      </span>
                      <span className="text-[10px] tabular-nums font-mono"
                        style={{ color: cfg.dot }}>
                        {trend.delta > 0 ? '+' : ''}{trend.delta} pts
                      </span>
                    </div>
                  </motion.div>
                )
              })}
            </motion.div>
          ))}
        </div>

        {/* Legend */}
        <div className="flex items-center gap-4 mt-4 pt-3" style={{ borderTop: '1px solid #F0EDE7' }}>
          {Object.entries(statusConfig).map(([key, cfg]) => (
            <div key={key} className="flex items-center gap-1.5">
              <span className="w-2 h-2 rounded-full" style={{ background: cfg.dot }} />
              <span className="text-[10px]" style={{ color: '#9CA3AF' }}>{cfg.label}</span>
            </div>
          ))}
          <span className="text-[10px] ml-auto" style={{ color: '#C4C9D1' }}>
            Click drug to view full analysis →
          </span>
        </div>
      </div>
    </div>
  )
}
