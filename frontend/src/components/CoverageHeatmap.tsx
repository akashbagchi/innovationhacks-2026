import { motion } from 'framer-motion'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'

interface CoverageHeatmapProps {
  portfolio: DrugPortfolioEntry[]
  onSelectDrug: (id: string) => void
}

const statusConfig = {
  tightening: { label: 'Tightened', bg: '#FDE8CC', text: '#D97706' },
  loosening: { label: 'Loosened', bg: '#D9F0DF', text: '#1F7A4C' },
  stable: { label: 'Stable', bg: 'rgba(15, 118, 110, 0.1)', text: '#0F766E' },
}

export function CoverageHeatmap({ portfolio, onSelectDrug }: CoverageHeatmapProps) {
  const payers = portfolio[0]?.trends.map(trend => trend.payerName) ?? []

  return (
    <div
      className="overflow-hidden rounded-[26px] border p-5 shadow-[0_18px_48px_rgba(18,52,51,0.1)] backdrop-blur-[18px]"
      style={{
        background: 'rgba(255, 252, 245, 0.82)',
        borderColor: 'rgba(53, 76, 72, 0.14)',
      }}
    >
      <div className="mb-5 flex items-start justify-between gap-3">
        <div>
          <p className="mb-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-[#5B716F]">
            Coverage Change Heatmap
          </p>
          <h3 className="text-[1.35rem] leading-tight text-[#123433]">
            Where payer policies shifted this quarter
          </h3>
        </div>
        <span className="rounded-full bg-[rgba(18,52,51,0.08)] px-3 py-1.5 text-xs font-semibold text-[#123433]">
          Q1 2026
        </span>
      </div>

      <div className="grid gap-3">
        <div
          className="grid items-center gap-3"
          style={{ gridTemplateColumns: `150px repeat(${payers.length}, minmax(0, 1fr))` }}
        >
          <span />
          {payers.map(payer => (
            <span key={payer} className="text-center text-[0.88rem] font-medium text-[#5B716F]">
              {payer}
            </span>
          ))}
        </div>

        {portfolio.map((drug, index) => (
          <motion.button
            key={drug.id}
            type="button"
            onClick={() => onSelectDrug(drug.id)}
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: index * 0.05, duration: 0.18 }}
            className="grid items-center gap-3 text-left"
            style={{ gridTemplateColumns: `150px repeat(${payers.length}, minmax(0, 1fr))` }}
          >
            <span className="text-sm font-semibold text-[#123433]">{drug.brandName}</span>
            {payers.map(payerName => {
              const trend = drug.trends.find(item => item.payerName === payerName)
              if (!trend) return <span key={payerName} />

              const config = statusConfig[trend.direction]
              return (
                <div
                  key={payerName}
                  className="flex flex-col items-center gap-1"
                >
                  <span
                    className="w-full rounded-[14px] px-3 py-2 text-center text-[0.86rem] font-semibold"
                    style={{ background: config.bg, color: config.text }}
                  >
                    {config.label}
                  </span>
                  <span className="text-[10px] font-mono tabular-nums" style={{ color: config.text }}>
                    {trend.delta > 0 ? '+' : ''}{trend.delta} pts
                  </span>
                </div>
              )
            })}
          </motion.button>
        ))}
      </div>

      <div className="mt-4 flex items-center gap-4 border-t pt-4" style={{ borderTopColor: 'rgba(53, 76, 72, 0.09)' }}>
        {Object.entries(statusConfig).map(([key, config]) => (
          <div key={key} className="flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full" style={{ background: config.text }} />
            <span className="text-[10px]" style={{ color: '#8B9692' }}>{config.label}</span>
          </div>
        ))}
        <span className="ml-auto text-[10px]" style={{ color: '#8B9692' }}>
          Click drug to view full analysis →
        </span>
      </div>
    </div>
  )
}
