import { motion } from 'framer-motion'
import type { InsightCard } from '../types/policy'

interface InsightPanelProps {
  insights: InsightCard[]
  drugName?: string
}

const severityConfig = {
  high:   { accent: '#DC2626', label: 'CRITICAL INSIGHT' },
  medium: { accent: '#D97706', label: 'NOTABLE DIFFERENCE' },
  low:    { accent: '#2D6A90', label: 'OBSERVATION' },
}

export function InsightPanel({ insights, drugName }: InsightPanelProps) {
  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="mb-5">
        <p className="text-[10px] font-semibold uppercase tracking-widest mb-1" style={{ color: '#2D6A90' }}>
          AI Analysis
        </p>
        <p className="text-sm font-semibold" style={{ color: '#0E1117' }}>
          Policy Insights{drugName ? ` — ${drugName}` : ''}
        </p>
        <p className="text-xs mt-0.5" style={{ color: '#6B7583' }}>
          {insights.length} findings · with recommended actions
        </p>
      </div>

      {insights.map((insight, i) => {
        const cfg = severityConfig[insight.severity]
        return (
          <motion.div
            key={i}
            initial={{ opacity: 0, x: 10 }}
            animate={{ opacity: 1, x: 0 }}
            transition={{ delay: i * 0.06, duration: 0.15, ease: 'easeOut' }}
            className="rounded-lg overflow-hidden"
            style={{
              background: '#fff',
              border: '1px solid #E2E7EF',
              borderLeft: `3px solid ${cfg.accent}`,
            }}
          >
            <div className="px-4 py-3.5">
              <p className="text-[10px] font-bold tracking-widest mb-2" style={{ color: cfg.accent }}>
                {cfg.label}
              </p>
              <p className="text-xs leading-relaxed mb-3" style={{ color: '#334155' }}>
                {insight.text}
              </p>
              {/* Action line */}
              <div className="flex items-start gap-1.5 pt-2.5" style={{ borderTop: '1px solid #E2E7EF' }}>
                <span className="text-[11px] flex-shrink-0 font-bold mt-px" style={{ color: cfg.accent }}>→</span>
                <p className="text-[11px] leading-relaxed" style={{ color: '#475569' }}>
                  {insight.action}
                </p>
              </div>
            </div>
          </motion.div>
        )
      })}

      {/* Footer */}
      <div className="rounded-lg px-3.5 py-2.5 mt-1" style={{ background: '#F5F6F8', border: '1px solid #E2E7EF' }}>
        <p className="text-[10px]" style={{ color: '#9AA3AF' }}>
          Structured extraction · LLM-generated narrative in production
        </p>
      </div>
    </div>
  )
}
