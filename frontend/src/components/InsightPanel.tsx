import { motion } from 'framer-motion'
import type { InsightCard, InsightSource } from '../types/policy'

interface InsightPanelProps {
  insights: InsightCard[]
  sources?: InsightSource[]
  drugName?: string
  loading?: boolean
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

const severityConfig = {
  high:   { rail: '#B81C1C', label: 'CRITICAL INSIGHT' },
  medium: { rail: '#8B6428', label: 'NOTABLE DIFFERENCE' },
  low:    { rail: '#1A7840', label: 'OBSERVATION' },
}

export function InsightPanel({ insights, sources = [], drugName, loading }: InsightPanelProps) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
      <div style={{ marginBottom: '8px' }}>
        <p style={{ ...LABEL, color: '#91bfeb', marginBottom: '3px' }}>AI Analysis</p>
        <p style={{ fontSize: '14px', fontWeight: 700, color: '#131210' }}>
          Policy Insights{drugName ? ` — ${drugName}` : ''}
        </p>
        <p style={{ fontSize: '11px', color: '#4A4845', marginTop: '2px' }}>
          {insights.length} findings · with recommended actions
        </p>
      </div>

      {insights.map((insight, i) => {
        const cfg = severityConfig[insight.severity]
        return (
          <motion.div key={i}
            initial={{ opacity: 0, x: 6 }} animate={{ opacity: 1, x: 0 }}
            transition={{ delay: i * 0.05, duration: 0.13 }}
            style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderLeft: `3px solid ${cfg.rail}`, borderRadius: '2px', padding: '12px 13px' }}>
            <p style={{ ...LABEL, color: cfg.rail, marginBottom: '6px' }}>{cfg.label}</p>
            <p style={{ fontSize: '12px', lineHeight: 1.6, color: '#131210', marginBottom: '8px' }}>{insight.text}</p>
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: '6px', paddingTop: '8px', borderTop: '1px solid #EBEBEB' }}>
              <span style={{ ...mono, fontSize: '11px', color: cfg.rail, flexShrink: 0, marginTop: '1px' }}>→</span>
              <p style={{ fontSize: '11px', lineHeight: 1.5, color: '#4A4845' }}>{insight.action}</p>
            </div>
          </motion.div>
        )
      })}

      <div style={{ padding: '10px 12px', background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px', marginTop: '4px' }}>
        <p style={{ ...LABEL, opacity: loading ? 0.4 : 1, transition: 'opacity 0.3s', marginBottom: sources.length ? '8px' : 0 }}>
          {loading ? 'Generating live analysis…' : 'Structured extraction · LLM-generated'}
        </p>
        {!loading && sources.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
            <p style={{ ...LABEL, color: '#4A4845', marginBottom: '2px' }}>Source documents</p>
            {sources.map((src, i) => (
              <div key={i} style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: '8px' }}>
                <span style={{ fontSize: '10px', color: '#4A4845', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {src.payer}
                </span>
                {src.doc_hash && (
                  <span style={{ ...mono, fontSize: '9px', color: '#918D88', flexShrink: 0 }} title={`sha256: ${src.doc_hash}`}>
                    #{src.doc_hash.slice(0, 8)}
                  </span>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
