import { motion } from 'framer-motion'
import type { ChangeEntry } from '../types/policy'

interface ChangeDigestProps {
  changes: ChangeEntry[]
}

const severityConfig = {
  HIGH: { accent: '#DC2626', label: 'HIGH' },
  MED:  { accent: '#D97706', label: 'MED' },
  LOW:  { accent: '#6B7583', label: 'LOW' },
}

const changeTypeLabels: Record<string, string> = {
  ADDED_STEP_THERAPY:   'Step therapy added',
  ADDED_CRITERION:      'Criterion added',
  REMOVED_CRITERION:    'Criterion removed',
  MODIFIED_THRESHOLD:   'Threshold changed',
  MODIFIED_WORDING:     'Wording updated',
  MODIFIED_PA_REQUIRED: 'PA status changed',
}

export function ChangeDigest({ changes }: ChangeDigestProps) {
  const highCount  = changes.filter(c => c.severity === 'HIGH').length
  const payerCount = new Set(changes.map(c => c.payer)).size

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-widest mb-1" style={{ color: '#2D6A90' }}>
            Policy Surveillance
          </p>
          <p className="text-sm font-semibold" style={{ color: '#0E1117' }}>Q1 2026 — Policy Changes</p>
          <p className="text-xs mt-1" style={{ color: '#6B7583' }}>
            {changes.length} changes across {payerCount} payer{payerCount !== 1 ? 's' : ''}
            {highCount > 0 && (
              <span className="ml-2 font-semibold" style={{ color: '#DC2626' }}>
                · {highCount} high-severity
              </span>
            )}
          </p>
        </div>
        <button className="text-xs px-3 py-1.5 rounded-lg border transition-colors"
          style={{ borderColor: '#E2E7EF', color: '#6B7583', background: '#fff' }}>
          Subscribe to alerts
        </button>
      </div>

      {/* Count chips */}
      <div className="flex gap-2">
        {(['HIGH', 'MED', 'LOW'] as const).map(sev => {
          const count = changes.filter(c => c.severity === sev).length
          const cfg = severityConfig[sev]
          return (
            <div key={sev} className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium"
              style={{ background: '#fff', border: '1px solid #E2E7EF', color: cfg.accent }}>
              <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ background: cfg.accent }} />
              {count} {cfg.label}
            </div>
          )
        })}
      </div>

      {/* Change entries */}
      <div className="space-y-2.5">
        {changes.map((change, i) => {
          const cfg = severityConfig[change.severity]
          return (
            <motion.div
              key={i}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.05, duration: 0.15, ease: 'easeOut' }}
              className="rounded-lg overflow-hidden"
              style={{
                background: '#fff',
                border: '1px solid #E2E7EF',
                borderLeft: `3px solid ${cfg.accent}`,
              }}
            >
              <div className="px-4 py-3.5">
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap mb-1.5">
                      <span className="text-[10px] font-bold tracking-wider" style={{ color: cfg.accent }}>
                        {cfg.label}
                      </span>
                      <span style={{ color: '#D0D9E4' }}>·</span>
                      <span className="text-xs font-semibold" style={{ color: '#0E1117' }}>{change.payer}</span>
                      <span style={{ color: '#D0D9E4' }}>·</span>
                      <span className="text-xs" style={{ color: '#6B7583' }}>{change.drug}</span>
                      <span style={{ color: '#D0D9E4' }}>·</span>
                      <span className="text-[10px] font-mono px-1.5 py-0.5 rounded"
                        style={{ background: '#F5F6F8', color: '#6B7583' }}>
                        {changeTypeLabels[change.change_type] ?? change.change_type}
                      </span>
                    </div>
                    <p className="text-sm leading-relaxed" style={{ color: '#334155' }}>{change.summary}</p>
                  </div>
                  <div className="text-[11px] flex-shrink-0 font-mono tabular-nums" style={{ color: '#9AA3AF' }}>
                    {change.date}
                  </div>
                </div>
              </div>
            </motion.div>
          )
        })}
      </div>

      <p className="text-[10px] text-center" style={{ color: '#9AA3AF' }}>
        Showing all changes for the current quarter
      </p>
    </div>
  )
}
