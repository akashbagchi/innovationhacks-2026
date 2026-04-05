import { motion } from 'framer-motion'
import type { ChangeEntry } from '../types/policy'

interface ChangeDigestProps {
  changes: ChangeEntry[]
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }

const severityConfig = {
  HIGH: { rail: '#B81C1C', bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)', label: 'HIGH' },
  MED:  { rail: '#8B6428', bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)', label: 'MED' },
  LOW:  { rail: '#918D88', bg: '#F0EFEB', text: '#918D88', border: '#D8D4CC', label: 'LOW' },
}

const changeTypeLabels: Record<string, string> = {
  ADDED_STEP_THERAPY:   'Step therapy added',
  ADDED_CRITERION:      'Criterion added',
  REMOVED_CRITERION:    'Criterion removed',
  MODIFIED_THRESHOLD:   'Threshold changed',
  MODIFIED_WORDING:     'Wording updated',
  MODIFIED_PA_REQUIRED: 'PA status changed',
  ADDED_INDICATION:     'Indication added',
  REMOVED_INDICATION:   'Indication removed',
}

const criterionChipColors: Record<string, { bg: string; text: string; border: string }> = {
  step_therapy:            { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  combination_restriction: { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  prior_therapy:           { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  line_of_therapy:         { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  disease_severity:        { bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  lab_value:               { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  diagnosis:               { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  prescriber:              { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  clinical_response:       { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  other:                   { bg: '#F0EFEB', text: '#918D88', border: '#D8D4CC' },
}

export function ChangeDigest({ changes }: ChangeDigestProps) {
  const highCount  = changes.filter(c => c.severity === 'HIGH').length
  const payerCount = new Set(changes.map(c => c.payer)).size

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
        <div>
          <p style={{ ...LABEL, color: '#91bfeb', marginBottom: '4px' }}>Policy Surveillance</p>
          <p style={{ fontSize: '14px', fontWeight: 700, color: '#131210' }}>Q1 2026 — Policy Changes</p>
          <p style={{ fontSize: '12px', color: '#4A4845', marginTop: '3px' }}>
            {changes.length} changes across {payerCount} payer{payerCount !== 1 ? 's' : ''}
            {highCount > 0 && <span style={{ marginLeft: '8px', fontWeight: 600, color: '#B81C1C' }}>· {highCount} high-severity</span>}
          </p>
        </div>
        <button style={{ ...mono, fontSize: '10px', letterSpacing: '0.1em', textTransform: 'uppercase', padding: '6px 12px', borderRadius: '2px', border: '1px solid #D8D4CC', background: '#FFFFFF', color: '#4A4845', cursor: 'pointer' }}>
          Subscribe to alerts
        </button>
      </div>

      {/* Severity chips */}
      <div style={{ display: 'flex', gap: '6px' }}>
        {(['HIGH', 'MED', 'LOW'] as const).map(sev => {
          const count = changes.filter(c => c.severity === sev).length
          const cfg   = severityConfig[sev]
          return (
            <div key={sev} style={{ display: 'flex', alignItems: 'center', gap: '5px', padding: '4px 10px', background: cfg.bg, border: `1px solid ${cfg.border}`, borderRadius: '2px' }}>
              <span style={{ width: '5px', height: '5px', borderRadius: '50%', background: cfg.rail, flexShrink: 0 }} />
              <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', color: cfg.text, fontWeight: 600 }}>
                {count} {cfg.label}
              </span>
            </div>
          )
        })}
      </div>

      {/* Entries */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
        {changes.map((change, i) => {
          const cfg = severityConfig[change.severity]
          return (
            <motion.div key={i}
              initial={{ opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.04, duration: 0.13 }}
              style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderLeft: `3px solid ${cfg.rail}`, borderRadius: '2px', padding: '12px 14px' }}>
              {/* Meta row */}
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap', marginBottom: '6px' }}>
                <span style={{ ...mono, fontSize: '8px', letterSpacing: '0.1em', textTransform: 'uppercase', fontWeight: 600, color: cfg.text }}>
                  {cfg.label}
                </span>
                <span style={{ color: '#D8D4CC' }}>·</span>
                <span style={{ fontSize: '12px', fontWeight: 600, color: '#131210' }}>{change.payer}</span>
                <span style={{ color: '#D8D4CC' }}>·</span>
                <span style={{ fontSize: '12px', color: '#4A4845' }}>{change.drug}</span>
                <span style={{ color: '#D8D4CC' }}>·</span>
                <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: '#F0EFEB', border: '1px solid #D8D4CC', color: '#4A4845' }}>
                  {changeTypeLabels[change.change_type] ?? change.change_type}
                </span>
                {change.criterion_type && (() => {
                  const chip = criterionChipColors[change.criterion_type] ?? criterionChipColors.other
                  return (
                    <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: chip.bg, border: `1px solid ${chip.border}`, color: chip.text }}>
                      {change.criterion_type.replace(/_/g, ' ')}
                    </span>
                  )
                })()}
                {change.auth_phase === 'reauth' && (
                  <span style={{ ...mono, fontSize: '9px', padding: '1px 5px', borderRadius: '1px', background: '#F0EFEB', border: '1px solid #D8D4CC', color: '#4A4845' }}>reauth</span>
                )}
                <span style={{ ...mono, fontSize: '9px', color: '#918D88', marginLeft: 'auto' }}>{change.date}</span>
              </div>

              <p style={{ fontSize: '12px', lineHeight: 1.6, color: '#131210' }}>{change.summary}</p>

              {change.before_text && change.after_text && (
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginTop: '6px' }}>
                  <span style={{ ...mono, fontSize: '10px', padding: '2px 6px', borderRadius: '1px', background: '#FBEAEA', border: '1px solid rgba(184,28,28,0.2)', color: '#B81C1C' }}>
                    {change.before_text}mo
                  </span>
                  <span style={{ color: '#D8D4CC' }}>→</span>
                  <span style={{ ...mono, fontSize: '10px', padding: '2px 6px', borderRadius: '1px', background: '#E0F2E8', border: '1px solid rgba(26,120,64,0.2)', color: '#1A7840' }}>
                    {change.after_text}mo
                  </span>
                </div>
              )}
            </motion.div>
          )
        })}
      </div>

      <p style={{ ...LABEL, textAlign: 'center' }}>Showing all changes for the current quarter</p>
    </div>
  )
}
