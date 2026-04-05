import type { ChangeEntry } from '../types/policy'
import { CHANGE_TYPE_LABELS } from '../lib/formatters'

interface RecentChangeFeedProps {
  changes: ChangeEntry[]
}

const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }
const LABEL: React.CSSProperties = { ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase' as const, color: '#918D88' }


const severityOrder = { HIGH: 0, MED: 1, LOW: 2 }

const sevStyle = {
  HIGH: { bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  MED:  { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  LOW:  { bg: '#F0EFEB', text: '#918D88', border: '#D8D4CC' },
}

export function RecentChangeFeed({ changes }: RecentChangeFeedProps) {
  const sorted = [...changes].sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity])
  const mix = {
    HIGH: changes.filter(c => c.severity === 'HIGH').length,
    MED:  changes.filter(c => c.severity === 'MED').length,
    LOW:  changes.filter(c => c.severity === 'LOW').length,
  }
  const total = Math.max(changes.length, 1)

  return (
    <div style={{ background: '#FFFFFF', border: '1px solid #D8D4CC', borderRadius: '2px', display: 'flex', flexDirection: 'column', overflow: 'hidden', height: '100%' }}>
      {/* Header */}
      <div style={{ padding: '10px 14px', background: '#F0EFEB', borderBottom: '1px solid #D8D4CC', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <p style={{ ...LABEL }}>Recent Changes</p>
        <span style={{ ...mono, fontSize: '9px', letterSpacing: '0.1em', textTransform: 'uppercase', color: '#91bfeb' }}>All →</span>
      </div>

      {/* Severity mix bar */}
      <div style={{ padding: '10px 14px', borderBottom: '1px solid #EBEBEB' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '5px' }}>
          <p style={{ ...LABEL }}>Severity mix</p>
          <p style={{ ...mono, fontSize: '9px', color: '#918D88' }}>{changes.length} changes</p>
        </div>
        <div style={{ display: 'flex', height: '3px', background: '#EBEBEB', borderRadius: '1px', overflow: 'hidden', gap: '1px' }}>
          {mix.HIGH > 0 && <div style={{ width: `${(mix.HIGH / total) * 100}%`, background: '#B81C1C' }} />}
          {mix.MED  > 0 && <div style={{ width: `${(mix.MED  / total) * 100}%`, background: '#8B6428' }} />}
          {mix.LOW  > 0 && <div style={{ width: `${(mix.LOW  / total) * 100}%`, background: '#918D88' }} />}
        </div>
        <div style={{ display: 'flex', gap: '12px', marginTop: '5px' }}>
          {(['HIGH', 'MED', 'LOW'] as const).map(sev => (
            <span key={sev} style={{ ...LABEL, color: sevStyle[sev].text }}>
              {sev} {mix[sev]}
            </span>
          ))}
        </div>
      </div>

      {/* Feed items */}
      <div style={{ flex: 1, overflowY: 'auto' }}>
        {sorted.map((change, i) => {
          const sev  = sevStyle[change.severity]
          const title = `${change.payer} — ${CHANGE_TYPE_LABELS[change.change_type] ?? change.change_type}`
          return (
            <div key={i} style={{ padding: '11px 14px', borderBottom: '1px solid #EBEBEB' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '7px', marginBottom: '4px' }}>
                <span style={{ ...mono, fontSize: '8px', letterSpacing: '0.1em', textTransform: 'uppercase', padding: '2px 5px', borderRadius: '1px', fontWeight: 600, background: sev.bg, color: sev.text, border: `1px solid ${sev.border}` }}>
                  {change.severity}
                </span>
                <span style={{ ...mono, fontSize: '9px', color: '#918D88', marginLeft: 'auto' }}>{change.date}</span>
              </div>
              <p style={{ fontSize: '12px', fontWeight: 600, color: '#131210', lineHeight: 1.3, marginBottom: '3px' }}>{title}</p>
              <p style={{ fontSize: '11px', color: '#4A4845', lineHeight: 1.5, marginBottom: '3px' }}>{change.summary}</p>
              <p style={{ ...mono, fontSize: '9px', color: '#91bfeb' }}>{change.drug}</p>
            </div>
          )
        })}
      </div>
    </div>
  )
}
