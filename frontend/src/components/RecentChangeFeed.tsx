import type { ChangeEntry } from '../types/policy'

interface RecentChangeFeedProps {
  changes: ChangeEntry[]
}

const changeTypeLabels: Record<string, string> = {
  ADDED_STEP_THERAPY:   'Step Therapy Added',
  ADDED_CRITERION:      'Criterion Added',
  REMOVED_CRITERION:    'Criterion Removed',
  MODIFIED_THRESHOLD:   'Threshold Changed',
  MODIFIED_WORDING:     'Wording Updated',
  MODIFIED_PA_REQUIRED: 'PA Status Changed',
}

const severityOrder = { HIGH: 0, MED: 1, LOW: 2 }

export function RecentChangeFeed({ changes }: RecentChangeFeedProps) {
  const sorted = [...changes].sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity])
  const severityMix = {
    HIGH: changes.filter(change => change.severity === 'HIGH').length,
    MED: changes.filter(change => change.severity === 'MED').length,
    LOW: changes.filter(change => change.severity === 'LOW').length,
  }
  const total = Math.max(changes.length, 1)

  return (
    <div
      className="flex h-full flex-col overflow-hidden rounded-[26px] border shadow-[0_18px_48px_rgba(18,52,51,0.1)] backdrop-blur-[18px]"
      style={{ background: 'rgba(255, 252, 245, 0.82)', borderColor: 'rgba(53, 76, 72, 0.14)' }}
    >

      {/* Header */}
      <div className="flex flex-shrink-0 items-start justify-between px-5 py-4"
        style={{ borderBottom: '1px solid rgba(53, 76, 72, 0.09)' }}>
        <div>
          <p className="mb-1 text-[11px] font-semibold uppercase tracking-[0.14em]" style={{ color: '#5B716F' }}>
            Recent Change Feed
          </p>
          <h3 className="text-[1.35rem] leading-tight" style={{ color: '#123433' }}>
            What deserves attention now
          </h3>
        </div>
        <span className="text-[10px] font-bold px-2 py-1 rounded-full flex-shrink-0"
          style={{ background: '#F5D5CF', color: '#B93823' }}>
          Live Alerts
        </span>
      </div>

      <div className="px-5 pt-4">
        <div className="mb-2 flex items-center justify-between">
          <p className="text-[10px] font-semibold uppercase tracking-[0.16em]" style={{ color: '#8B9692' }}>
            Severity mix
          </p>
          <p className="text-[10px]" style={{ color: '#8B9692' }}>
            {changes.length} changes
          </p>
        </div>
        <div className="flex h-2 overflow-hidden rounded-full" style={{ background: 'rgba(18, 52, 51, 0.08)' }}>
          {severityMix.HIGH > 0 && <div style={{ width: `${(severityMix.HIGH / total) * 100}%`, background: '#B93823' }} />}
          {severityMix.MED > 0 && <div style={{ width: `${(severityMix.MED / total) * 100}%`, background: '#D97706' }} />}
          {severityMix.LOW > 0 && <div style={{ width: `${(severityMix.LOW / total) * 100}%`, background: '#7BA8C4' }} />}
        </div>
        <div className="mt-2 flex flex-wrap gap-3">
          <span className="text-[10px]" style={{ color: '#8B9692' }}>High {severityMix.HIGH}</span>
          <span className="text-[10px]" style={{ color: '#8B9692' }}>Med {severityMix.MED}</span>
          <span className="text-[10px]" style={{ color: '#8B9692' }}>Low {severityMix.LOW}</span>
        </div>
      </div>

      {/* Feed */}
      <div className="flex-1 overflow-y-auto pt-2">
        {sorted.map((change, i) => {
          const dotColor = change.severity === 'HIGH' ? '#DC2626'
            : change.severity === 'MED' ? '#D97706' : '#9CA3AF'
          const title = `${change.payer} — ${changeTypeLabels[change.change_type] ?? change.change_type}`
          return (
            <div
              key={i}
              className="mx-4 my-2 flex items-start gap-3 rounded-[20px] border px-4 py-4"
              style={{
                borderColor: 'rgba(53, 76, 72, 0.14)',
                background: 'rgba(255, 255, 255, 0.58)',
              }}
            >
              <span className="w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0" style={{ background: dotColor }} />
              <div className="flex-1 min-w-0">
                <div className="flex items-start justify-between gap-2">
                  <p className="text-sm font-semibold leading-snug" style={{ color: '#123433' }}>{title}</p>
                  <p className="text-[11px] font-medium flex-shrink-0" style={{ color: '#5B716F' }}>{change.date}</p>
                </div>
                <p className="text-sm mt-1 leading-relaxed" style={{ color: '#5B716F' }}>{change.summary}</p>
                <p className="text-[11px] mt-2 font-medium" style={{ color: '#8B9692' }}>{change.drug}</p>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
