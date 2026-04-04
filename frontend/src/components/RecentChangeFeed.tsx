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

  return (
    <div className="rounded-xl overflow-hidden h-full flex flex-col"
      style={{ background: '#fff', border: '1px solid #E5E2DC' }}>

      {/* Header */}
      <div className="px-5 py-4 flex items-start justify-between flex-shrink-0"
        style={{ borderBottom: '1px solid #F0EDE7' }}>
        <div>
          <p className="text-[10px] font-bold uppercase tracking-widest mb-1" style={{ color: '#9CA3AF' }}>
            Recent Change Feed
          </p>
          <p className="text-sm font-semibold" style={{ color: '#0E1117' }}>
            What deserves attention now
          </p>
        </div>
        <span className="text-[10px] font-bold px-2 py-1 rounded-full flex-shrink-0"
          style={{ background: '#FEE2E2', color: '#DC2626' }}>
          Live Alerts
        </span>
      </div>

      {/* Feed */}
      <div className="flex-1 overflow-y-auto">
        {sorted.map((change, i) => {
          const dotColor = change.severity === 'HIGH' ? '#DC2626'
            : change.severity === 'MED' ? '#D97706' : '#9CA3AF'
          const title = `${change.payer} — ${changeTypeLabels[change.change_type] ?? change.change_type}`
          return (
            <div
              key={i}
              className="px-5 py-3.5 flex items-start gap-3"
              style={{ borderBottom: i < sorted.length - 1 ? '1px solid #F5F3EF' : 'none' }}
            >
              <span className="w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0" style={{ background: dotColor }} />
              <div className="flex-1 min-w-0">
                <div className="flex items-start justify-between gap-2">
                  <p className="text-xs font-semibold leading-snug" style={{ color: '#0E1117' }}>{title}</p>
                  <p className="text-[10px] font-mono flex-shrink-0" style={{ color: '#9CA3AF' }}>{change.date}</p>
                </div>
                <p className="text-xs mt-0.5 leading-relaxed" style={{ color: '#6B7280' }}>{change.summary}</p>
                <p className="text-[10px] mt-1 font-medium" style={{ color: '#9CA3AF' }}>{change.drug}</p>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
