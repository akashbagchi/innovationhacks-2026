import { LayoutGrid, ArrowLeftRight, Bell } from 'lucide-react'
import type { ComponentType } from 'react'
import type { ChangeEntry } from '../types/policy'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'

const changeTypeShort: Record<string, string> = {
  ADDED_STEP_THERAPY: 'step therapy added',
  ADDED_CRITERION: 'criterion added',
  MODIFIED_THRESHOLD: 'threshold changed',
  MODIFIED_WORDING: 'wording updated',
  MODIFIED_PA_REQUIRED: 'PA status changed',
}

type NavItem = {
  id: string
  label: string
  Icon: ComponentType<{ size?: number; strokeWidth?: number }>
}

const navItems: NavItem[] = [
  { id: 'portfolio', label: 'Overview', Icon: LayoutGrid },
  { id: 'compare', label: 'Coverage Matrix', Icon: ArrowLeftRight },
  { id: 'digest', label: 'Policy Changes', Icon: Bell },
]

interface SidebarProps {
  active: string
  onNavigate: (id: string) => void
  changes: ChangeEntry[]
  portfolio: DrugPortfolioEntry[]
}

export function Sidebar({ active, onNavigate, changes, portfolio }: SidebarProps) {
  const allTrends = portfolio.flatMap(d => d.trends)
  const tighteningCount = allTrends.filter(t => t.direction === 'tightening').length
  const totalTrends = allTrends.length

  const payerTightenCount: Record<string, number> = {}
  allTrends.filter(t => t.direction === 'tightening').forEach(t => {
    payerTightenCount[t.payerName] = (payerTightenCount[t.payerName] ?? 0) + 1
  })

  const mostTighteningPayer = Object.entries(payerTightenCount)
    .sort((a, b) => b[1] - a[1])[0]?.[0] ?? 'Unknown'

  const shortMTP = mostTighteningPayer === 'Blue Cross NC'
    ? 'BCNC'
    : mostTighteningPayer === 'UnitedHealth'
      ? 'UHC'
      : mostTighteningPayer

  const alertCount = changes.filter(c => c.severity === 'HIGH').length
  const watchlistItems = changes.filter(c => c.severity !== 'LOW').slice(0, 3)
  return (
    <aside
      className="glass-card sticky top-6 flex h-[calc(100vh-3rem)] w-[290px] flex-shrink-0 flex-col gap-4 rounded-[28px] p-5"
      style={{
        background: 'linear-gradient(180deg, rgba(255, 252, 245, 0.9), rgba(247, 242, 232, 0.84))',
      }}
    >
      <div className="flex items-center gap-4">
        <div
          className="flex h-[62px] w-[62px] items-center justify-center rounded-[20px] overflow-hidden shadow-[0_14px_32px_rgba(18,52,51,0.18)]"
          style={{
            background: '#0E1117',
          }}
        >
          <svg width="32" height="32" viewBox="0 0 18 18" fill="none" aria-hidden="true">
            <circle cx="9" cy="9" r="8" fill="#161C26" />
            <path d="M1 9 A8 8 0 0 1 17 9 Z" fill="#7BA8C4" opacity="0.35" />
            <path d="M4 13 L9 5 L14 13 Z" fill="#7BA8C4" />
            <path d="M7 13 L9 8.5 L11 13 Z" fill="rgba(255,255,255,0.12)" />
          </svg>
        </div>
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-[#5B716F]">Anton Rx Track</p>
          <p className="mt-1 text-[1.7rem] font-bold leading-none tracking-tight">
            <span style={{ color: '#0E1117' }}>ANTON</span>
            <span style={{ color: '#7BA8C4' }}>Rx</span>
          </p>
          <p className="mt-1 text-[12px] font-medium uppercase tracking-[0.12em] text-[#5B716F]">Policy Intelligence</p>
        </div>
      </div>

      <nav className="grid gap-2">
        {navItems.map(({ id, label, Icon }) => {
          const isActive = active === id

          return (
            <button
              key={id}
              type="button"
              onClick={() => onNavigate(id)}
              className="flex items-center gap-3 rounded-[14px] px-3.5 py-3 text-left text-[15px] transition-all duration-150"
              style={{
                background: isActive ? 'rgba(15, 118, 110, 0.12)' : 'transparent',
                color: '#123433',
                transform: isActive ? 'translateX(3px)' : 'none',
              }}
            >
              <Icon size={16} strokeWidth={isActive ? 2.1 : 1.7} />
              <span>{label}</span>
              {id === 'digest' && alertCount > 0 && (
                <span
                  className="ml-auto rounded-full px-2 py-0.5 text-[10px] font-semibold"
                  style={{ background: '#F5D5CF', color: '#B93823' }}
                >
                  {alertCount}
                </span>
              )}
            </button>
          )
        })}
      </nav>

      <section className="rounded-[20px] border border-[#354C4824] bg-white/60 p-4">
        <p className="mb-2 text-[11px] uppercase tracking-[0.12em] text-[#5B716F]">Quarter Signal</p>
        <h2 className="m-0 text-[1.2rem] leading-tight text-[#123433]">{alertCount} high-impact changes</h2>
        <p className="mt-3 text-sm leading-6 text-[#5B716F]">
          {tighteningCount} of {totalTrends} payer trends tightening. <span className="font-medium text-[#123433]">{shortMTP}</span> adding the most restrictions this quarter.
        </p>
      </section>

      <section className="mt-auto rounded-[20px] border border-[#354C4824] bg-white/60 p-4">
        <p className="mb-2 text-[11px] uppercase tracking-[0.12em] text-[#5B716F]">Watchlist</p>
        <ul className="space-y-2.5 text-xs leading-relaxed text-[#374151]">
          {watchlistItems.map(item => (
            <li key={`${item.payer}-${item.change_type}-${item.date}`} className="flex items-start gap-2">
              <span
                className="mt-1.5 h-1.5 w-1.5 flex-shrink-0 rounded-full"
                style={{ background: item.severity === 'HIGH' ? '#DC2626' : '#D97706' }}
              />
              <span>
                <span className="font-medium">{item.payer}</span> {changeTypeShort[item.change_type] ?? 'policy updated'} - {item.drug}
              </span>
            </li>
          ))}
        </ul>
      </section>

      <div className="pt-1">
        <p className="text-[10px] font-mono" style={{ color: '#8B9692' }}>
          {portfolio.length} products · Q1 2026
        </p>
      </div>
    </aside>
  )
}
