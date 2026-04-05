import { LayoutGrid, ArrowLeftRight, Bell } from 'lucide-react'
import type { ComponentType } from 'react'
import type { ChangeEntry } from '../types/policy'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'

const changeTypeShort: Record<string, string> = {
  ADDED_STEP_THERAPY:   'step therapy added',
  ADDED_CRITERION:      'criterion added',
  MODIFIED_THRESHOLD:   'threshold changed',
  MODIFIED_WORDING:     'wording updated',
  MODIFIED_PA_REQUIRED: 'PA status changed',
}

type NavItem = {
  id: string
  label: string
  Icon: ComponentType<{ size?: number; strokeWidth?: number }>
}

const navItems: NavItem[] = [
  { id: 'portfolio', label: 'Overview',        Icon: LayoutGrid },
  { id: 'compare',   label: 'Coverage Matrix', Icon: ArrowLeftRight },
  { id: 'digest',    label: 'Policy Changes',  Icon: Bell },
]

interface SidebarProps {
  active: string
  onNavigate: (id: string) => void
  changes: ChangeEntry[]
  portfolio: DrugPortfolioEntry[]
}

export function Sidebar({ active, onNavigate, changes, portfolio }: SidebarProps) {
  const allTrends       = portfolio.flatMap(d => d.trends)
  const tighteningCount = allTrends.filter(t => t.direction === 'tightening').length
  const totalTrends     = allTrends.length

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

  const alertCount     = changes.filter(c => c.severity === 'HIGH').length
  const watchlistItems = changes.filter(c => c.severity !== 'LOW').slice(0, 3)

  const mono: React.CSSProperties = { fontFamily: "'IBM Plex Mono', monospace" }

  return (
    <aside className="sticky top-0 flex h-screen w-[240px] flex-shrink-0 flex-col"
      style={{ background: '#FFFFFF', borderRight: '1px solid #D8D4CC' }}>

      {/* Logo */}
      <div className="px-5 py-5" style={{ borderBottom: '1px solid #D8D4CC' }}>
        <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase', color: '#918D88', marginBottom: '3px' }}>
          Anton Rx Track
        </p>
        <p style={{ fontSize: '22px', fontWeight: 700, letterSpacing: '-0.01em', lineHeight: 1 }}>
          <span style={{ color: '#131210' }}>ANTON</span>
          <span style={{ color: '#EF5050' }}>Rx</span>
        </p>
        <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.14em', textTransform: 'uppercase', color: '#918D88', marginTop: '3px' }}>
          Policy Intelligence
        </p>
      </div>

      {/* Nav */}
      <nav className="px-3 py-4 flex flex-col gap-0.5">
        <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase', color: '#918D88', padding: '0 8px', marginBottom: '5px' }}>
          Navigation
        </p>

        {navItems.map(({ id, label, Icon }) => {
          const isActive = active === id
          return (
            <button key={id} type="button" onClick={() => onNavigate(id)}
              className="flex items-center gap-2.5 px-2.5 py-2 text-left text-sm font-medium transition-all duration-100"
              style={{
                borderRadius: '2px',
                background: isActive ? '#FBEAEA' : 'transparent',
                border: isActive ? '1px solid rgba(239,80,80,0.25)' : '1px solid transparent',
                color: isActive ? '#EF5050' : '#4A4845',
              }}>
              <Icon size={13} strokeWidth={isActive ? 2.2 : 1.8} />
              <span>{label}</span>
              {id === 'digest' && alertCount > 0 && (
                <span className="ml-auto px-1.5 py-0.5 text-[9px] font-bold"
                  style={{ ...mono, background: '#B81C1C', color: '#fff', borderRadius: '1px' }}>
                  {alertCount}
                </span>
              )}
            </button>
          )
        })}
      </nav>

      {/* Quarter Signal */}
      <div className="mx-3 p-3" style={{ background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px' }}>
        <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase', color: '#918D88', marginBottom: '6px' }}>
          Quarter Signal
        </p>
        <p style={{ fontSize: '15px', fontWeight: 700, color: '#131210', lineHeight: 1.2 }}>
          {alertCount} high-impact changes
        </p>
        <p style={{ fontSize: '11px', color: '#4A4845', marginTop: '6px', lineHeight: 1.6 }}>
          {tighteningCount} of {totalTrends} payer trends tightening.{' '}
          <span style={{ color: '#131210', fontWeight: 600 }}>{shortMTP}</span>{' '}
          adding the most restrictions.
        </p>
      </div>

      {/* Watchlist */}
      <div className="mx-3 mt-2.5 p-3 flex-1" style={{ background: '#F0EFEB', border: '1px solid #D8D4CC', borderRadius: '2px' }}>
        <p style={{ ...mono, fontSize: '9px', letterSpacing: '0.2em', textTransform: 'uppercase', color: '#918D88', marginBottom: '8px' }}>
          Watchlist
        </p>
        <ul className="space-y-2">
          {watchlistItems.map(item => (
            <li key={`${item.payer}-${item.change_type}-${item.date}`} className="flex items-start gap-2"
              style={{ paddingBottom: '7px', borderBottom: '1px solid #EBEBEB' }}>
              <span className="mt-1 flex-shrink-0" style={{
                width: '5px', height: '5px', borderRadius: '50%',
                background: item.severity === 'HIGH' ? '#B81C1C' : '#8B6428',
              }} />
              <span style={{ fontSize: '11px', color: '#4A4845', lineHeight: 1.5 }}>
                <span style={{ color: '#131210', fontWeight: 600 }}>{item.payer}</span>{' '}
                {changeTypeShort[item.change_type] ?? 'policy updated'} — {item.drug}
              </span>
            </li>
          ))}
        </ul>
      </div>

      {/* Footer */}
      <div className="px-5 py-3" style={{ borderTop: '1px solid #D8D4CC' }}>
        <p style={{ ...mono, fontSize: '10px', color: '#918D88' }}>
          {portfolio.length} products · Q1 2026
        </p>
      </div>
    </aside>
  )
}
