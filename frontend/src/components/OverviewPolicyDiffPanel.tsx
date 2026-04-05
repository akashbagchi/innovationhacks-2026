import { useMemo, useState } from 'react'
import type { ChangeEntry } from '../types/policy'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'

interface OverviewPolicyDiffPanelProps {
  portfolio: DrugPortfolioEntry[]
  changes: ChangeEntry[]
}

const severityPalette = {
  HIGH: { bg: '#FDE7E3', text: '#B54522', rail: '#EA6B42' },
  MED: { bg: '#FEEFD8', text: '#C27A14', rail: '#F1A32A' },
  LOW: { bg: '#E8F2ED', text: '#2E7C5A', rail: '#53A87A' },
}

const changeTypeLabels: Record<string, string> = {
  ADDED_STEP_THERAPY: 'Step therapy added',
  ADDED_CRITERION: 'Criterion added',
  REMOVED_CRITERION: 'Criterion removed',
  MODIFIED_THRESHOLD: 'Threshold shifted',
  MODIFIED_WORDING: 'Language updated',
  MODIFIED_PA_REQUIRED: 'PA status changed',
}

function formatDate(date: string) {
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
  }).format(new Date(date))
}

function buildPolicyPairs(drug: DrugPortfolioEntry) {
  return drug.policies.map(policy => {
    const updateCount = drug.policies.length > 1
      ? Math.max(1, Math.round(policy.indications.length / 2))
      : 1

    return {
      id: policy.payer.name,
      label: `${policy.payer.name} latest revision`,
      caption: `${updateCount} material change${updateCount === 1 ? '' : 's'}`,
    }
  })
}

function getGraphicMix(changes: ChangeEntry[]) {
  const totals = {
    added: 0,
    changed: 0,
    removed: 0,
  }

  changes.forEach(change => {
    if (change.change_type.startsWith('ADDED')) {
      totals.added += 1
      return
    }

    if (change.change_type.startsWith('REMOVED')) {
      totals.removed += 1
      return
    }

    totals.changed += 1
  })

  const totalCount = changes.length || 1

  return [
    { label: 'Added', value: totals.added, color: '#53A87A', width: `${(totals.added / totalCount) * 100}%` },
    { label: 'Changed', value: totals.changed, color: '#F1A32A', width: `${(totals.changed / totalCount) * 100}%` },
    { label: 'Removed', value: totals.removed, color: '#EA6B42', width: `${(totals.removed / totalCount) * 100}%` },
  ]
}

function derivePolicyChangeScope(selectedDrug: DrugPortfolioEntry, allChanges: ChangeEntry[], selectedPair: string) {
  const matching = allChanges.filter(change =>
    change.drug.toLowerCase().includes(selectedDrug.brandName.toLowerCase()) ||
    change.drug.toLowerCase().includes(selectedDrug.genericName.toLowerCase())
  )

  const payerFiltered = matching.filter(change => change.payer === selectedPair)
  return payerFiltered.length > 0 ? payerFiltered : matching
}

export function OverviewPolicyDiffPanel({ portfolio, changes }: OverviewPolicyDiffPanelProps) {
  const [selectedDrugId, setSelectedDrugId] = useState(portfolio[1]?.id ?? portfolio[0]?.id ?? '')
  const selectedDrug = portfolio.find(entry => entry.id === selectedDrugId) ?? portfolio[0]
  const policyPairs = useMemo(() => buildPolicyPairs(selectedDrug), [selectedDrug])
  const [selectedPair, setSelectedPair] = useState(policyPairs[0]?.id ?? '')

  const visibleChanges = useMemo(
    () => derivePolicyChangeScope(selectedDrug, changes, selectedPair || policyPairs[0]?.id || ''),
    [selectedDrug, changes, selectedPair, policyPairs]
  )

  const mix = getGraphicMix(visibleChanges)
  const materialCount = visibleChanges.filter(change => change.severity !== 'LOW').length

  return (
    <div
      className="space-y-5 rounded-[28px] border p-5 shadow-[0_18px_48px_rgba(18,52,51,0.1)] backdrop-blur-[18px]"
      style={{ background: 'rgba(255, 252, 245, 0.82)', borderColor: 'rgba(53, 76, 72, 0.14)' }}
    >
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-[#7A8B85]">Policy Diff</p>
          <h3 className="mt-2 text-[30px] font-semibold tracking-tight text-[#14343A]">
            {selectedPair || policyPairs[0]?.label || 'Revision snapshot'}
          </h3>
        </div>

        <span className="inline-flex rounded-full bg-[#FEE8CC] px-3 py-1.5 text-xs font-semibold text-[#D8871D]">
          {materialCount} material changes
        </span>
      </div>

      <div className="grid gap-3">
        <label className="space-y-2">
          <span className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#8B9692]">Drug</span>
          <select
            value={selectedDrugId}
            onChange={event => {
              const nextDrug = portfolio.find(entry => entry.id === event.target.value) ?? portfolio[0]
              const nextPairs = buildPolicyPairs(nextDrug)
              setSelectedDrugId(event.target.value)
              setSelectedPair(nextPairs[0]?.id ?? '')
            }}
            className="w-full rounded-2xl border px-4 py-3 text-sm text-[#27404A] outline-none"
            style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 255, 255, 0.7)' }}
          >
            {portfolio.map(drug => (
              <option key={drug.id} value={drug.id}>{drug.brandName}</option>
            ))}
          </select>
        </label>

        <label className="space-y-2">
          <span className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#8B9692]">Compare focus</span>
          <select
            value={selectedPair || policyPairs[0]?.id || ''}
            onChange={event => setSelectedPair(event.target.value)}
            className="w-full rounded-2xl border px-4 py-3 text-sm text-[#27404A] outline-none"
            style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 255, 255, 0.7)' }}
          >
            {policyPairs.map(pair => (
              <option key={pair.id} value={pair.id}>{pair.label}</option>
            ))}
          </select>
        </label>
      </div>

      <div className="rounded-[24px] border p-4" style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 255, 255, 0.58)' }}>
        <div className="flex items-center justify-between">
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[#8B9692]">Change mix</p>
          <p className="text-xs text-[#6B7D80]">{visibleChanges.length} updates in scope</p>
        </div>

        <div className="mt-3 flex h-3 overflow-hidden rounded-full bg-[#ECE8DF]">
          {mix.map(segment => (
            segment.value > 0 && (
              <div
                key={segment.label}
                title={`${segment.label}: ${segment.value}`}
                style={{ width: segment.width, background: segment.color }}
              />
            )
          ))}
        </div>

        <div className="mt-4 grid gap-2 sm:grid-cols-3">
          {mix.map(segment => (
            <div key={segment.label} className="rounded-2xl px-3 py-3" style={{ background: 'rgba(244, 241, 232, 0.88)' }}>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#8B9692]">{segment.label}</p>
              <p className="mt-1 text-xl font-semibold text-[#14343A]">{segment.value}</p>
            </div>
          ))}
        </div>
      </div>

      <div className="space-y-3">
        {visibleChanges.map(change => {
          const style = severityPalette[change.severity]

          return (
            <div
              key={`${change.payer}-${change.change_type}-${change.date}`}
              className="relative rounded-[24px] border p-4 pl-5"
              style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 255, 255, 0.58)' }}
            >
              <div
                className="absolute left-0 top-5 bottom-5 w-1 rounded-full"
                style={{ background: style.rail }}
              />
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="space-y-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <span
                      className="inline-flex rounded-full px-3 py-1 text-[11px] font-semibold"
                      style={{ background: style.bg, color: style.text }}
                    >
                      {change.change_type.startsWith('ADDED')
                        ? 'Added'
                        : change.change_type.startsWith('REMOVED')
                          ? 'Removed'
                          : 'Changed'}
                    </span>
                    <span className="text-sm font-semibold text-[#14343A]">
                      {changeTypeLabels[change.change_type] ?? change.change_type}
                    </span>
                  </div>
                  <p className="text-sm leading-6 text-[#62757B]">{change.summary}</p>
                </div>

                <span className="text-xs font-medium text-[#8B9692]">{formatDate(change.date)}</span>
              </div>
            </div>
          )
        })}
      </div>

      <div className="rounded-[24px] border px-4 py-4" style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 255, 255, 0.58)' }}>
        <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#8B9692]">Why this view works</p>
        <p className="mt-2 text-sm leading-6 text-[#62757B]">
          The stacked change mix gives a quick visual read on how the policy is evolving, while the rail cards preserve the exact text of each substantive update.
        </p>
      </div>
    </div>
  )
}
