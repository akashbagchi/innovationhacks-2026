import { useMemo, useState } from 'react'
import type { DrugPortfolioEntry } from '../data/mockPortfolio'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from './ui/dialog'

interface OverviewMatrixPanelProps {
  portfolio: DrugPortfolioEntry[]
}

type MatrixColumn = 'coverage' | 'pa' | 'stepTherapy' | 'restriction' | 'confidence' | 'updated'

interface MatrixRow {
  payer: string
  coverage: string
  pa: string
  stepTherapy: string
  restriction: string
  confidence: string
  updated: string
}

const DEFAULT_COLUMNS: MatrixColumn[] = ['coverage', 'pa', 'stepTherapy', 'restriction', 'confidence', 'updated']

const columnLabels: Record<MatrixColumn, string> = {
  coverage: 'Coverage',
  pa: 'PA',
  stepTherapy: 'Step Therapy',
  restriction: 'Key Restriction',
  confidence: 'Confidence',
  updated: 'Updated',
}

const coverageStyles: Record<string, { bg: string; text: string }> = {
  Covered: { bg: '#DDF4E5', text: '#18794E' },
  Conditional: { bg: '#FEF0DB', text: '#C4660A' },
  'No Policy': { bg: '#ECEDE9', text: '#5F6B66' },
}

function formatDate(date?: string) {
  if (!date) return 'Unknown'

  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
  }).format(new Date(date))
}

function normalizeConfidence(score: number) {
  if (score >= 0.9) return 'High'
  if (score >= 0.75) return 'Medium'
  return 'Low'
}

function summarizeRestriction(criteria: string[]) {
  if (criteria.length === 0) return 'No explicit restriction'
  return criteria[0]
}

function buildMatrixRows(drug: DrugPortfolioEntry, indicationFilter: string): MatrixRow[] {
  return drug.policies.map(policy => {
    const indication = policy.indications.find(item => item.name === indicationFilter)
    const allCriteria = policy.indications.flatMap(item =>
      item.initial_authorization.criteria.map(criterion => criterion.description)
    )

    if (!indication) {
      return {
        payer: policy.payer.name,
        coverage: 'No Policy',
        pa: 'Unknown',
        stepTherapy: 'Unknown',
        restriction: policy.exclusions?.[0]?.description ?? summarizeRestriction(allCriteria),
        confidence: normalizeConfidence(policy.confidence_scores.overall),
        updated: formatDate(policy.payer.revision_date ?? policy.payer.effective_date),
      }
    }

    const restriction = summarizeRestriction(
      indication.initial_authorization.criteria.map(criterion => criterion.description)
    )

    const hasCriteria = indication.initial_authorization.criteria.length > 0 || indication.step_therapy_required

    return {
      payer: policy.payer.name,
      coverage: indication.pa_required || hasCriteria ? 'Covered' : 'Conditional',
      pa: indication.pa_required ? 'Required' : 'Not required',
      stepTherapy: indication.step_therapy_required ? 'Yes' : 'No',
      restriction,
      confidence: normalizeConfidence(policy.confidence_scores.overall),
      updated: formatDate(policy.payer.revision_date ?? policy.payer.effective_date),
    }
  })
}

function MatrixTable({
  rows,
  visibleColumns,
  previewCount,
}: {
  rows: MatrixRow[]
  visibleColumns: MatrixColumn[]
  previewCount?: number
}) {
  const displayedRows = typeof previewCount === 'number' ? rows.slice(0, previewCount) : rows

  return (
    <div
      className="overflow-hidden rounded-[24px] border"
      style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 255, 255, 0.58)' }}
    >
      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse text-left">
          <thead>
            <tr className="border-b border-[#EAE5DC]">
              <th className="px-6 py-4 text-xs font-semibold text-[#6B7280]">Payer</th>
              {visibleColumns.map(column => (
                <th key={column} className="px-4 py-4 text-xs font-semibold text-[#6B7280]">
                  {columnLabels[column]}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayedRows.map(row => (
              <tr key={row.payer} className="border-b border-[#F0ECE4] last:border-b-0">
                <td className="px-6 py-4 text-sm font-medium text-[#27404A]">{row.payer}</td>
                {visibleColumns.map(column => {
                  const value = row[column]

                  if (column === 'coverage') {
                    const style = coverageStyles[value] ?? coverageStyles.Covered
                    return (
                      <td key={column} className="px-4 py-4 text-sm text-[#27404A]">
                        <span
                          className="inline-flex rounded-full px-3 py-1 text-xs font-semibold"
                          style={{ background: style.bg, color: style.text }}
                        >
                          {value}
                        </span>
                      </td>
                    )
                  }

                  return (
                    <td key={column} className="max-w-[220px] px-4 py-4 text-sm text-[#36515C]">
                      <span className={column === 'restriction' ? 'line-clamp-2' : undefined}>{value}</span>
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export function OverviewMatrixPanel({ portfolio }: OverviewMatrixPanelProps) {
  const [selectedDrugId, setSelectedDrugId] = useState(portfolio[1]?.id ?? portfolio[0]?.id ?? '')
  const selectedDrug = portfolio.find(entry => entry.id === selectedDrugId) ?? portfolio[0]
  const indicationOptions = useMemo(
    () => Array.from(new Set(selectedDrug.policies.flatMap(policy => policy.indications.map(indication => indication.name)))),
    [selectedDrug]
  )
  const [selectedIndication, setSelectedIndication] = useState(indicationOptions[0] ?? '')
  const [visibleColumns, setVisibleColumns] = useState<MatrixColumn[]>(DEFAULT_COLUMNS)
  const matrixRows = useMemo(
    () => buildMatrixRows(selectedDrug, selectedIndication || indicationOptions[0] || ''),
    [selectedDrug, selectedIndication, indicationOptions]
  )

  const activeIndication = selectedIndication || indicationOptions[0] || ''

  function toggleColumn(column: MatrixColumn) {
    setVisibleColumns(current =>
      current.includes(column)
        ? current.filter(item => item !== column)
        : [...current, column]
    )
  }

  return (
    <div
      className="space-y-4 rounded-[28px] border p-5 shadow-[0_18px_48px_rgba(18,52,51,0.1)] backdrop-blur-[18px]"
      style={{ background: 'rgba(255, 252, 245, 0.82)', borderColor: 'rgba(53, 76, 72, 0.14)' }}
    >
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-[#7A8B85]">Cross-Payer Matrix</p>
          <h3 className="mt-2 text-[30px] font-semibold tracking-tight text-[#14343A]">
            {selectedDrug.brandName} coverage snapshot
          </h3>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-[#6B7D80]">
            Choose the drug, narrow to an indication, and decide which matrix columns matter before expanding to the full view.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          <span className="inline-flex rounded-full bg-[#E9E9E1] px-3 py-1.5 text-xs font-semibold text-[#36515C]">
            Drug: {selectedDrug.brandName}
          </span>
          <span className="inline-flex rounded-full bg-[#E9E9E1] px-3 py-1.5 text-xs font-semibold text-[#36515C]">
            Indication: {activeIndication}
          </span>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2">
        <label className="space-y-2">
          <span className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#8B9692]">Drug</span>
          <select
            value={selectedDrugId}
            onChange={event => {
              const nextDrug = portfolio.find(entry => entry.id === event.target.value)
              const nextIndications = Array.from(new Set(nextDrug?.policies.flatMap(policy => policy.indications.map(indication => indication.name)) ?? []))
              setSelectedDrugId(event.target.value)
              setSelectedIndication(nextIndications[0] ?? '')
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
          <span className="text-[11px] font-semibold uppercase tracking-[0.16em] text-[#8B9692]">Indication</span>
          <select
            value={activeIndication}
            onChange={event => setSelectedIndication(event.target.value)}
            className="w-full rounded-2xl border px-4 py-3 text-sm text-[#27404A] outline-none"
            style={{ borderColor: 'rgba(53, 76, 72, 0.14)', background: 'rgba(255, 255, 255, 0.7)' }}
          >
            {indicationOptions.map(indication => (
              <option key={indication} value={indication}>{indication}</option>
            ))}
          </select>
        </label>
      </div>

      <div className="flex flex-wrap gap-2">
        {DEFAULT_COLUMNS.map(column => {
          const active = visibleColumns.includes(column)

          return (
            <button
              key={column}
              type="button"
              onClick={() => toggleColumn(column)}
              className="rounded-full px-3 py-1.5 text-xs font-semibold transition-colors"
              style={{
                background: active ? '#14343A' : '#ECE8DF',
                color: active ? '#FFFFFF' : '#5E706D',
              }}
            >
              {columnLabels[column]}
            </button>
          )
        })}
      </div>

      <MatrixTable rows={matrixRows} visibleColumns={visibleColumns} previewCount={4} />

      <div className="flex flex-col gap-3 border-t border-[#EAE5DC] pt-4 sm:flex-row sm:items-center sm:justify-between">
        <p className="text-sm text-[#6B7D80]">
          Showing the first {Math.min(4, matrixRows.length)} payer rows. Expand when you want the full comparison surface.
        </p>

        <Dialog>
          <DialogTrigger asChild>
            <button
              type="button"
              className="inline-flex items-center justify-center rounded-full bg-[#14343A] px-4 py-2.5 text-sm font-semibold text-white"
            >
              View full matrix
            </button>
          </DialogTrigger>
          <DialogContent className="max-w-[1100px] rounded-[28px] border border-[#E5E2DC] bg-[#FCFBF8] p-0 text-[#14343A]">
            <div className="border-b border-[#ECE7DE] px-7 py-6">
              <DialogHeader className="gap-3">
                <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-[#7A8B85]">Cross-Payer Matrix</p>
                <DialogTitle className="text-2xl font-semibold tracking-tight text-[#14343A]">
                  Full {selectedDrug.brandName} matrix for {activeIndication}
                </DialogTitle>
                <DialogDescription className="text-sm leading-6 text-[#6B7D80]">
                  The expanded view keeps your current drug, indication, and visible-column choices intact.
                </DialogDescription>
              </DialogHeader>
            </div>
            <div className="max-h-[70vh] overflow-y-auto px-7 py-6">
              <MatrixTable rows={matrixRows} visibleColumns={visibleColumns} />
            </div>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  )
}
