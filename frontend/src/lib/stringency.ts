import type { PolicyRecord } from '../types/policy'

export interface StringencyResult {
  score: number
  breakdown: {
    pa: number
    stepTherapy: number
    criteriaVolume: number
    exclusions: number
  }
}

export function computeStringency(record: PolicyRecord): StringencyResult {
  const { indications, exclusions } = record
  if (!indications.length) return { score: 0, breakdown: { pa: 0, stepTherapy: 0, criteriaVolume: 0, exclusions: 0 } }

  const paRatio = indications.filter(i => i.pa_required).length / indications.length
  const stepRatio = indications.filter(i => i.step_therapy_required).length / indications.length
  const avgCriteria = indications.reduce((sum, i) => sum + i.initial_authorization.criteria.length, 0) / indications.length

  const pa = Math.round(paRatio * 35)
  const stepTherapy = Math.round(stepRatio * 25)
  const criteriaVolume = Math.min(Math.round(avgCriteria * 8), 25)
  const exclusionScore = Math.min((exclusions?.length ?? 0) * 5, 15)

  return {
    score: pa + stepTherapy + criteriaVolume + exclusionScore,
    breakdown: { pa, stepTherapy, criteriaVolume, exclusions: exclusionScore },
  }
}

export function getStringencyColor(score: number): string {
  if (score >= 70) return 'bg-red-500'
  if (score >= 40) return 'bg-yellow-500'
  return 'bg-green-500'
}

export function getStringencyLabel(score: number): string {
  if (score >= 70) return 'High'
  if (score >= 40) return 'Medium'
  return 'Low'
}
