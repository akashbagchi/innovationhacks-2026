export type CriterionType =
  | 'diagnosis'
  | 'disease_severity'
  | 'step_therapy'
  | 'lab_value'
  | 'prescriber'
  | 'combination_restriction'
  | 'age_weight'
  | 'clinical_response'
  | 'prior_therapy'
  | 'line_of_therapy'
  | 'other'

export interface Criterion {
  criterion_type: CriterionType
  description: string
  logic_operator: 'AND' | 'OR'
}

export interface AuthBlock {
  criteria: Criterion[]
  authorization_duration_months?: number | null
  required_prescriber_specialties?: string[]
}

export interface Indication {
  name: string
  description?: string
  icd10_codes: string[]
  pa_required: boolean
  step_therapy_required?: boolean
  initial_authorization: AuthBlock
  reauthorization?: AuthBlock | null
}

export interface Exclusion {
  description: string
}

export interface ConfidenceScores {
  overall: number
  drug_identification?: number
  pa_criteria_completeness?: number
  review_flags?: string
}

export interface PolicyRecord {
  payer: {
    name: string
    policy_id?: string | null
    policy_title: string
    effective_date?: string
    revision_date?: string
  }
  drug: {
    brand_name: string
    generic_name: string
    j_codes?: string[]
    hcpcs_codes?: string[]
    drug_class?: string
    route_of_administration?: string
    benefit_type?: string
    limitations_of_use?: string
  }
  indications: Indication[]
  exclusions?: Exclusion[]
  confidence_scores: ConfidenceScores
}

export interface ChangeEntry {
  severity: 'HIGH' | 'MED' | 'LOW'
  payer: string
  drug: string
  change_type: string
  summary: string
  date: string
}

export interface InsightCard {
  severity: 'high' | 'medium' | 'low'
  text: string
  action: string
}

export interface TrendPoint {
  quarter: string
  score: number
}

export interface PayerTrend {
  payerName: string
  history: TrendPoint[]
  delta: number          // positive = tightening, negative = loosening
  direction: 'tightening' | 'loosening' | 'stable'
}
