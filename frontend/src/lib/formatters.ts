// Shared formatting utilities — single source of truth for labels, colors, abbreviations

export function formatPayerName(name: string): string {
  const abbrevs: Record<string, string> = {
    'Blue Cross NC': 'BCNC',
    'UnitedHealth': 'UHC',
    'UnitedHealthcare': 'UHC',
    'Florida Blue': 'FL Blue',
    'Cigna': 'Cigna',
    'Aetna': 'Aetna',
    'Priority Health': 'PH',
  }
  return abbrevs[name] ?? name
}

export const CRITERION_LABELS: Record<string, string> = {
  diagnosis:              'Diagnosis',
  disease_severity:       'Disease Severity',
  step_therapy:           'Step Therapy',
  lab_value:              'Lab Value',
  prescriber:             'Prescriber Restriction',
  combination_restriction:'Combination Restriction',
  age_weight:             'Age / Weight',
  clinical_response:      'Clinical Response',
  prior_therapy:          'Prior Therapy',
  line_of_therapy:        'Line of Therapy',
  other:                  'Other',
}

export const CRITERION_COLORS: Record<string, string> = {
  diagnosis:              '#4A90D9',
  disease_severity:       '#E07C30',
  step_therapy:           '#B81C1C',
  lab_value:              '#7B5EA7',
  prescriber:             '#2E7D52',
  combination_restriction:'#C0842A',
  age_weight:             '#4A7FA5',
  clinical_response:      '#1A7840',
  prior_therapy:          '#8B6428',
  line_of_therapy:        '#6B7280',
  other:                  '#918D88',
}

export const CHANGE_TYPE_LABELS: Record<string, string> = {
  ADDED_STEP_THERAPY:   'Step therapy added',
  ADDED_CRITERION:      'Criterion added',
  REMOVED_CRITERION:    'Criterion removed',
  MODIFIED_THRESHOLD:   'Threshold shifted',
  MODIFIED_WORDING:     'Language updated',
  MODIFIED_PA_REQUIRED: 'PA status changed',
  REMOVED_INDICATION:   'Indication removed',
  ADDED_INDICATION:     'Indication added',
}
