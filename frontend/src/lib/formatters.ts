// Shared formatting utilities — single source of truth for labels, colors, abbreviations

export function canonicalizePayerName(name: string): string {
  const normalized = name.trim()
  const aliases: Record<string, string> = {
    UnitedHealth: 'UnitedHealthcare',
    UHC: 'UnitedHealthcare',
    'Blue Cross Blue Shield of North Carolina (BCBS NC)': 'Blue Cross NC',
  }
  return aliases[normalized] ?? normalized
}

export function formatPayerName(name: string): string {
  const canonicalName = canonicalizePayerName(name)
  const abbrevs: Record<string, string> = {
    'Blue Cross NC': 'BCNC',
    'UnitedHealthcare': 'UHC',
    'Florida Blue': 'FL Blue',
    'Cigna': 'Cigna',
    'Aetna': 'Aetna',
    'Priority Health': 'PH',
    'EmblemHealth': 'Emblem',
    'Humana': 'Humana',
    'Molina Healthcare': 'Molina',
    'Anthem': 'Anthem',
    'Centene': 'Centene',
  }

  if (abbrevs[canonicalName]) return abbrevs[canonicalName]

  const n = canonicalName.toLowerCase()
  if (n.includes('unitedhealth'))                      return 'UHC'
  if (n.includes('blue cross') && n.includes('nc'))   return 'BCNC'
  if (n.includes('blue cross') && n.includes('north')) return 'BCNC'
  if (n.includes('florida blue'))                     return 'FL Blue'
  if (n.includes('blue shield') && n.includes('ca'))  return 'BCBS CA'
  if (n.includes('blue cross') || n.includes('bcbs')) return 'BCBS'
  if (n.includes('cigna'))                            return 'Cigna'
  if (n.includes('aetna'))                            return 'Aetna'
  if (n.includes('priority health'))                  return 'PH'
  if (n.includes('emblem'))                           return 'Emblem'
  if (n.includes('humana'))                           return 'Humana'
  if (n.includes('molina'))                           return 'Molina'
  if (n.includes('anthem'))                           return 'Anthem'
  if (n.includes('centene'))                          return 'Centene'
  return canonicalName.split(/\s+/)[0].slice(0, 8)
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

export const CRITERION_CHIP_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  step_therapy:            { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  combination_restriction: { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  prior_therapy:           { bg: '#F8EDDC', text: '#8B6428', border: 'rgba(139,100,40,0.2)' },
  line_of_therapy:         { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  disease_severity:        { bg: '#FBEAEA', text: '#B81C1C', border: 'rgba(184,28,28,0.2)' },
  lab_value:               { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  diagnosis:               { bg: '#F0EFEB', text: '#4A4845', border: '#D8D4CC' },
  prescriber:              { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  clinical_response:       { bg: '#EAF4FE', text: '#2870A8', border: 'rgba(40,112,168,0.25)' },
  other:                   { bg: '#F0EFEB', text: '#918D88', border: '#D8D4CC' },
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
