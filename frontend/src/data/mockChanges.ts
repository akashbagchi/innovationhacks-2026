import type { ChangeEntry } from '../types/policy'

export const mockChanges: ChangeEntry[] = [
  {
    severity: 'HIGH',
    payer: 'UnitedHealth',
    drug: 'Avastin (bevacizumab)',
    change_type: 'ADDED_STEP_THERAPY',
    summary: 'Step therapy added for metastatic colorectal cancer — must document failure of prior fluorouracil-based regimen for second-line use',
    date: '2025-10-01',
  },
  {
    severity: 'HIGH',
    payer: 'UnitedHealth',
    drug: 'Avastin (bevacizumab)',
    change_type: 'ADDED_CRITERION',
    summary: 'Lab value criterion added for HCC: Child-Pugh score A now required before authorization',
    date: '2025-10-01',
  },
  {
    severity: 'MED',
    payer: 'Cigna',
    drug: 'Avastin (bevacizumab)',
    change_type: 'MODIFIED_THRESHOLD',
    summary: 'Authorization duration reduced for colorectal cancer: 18 months → 12 months',
    date: '2025-07-01',
  },
  {
    severity: 'LOW',
    payer: 'Blue Cross NC',
    drug: 'Avastin (bevacizumab)',
    change_type: 'MODIFIED_WORDING',
    summary: 'Effective date updated: 2025-01-01 → 2026-01-01. No clinical criteria changes.',
    date: '2026-01-01',
  },
]
