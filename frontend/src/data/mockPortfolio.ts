import type { PolicyRecord, InsightCard, PayerTrend } from '../types/policy'

// Full Avastin record reused from mockPolicies
import { mockPolicies as avastinPolicies, mockInsights as avastinInsights } from './mockPolicies'

export { avastinPolicies, avastinInsights }

// ─── Dupixent (dupilumab) ────────────────────────────────────────────────────
export const dupixentPolicies: PolicyRecord[] = [
  {
    payer: { name: 'Blue Cross NC', policy_id: 'CP.PMN.112', policy_title: 'Clinical Policy Bulletin: Dupilumab (Dupixent)', effective_date: '2025-01-01', revision_date: '2024-11-15' },
    drug: { brand_name: 'Dupixent', generic_name: 'dupilumab', j_codes: ['J0222'], hcpcs_codes: ['J0222'], drug_class: 'IL-4/IL-13 receptor antagonist', route_of_administration: 'subcutaneous injection', benefit_type: 'medical' },
    indications: [
      { name: 'Atopic dermatitis', icd10_codes: ['L20.0'], pa_required: true, step_therapy_required: true, initial_authorization: { authorization_duration_months: 6, criteria: [{ criterion_type: 'diagnosis', description: 'Confirmed moderate-to-severe atopic dermatitis', logic_operator: 'AND' }, { criterion_type: 'step_therapy', description: 'Inadequate response to ≥2 topical therapies for ≥12 weeks', logic_operator: 'AND' }, { criterion_type: 'disease_severity', description: 'BSA ≥10% or EASI score ≥16', logic_operator: 'AND' }] } },
      { name: 'Asthma', icd10_codes: ['J45.50'], pa_required: true, step_therapy_required: false, initial_authorization: { authorization_duration_months: 12, criteria: [{ criterion_type: 'diagnosis', description: 'Moderate-to-severe eosinophilic or OCS-dependent asthma', logic_operator: 'AND' }, { criterion_type: 'lab_value', description: 'Blood eosinophils ≥300 cells/μL', logic_operator: 'AND' }] } },
      { name: 'CRS with nasal polyps', icd10_codes: ['J33.0'], pa_required: true, step_therapy_required: true, initial_authorization: { criteria: [{ criterion_type: 'prior_therapy', description: 'Inadequate response to intranasal corticosteroids', logic_operator: 'AND' }] } },
    ],
    exclusions: [{ description: 'Not covered for mild atopic dermatitis' }],
    confidence_scores: { overall: 0.96 },
  },
  {
    payer: { name: 'UnitedHealth', policy_id: 'CS-DERM-0018', policy_title: 'UnitedHealthcare: Dupilumab (Dupixent)', effective_date: '2025-10-01', revision_date: '2025-09-01' },
    drug: { brand_name: 'Dupixent', generic_name: 'dupilumab', j_codes: ['J0222'], hcpcs_codes: ['J0222'], drug_class: 'IL-4/IL-13 receptor antagonist', route_of_administration: 'subcutaneous injection', benefit_type: 'medical' },
    indications: [
      { name: 'Atopic dermatitis', icd10_codes: ['L20.0'], pa_required: true, step_therapy_required: true, initial_authorization: { authorization_duration_months: 6, criteria: [{ criterion_type: 'step_therapy', description: 'Failure of ≥2 conventional systemic therapies (methotrexate or cyclosporine) for ≥12 weeks', logic_operator: 'AND' }, { criterion_type: 'prescriber', description: 'Must be prescribed by dermatologist or allergist', logic_operator: 'AND' }, { criterion_type: 'disease_severity', description: 'EASI score ≥16 documented', logic_operator: 'AND' }, { criterion_type: 'lab_value', description: 'eGFR >30 mL/min/1.73m² at baseline', logic_operator: 'AND' }] } },
      { name: 'Asthma', icd10_codes: ['J45.50'], pa_required: true, step_therapy_required: true, initial_authorization: { authorization_duration_months: 6, criteria: [{ criterion_type: 'prior_therapy', description: 'Must fail ≥2 ICS/LABA combinations', logic_operator: 'AND' }, { criterion_type: 'lab_value', description: 'Eosinophils ≥300 or FeNO ≥25 ppb', logic_operator: 'AND' }] } },
    ],
    exclusions: [{ description: 'Not covered concurrently with other biologics' }, { description: 'Not covered for mild disease' }],
    confidence_scores: { overall: 0.91 },
  },
  {
    payer: { name: 'Cigna', policy_id: 'CPO-DUP-2025', policy_title: 'Cigna Coverage Policy: Dupilumab', effective_date: '2025-07-01', revision_date: '2025-06-15' },
    drug: { brand_name: 'Dupixent', generic_name: 'dupilumab', j_codes: ['J0222'], hcpcs_codes: ['J0222'], drug_class: 'IL-4/IL-13 receptor antagonist', route_of_administration: 'subcutaneous injection', benefit_type: 'medical' },
    indications: [
      { name: 'Atopic dermatitis', icd10_codes: ['L20.0'], pa_required: true, step_therapy_required: false, initial_authorization: { authorization_duration_months: 12, criteria: [{ criterion_type: 'diagnosis', description: 'Moderate-to-severe atopic dermatitis', logic_operator: 'AND' }, { criterion_type: 'prior_therapy', description: 'Inadequate response to topical corticosteroids', logic_operator: 'AND' }] } },
      { name: 'Asthma', icd10_codes: ['J45.50'], pa_required: true, step_therapy_required: false, initial_authorization: { authorization_duration_months: 12, criteria: [{ criterion_type: 'diagnosis', description: 'Uncontrolled moderate-to-severe asthma', logic_operator: 'AND' }] } },
      { name: 'CRS with nasal polyps', icd10_codes: ['J33.0'], pa_required: false, step_therapy_required: false, initial_authorization: { criteria: [] } },
    ],
    exclusions: [],
    confidence_scores: { overall: 0.98 },
  },
]

export const dupixentInsights: InsightCard[] = [
  {
    severity: 'high',
    text: 'UnitedHealth requires 4 criteria for atopic dermatitis — the highest barrier across all payers, including an eGFR lab value requirement not seen elsewhere.',
    action: 'Brief dermatology account managers: UHC patients need pre-authorization lab documentation (eGFR) before submission. Build this into hub intake forms to prevent delays.',
  },
  {
    severity: 'medium',
    text: 'Cigna waives PA for CRS with nasal polyps, making it the only payer with unrestricted access for that indication.',
    action: 'Flag Cigna CRS-NP as a high-conversion opportunity for ENT accounts — zero PA friction. Prioritize Cigna-covered patients in pull-through programs.',
  },
  {
    severity: 'low',
    text: 'Authorization duration varies 2×: UnitedHealth grants 6-month auth vs Cigna\'s 12-month, doubling reauthorization burden for UHC patients.',
    action: 'Update hub reauthorization scheduling: UHC patients require twice-annual renewals. Proactive outreach at month 5 reduces lapse risk.',
  },
]

// ─── Keytruda (pembrolizumab) ────────────────────────────────────────────────
export const keytrudaPolicies: PolicyRecord[] = [
  {
    payer: { name: 'Blue Cross NC', policy_id: 'CPO-PEM-2025', policy_title: 'Corporate Medical Policy: Pembrolizumab (Keytruda)', effective_date: '2025-04-01', revision_date: '2025-03-15' },
    drug: { brand_name: 'Keytruda', generic_name: 'pembrolizumab', j_codes: ['J9271'], hcpcs_codes: ['J9271'], drug_class: 'PD-1 checkpoint inhibitor', route_of_administration: 'intravenous (IV) infusion', benefit_type: 'medical' },
    indications: [
      { name: 'Melanoma', icd10_codes: ['C43.9'], pa_required: true, step_therapy_required: false, initial_authorization: { criteria: [{ criterion_type: 'diagnosis', description: 'Unresectable or metastatic melanoma', logic_operator: 'AND' }] } },
      { name: 'NSCLC', icd10_codes: ['C34.10'], pa_required: true, step_therapy_required: false, initial_authorization: { authorization_duration_months: 6, criteria: [{ criterion_type: 'lab_value', description: 'PD-L1 TPS ≥1%', logic_operator: 'AND' }, { criterion_type: 'line_of_therapy', description: 'First-line for high expressors (TPS ≥50%) or combination for TPS 1–49%', logic_operator: 'AND' }] } },
      { name: 'HNSCC', icd10_codes: ['C10.9'], pa_required: true, step_therapy_required: true, initial_authorization: { criteria: [{ criterion_type: 'prior_therapy', description: 'Recurrent or metastatic, progression on platinum-based chemotherapy', logic_operator: 'AND' }] } },
    ],
    exclusions: [],
    confidence_scores: { overall: 0.93 },
  },
  {
    payer: { name: 'UnitedHealth', policy_id: 'CS-ONCO-0091', policy_title: 'UHC Medical Policy: Pembrolizumab (Keytruda)', effective_date: '2025-10-01', revision_date: '2025-09-01' },
    drug: { brand_name: 'Keytruda', generic_name: 'pembrolizumab', j_codes: ['J9271'], hcpcs_codes: ['J9271'], drug_class: 'PD-1 checkpoint inhibitor', route_of_administration: 'intravenous (IV) infusion', benefit_type: 'medical' },
    indications: [
      { name: 'Melanoma', icd10_codes: ['C43.9'], pa_required: true, step_therapy_required: true, initial_authorization: { authorization_duration_months: 6, criteria: [{ criterion_type: 'diagnosis', description: 'Unresectable stage III/IV melanoma', logic_operator: 'AND' }, { criterion_type: 'prior_therapy', description: 'Must have failed BRAF inhibitor if BRAF V600 mutant', logic_operator: 'AND' }] } },
      { name: 'NSCLC', icd10_codes: ['C34.10'], pa_required: true, step_therapy_required: false, initial_authorization: { authorization_duration_months: 6, criteria: [{ criterion_type: 'lab_value', description: 'PD-L1 TPS ≥50% for monotherapy; ≥1% for combination', logic_operator: 'AND' }, { criterion_type: 'prescriber', description: 'Prescribed by board-certified oncologist', logic_operator: 'AND' }] } },
    ],
    exclusions: [{ description: 'Not covered for MSI-H/dMMR tumors without prior chemotherapy in settings where chemotherapy is appropriate' }],
    confidence_scores: { overall: 0.89 },
  },
  {
    payer: { name: 'Cigna', policy_id: 'CPO-KEY-2025', policy_title: 'Cigna Coverage Policy: Pembrolizumab (Keytruda)', effective_date: '2025-01-01', revision_date: '2024-12-01' },
    drug: { brand_name: 'Keytruda', generic_name: 'pembrolizumab', j_codes: ['J9271'], hcpcs_codes: ['J9271'], drug_class: 'PD-1 checkpoint inhibitor', route_of_administration: 'intravenous (IV) infusion', benefit_type: 'medical' },
    indications: [
      { name: 'Melanoma', icd10_codes: ['C43.9'], pa_required: true, step_therapy_required: false, initial_authorization: { authorization_duration_months: 12, criteria: [{ criterion_type: 'diagnosis', description: 'Unresectable or metastatic melanoma', logic_operator: 'AND' }] } },
      { name: 'NSCLC', icd10_codes: ['C34.10'], pa_required: true, step_therapy_required: false, initial_authorization: { authorization_duration_months: 12, criteria: [{ criterion_type: 'lab_value', description: 'PD-L1 expression confirmed by approved assay', logic_operator: 'AND' }] } },
      { name: 'HNSCC', icd10_codes: ['C10.9'], pa_required: true, step_therapy_required: false, initial_authorization: { criteria: [{ criterion_type: 'diagnosis', description: 'Recurrent or metastatic HNSCC', logic_operator: 'AND' }] } },
      { name: 'TMB-H solid tumors', icd10_codes: [], pa_required: false, step_therapy_required: false, initial_authorization: { criteria: [] } },
    ],
    exclusions: [],
    confidence_scores: { overall: 0.97 },
  },
]

export const keytrudaInsights: InsightCard[] = [
  {
    severity: 'high',
    text: 'UnitedHealth requires BRAF inhibitor failure before pembrolizumab for BRAF-mutant melanoma — a sequencing requirement not present at other payers.',
    action: 'Flag for medical affairs: UHC sequencing policy conflicts with NCCN guidelines that permit pembrolizumab as first-line for BRAF-mutant melanoma. Escalate to payer medical director for policy review.',
  },
  {
    severity: 'medium',
    text: 'Cigna covers TMB-H solid tumors without PA, the broadest coverage position across all three payers for biomarker-selected patients.',
    action: 'Prioritize Cigna for TMB-H patient identification programs. No PA friction enables fastest time-to-treatment. Align molecular testing partners to route Cigna-insured patients first.',
  },
  {
    severity: 'low',
    text: 'Authorization duration ranges from 6 months (UHC) to 12 months (Cigna/BCNC), with direct impact on administrative burden.',
    action: 'Quantify annualized reauth cost for UHC accounts — 2× renewal frequency directly affects oncology practice administrative overhead. Use in value story with practice managers.',
  },
]

// ─── Portfolio catalog ────────────────────────────────────────────────────────
export interface DrugPortfolioEntry {
  id: string
  brandName: string
  genericName: string
  drugClass: string
  jCode: string
  policies: PolicyRecord[]
  insights: InsightCard[]
  trends: PayerTrend[]
  livesAtRisk: string
  lastUpdated: string
  changeCount: number
}

export const portfolio: DrugPortfolioEntry[] = [
  {
    id: 'bevacizumab',
    brandName: 'Avastin',
    genericName: 'bevacizumab',
    drugClass: 'VEGF inhibitor',
    jCode: 'J9035',
    policies: avastinPolicies,
    insights: avastinInsights,
    trends: [
      {
        payerName: 'Blue Cross NC',
        history: [
          { quarter: 'Q2 2025', score: 65 },
          { quarter: 'Q3 2025', score: 66 },
          { quarter: 'Q4 2025', score: 67 },
          { quarter: 'Q1 2026', score: 68 },
        ],
        delta: 3,
        direction: 'tightening',
      },
      {
        payerName: 'UnitedHealth',
        history: [
          { quarter: 'Q2 2025', score: 70 },
          { quarter: 'Q3 2025', score: 74 },
          { quarter: 'Q4 2025', score: 78 },
          { quarter: 'Q1 2026', score: 82 },
        ],
        delta: 12,
        direction: 'tightening',
      },
      {
        payerName: 'Cigna',
        history: [
          { quarter: 'Q2 2025', score: 55 },
          { quarter: 'Q3 2025', score: 51 },
          { quarter: 'Q4 2025', score: 46 },
          { quarter: 'Q1 2026', score: 41 },
        ],
        delta: -14,
        direction: 'loosening',
      },
    ],
    livesAtRisk: '~3.2M commercially insured',
    lastUpdated: '2026-01-01',
    changeCount: 4,
  },
  {
    id: 'dupilumab',
    brandName: 'Dupixent',
    genericName: 'dupilumab',
    drugClass: 'IL-4/IL-13 antagonist',
    jCode: 'J0222',
    policies: dupixentPolicies,
    insights: dupixentInsights,
    trends: [
      {
        payerName: 'Blue Cross NC',
        history: [
          { quarter: 'Q2 2025', score: 70 },
          { quarter: 'Q3 2025', score: 71 },
          { quarter: 'Q4 2025', score: 73 },
          { quarter: 'Q1 2026', score: 75 },
        ],
        delta: 5,
        direction: 'tightening',
      },
      {
        payerName: 'UnitedHealth',
        history: [
          { quarter: 'Q2 2025', score: 83 },
          { quarter: 'Q3 2025', score: 85 },
          { quarter: 'Q4 2025', score: 86 },
          { quarter: 'Q1 2026', score: 88 },
        ],
        delta: 5,
        direction: 'tightening',
      },
      {
        payerName: 'Cigna',
        history: [
          { quarter: 'Q2 2025', score: 58 },
          { quarter: 'Q3 2025', score: 57 },
          { quarter: 'Q4 2025', score: 56 },
          { quarter: 'Q1 2026', score: 55 },
        ],
        delta: -3,
        direction: 'stable',
      },
    ],
    livesAtRisk: '~8.1M commercially insured',
    lastUpdated: '2025-10-01',
    changeCount: 2,
  },
  {
    id: 'pembrolizumab',
    brandName: 'Keytruda',
    genericName: 'pembrolizumab',
    drugClass: 'PD-1 inhibitor',
    jCode: 'J9271',
    policies: keytrudaPolicies,
    insights: keytrudaInsights,
    trends: [
      {
        payerName: 'Blue Cross NC',
        history: [
          { quarter: 'Q2 2025', score: 58 },
          { quarter: 'Q3 2025', score: 59 },
          { quarter: 'Q4 2025', score: 61 },
          { quarter: 'Q1 2026', score: 63 },
        ],
        delta: 5,
        direction: 'tightening',
      },
      {
        payerName: 'UnitedHealth',
        history: [
          { quarter: 'Q2 2025', score: 72 },
          { quarter: 'Q3 2025', score: 73 },
          { quarter: 'Q4 2025', score: 74 },
          { quarter: 'Q1 2026', score: 74 },
        ],
        delta: 2,
        direction: 'stable',
      },
      {
        payerName: 'Cigna',
        history: [
          { quarter: 'Q2 2025', score: 50 },
          { quarter: 'Q3 2025', score: 48 },
          { quarter: 'Q4 2025', score: 46 },
          { quarter: 'Q1 2026', score: 44 },
        ],
        delta: -6,
        direction: 'loosening',
      },
    ],
    livesAtRisk: '~1.4M commercially insured',
    lastUpdated: '2025-10-01',
    changeCount: 3,
  },
]
