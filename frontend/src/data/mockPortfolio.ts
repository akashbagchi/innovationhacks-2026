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
    // Source: UHC policy 2025 P 2116-22 (extracted from test-doc.json, confidence 0.95)
    payer: { name: 'UnitedHealth', policy_id: '2025 P 2116-22', policy_title: 'Dupixent (dupilumab) Prior Authorization/Medical Necessity', effective_date: '2025-11-01', revision_date: '2025-08-01' },
    drug: { brand_name: 'Dupixent', generic_name: 'dupilumab', j_codes: ['J0222'], hcpcs_codes: ['J0222'], drug_class: 'Interleukin-4 receptor alpha antagonist', route_of_administration: 'subcutaneous injection', benefit_type: 'pharmacy', limitations_of_use: 'Not for the relief of acute bronchospasm or status asthmaticus.' },
    indications: [
      {
        name: 'Atopic Dermatitis',
        icd10_codes: ['L20.0'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'diagnosis', description: 'Diagnosis of moderate-to-severe chronic atopic dermatitis', logic_operator: 'AND' },
            { criterion_type: 'step_therapy', description: 'History of failure, contraindication, or intolerance to two of the following topical therapy classes: (a) Medium/high/very-high potency topical corticosteroid; (b) Topical calcineurin inhibitor [Elidel, Protopic]; (c) Eucrisa (crisaborole)', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Not receiving Dupixent in combination with a biologic immunomodulator or Janus kinase inhibitor for the same indication', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by a dermatologist, allergist, or immunologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['Dermatologist', 'Allergist', 'Immunologist'],
        },
        reauthorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'clinical_response', description: 'Documentation of positive clinical response to Dupixent therapy', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Not receiving Dupixent in combination with a biologic immunomodulator or Janus kinase inhibitor', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by a dermatologist, allergist, or immunologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['Dermatologist', 'Allergist', 'Immunologist'],
        },
      },
      {
        name: 'Asthma',
        icd10_codes: ['J45.50', 'J45.51'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'diagnosis', description: 'Diagnosis of moderate-to-severe asthma', logic_operator: 'AND' },
            { criterion_type: 'disease_severity', description: 'Asthma classified as uncontrolled or inadequately controlled: poor symptom control (ACQ >1.5 or ACT <20), ≥2 corticosteroid bursts in prior 12 months, emergency treatment, FEV1 <80% predicted, or OCS-dependent', logic_operator: 'AND' },
            { criterion_type: 'lab_value', description: 'Baseline peripheral blood eosinophil level ≥150 cells/μL OR patient is currently dependent on oral corticosteroids for asthma', logic_operator: 'AND' },
            { criterion_type: 'step_therapy', description: 'Dupixent used in combination with a maximally dosed ICS/LABA (e.g., Advair, Symbicort, Breo) or combination ICS + additional controller medication', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Not receiving Dupixent concurrently with anti-IL-5, anti-IgE, or TSLP inhibitor therapy for the same indication', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by an allergist, immunologist, or pulmonologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['Allergist', 'Immunologist', 'Pulmonologist'],
        },
        reauthorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'clinical_response', description: 'Documentation of positive clinical response: reduction in exacerbations, decreased rescue medication use, improvement in FEV1, reduction in OCS requirements, or symptom improvement', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['Allergist', 'Immunologist', 'Pulmonologist'],
        },
      },
      {
        name: 'Chronic Rhinosinusitis with Nasal Polyposis (CRSwNP)',
        icd10_codes: ['J33.0'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'diagnosis', description: 'CRSwNP: ≥2 symptoms for >12 weeks (nasal discharge, obstruction, facial pain, anosmia) confirmed by endoscopy or sinus CT; bilateral nasal polyposis or prior surgical removal; prior sinus surgery, systemic corticosteroids in last 2 years, or failure of 2 topical agent classes', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Dupixent used as add-on maintenance with intranasal corticosteroids; not concurrent with anti-IL-5, anti-IgE, or TSLP inhibitor', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by allergist, immunologist, otolaryngologist, or pulmonologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['Allergist', 'Immunologist', 'Otolaryngologist', 'Pulmonologist'],
        },
        reauthorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'clinical_response', description: 'Positive clinical response documented', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Continuing add-on maintenance with intranasal corticosteroids; not concurrent with anti-IL-5, anti-IgE, or TSLP inhibitor', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Eosinophilic Esophagitis (EoE)',
        icd10_codes: ['K20.0'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'diagnosis', description: 'Diagnosis of eosinophilic esophagitis', logic_operator: 'AND' },
            { criterion_type: 'disease_severity', description: 'Experiencing symptoms of esophageal dysfunction: dysphagia, food impaction, chest pain, GERD-like symptoms, or upper abdominal pain', logic_operator: 'AND' },
            { criterion_type: 'lab_value', description: 'Eosinophil-predominant inflammation on esophageal biopsy: peak value ≥15 intraepithelial eosinophils per HPF (or ≥60 eosinophils per mm²)', logic_operator: 'AND' },
            { criterion_type: 'step_therapy', description: 'Symptoms persisted after 8-week trial of proton pump inhibitors and/or topical esophageal corticosteroids (budesonide, fluticasone)', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Not concurrent with anti-IL-5, anti-IgE, or TSLP inhibitor', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by a gastroenterologist or allergist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['Gastroenterologist', 'Allergist'],
        },
      },
    ],
    exclusions: [
      { description: 'Not covered concurrently with biologic immunomodulator or JAK inhibitor for the same indication' },
      { description: 'Not covered for mild disease' },
    ],
    confidence_scores: { overall: 0.95, drug_identification: 1.0, pa_criteria_completeness: 0.92 },
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
    text: 'UHC now covers 4 indications (including EoE — new since Nov 2025) but has the strictest step therapy: atopic dermatitis requires documented failure of 2 topical therapy classes, and asthma requires both an ICS/LABA combination AND documented eosinophilic phenotype.',
    action: 'Ensure hub intake forms capture topical therapy trial history for atopic dermatitis and eosinophil counts for asthma — both required for UHC submission. EoE patients must document PPI/topical steroid failure.',
  },
  {
    severity: 'medium',
    text: 'Cigna waives PA for CRS with nasal polyps, making it the only payer with unrestricted access for that indication. UHC requires extensive multi-part diagnostic criteria including endoscopy or CT confirmation.',
    action: 'Flag Cigna CRS-NP as a high-conversion opportunity for ENT accounts. For UHC CRSwNP submissions, require complete diagnostic workup documentation upfront — partial submissions will be denied.',
  },
  {
    severity: 'low',
    text: 'All 3 payers now grant 12-month authorization duration for atopic dermatitis. UHC previously issued 6-month auth — this has been corrected in the Nov 2025 policy update.',
    action: 'Update hub reauthorization calendars: UHC atopic dermatitis patients previously on 6-month cycles may now qualify for annual renewals under the updated policy. Review active auths.',
  },
]

// ─── Rituxan (rituximab) — Non-oncology ─────────────────────────────────────
export const rituximabPolicies: PolicyRecord[] = [
  {
    // Source: Cigna policy IP0319 (real extracted data)
    payer: { name: 'Cigna', policy_id: 'IP0319', policy_title: 'Rituximab Intravenous Products for Non-Oncology Indications', effective_date: '2025-07-01', revision_date: '2025-06-15' },
    drug: { brand_name: 'Rituxan / Truxima / Ruxience / Riabni', generic_name: 'rituximab (intravenous)', j_codes: ['J9310'], hcpcs_codes: ['J9310'], drug_class: 'Anti-CD20 monoclonal antibody', route_of_administration: 'intravenous infusion', benefit_type: 'medical' },
    indications: [
      {
        name: 'ANCA-Associated Vasculitis — Induction',
        icd10_codes: ['M31.30', 'M31.31'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 1,
          criteria: [
            { criterion_type: 'diagnosis', description: 'Patient has ANCA-associated vasculitis (granulomatosis with polyangiitis or microscopic polyangiitis)', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Administered in combination with glucocorticoids', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by or in consultation with a rheumatologist, nephrologist, pulmonologist, or immunologist', logic_operator: 'AND' },
            { criterion_type: 'other', description: 'Dosing: 375 mg/m² IV for 4 doses separated by ≥7 days; OR up to two 1,000 mg IV doses separated by ≥2 weeks', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['rheumatologist', 'nephrologist', 'pulmonologist', 'immunologist'],
        },
      },
      {
        name: 'ANCA-Associated Vasculitis — Maintenance',
        icd10_codes: ['M31.30', 'M31.31'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'clinical_response', description: 'Patient achieved disease control with induction treatment (rituximab or other standard of care immunosuppressants)', logic_operator: 'AND' },
            { criterion_type: 'other', description: 'At least 16 weeks will elapse between treatment courses', logic_operator: 'AND' },
            { criterion_type: 'other', description: 'Dosing: ≥18 yrs: up to 1,000 mg IV for 6 doses; <18 yrs: up to 250 mg/m² IV for two doses', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Pemphigus Vulgaris',
        icd10_codes: ['L10.0'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'Initiated in combination with a corticosteroid (e.g., prednisone) unless contraindicated', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by or in consultation with a dermatologist', logic_operator: 'AND' },
            { criterion_type: 'other', description: 'Dosing: up to two 1,000 mg IV doses separated by ≥2 weeks per course; subsequent courses no sooner than 16 weeks apart', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['dermatologist'],
        },
      },
      {
        name: 'Rheumatoid Arthritis — Initial Therapy',
        icd10_codes: ['M05.00', 'M05.10', 'M06.00'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 1,
          criteria: [
            { criterion_type: 'step_therapy', description: 'Must have tried one conventional synthetic DMARD for ≥3 months (e.g., methotrexate, leflunomide, hydroxychloroquine, sulfasalazine), OR have a 3-month trial of a prior biologic (no step-back required if already tried a biologic)', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Will not be used concurrently with another biologic or targeted synthetic DMARD', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by or in consultation with a rheumatologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['rheumatologist'],
        },
      },
      {
        name: 'Rheumatoid Arthritis — Continuation',
        icd10_codes: ['M05.00', 'M05.10', 'M06.00'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 1,
          criteria: [
            { criterion_type: 'other', description: '≥16 weeks will elapse between treatment courses', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Will not be used concurrently with another biologic or targeted synthetic DMARD', logic_operator: 'AND' },
          ],
        },
      },
    ],
    exclusions: [],
    confidence_scores: { overall: 0.97, drug_identification: 1.0, pa_criteria_completeness: 0.95 },
  },
  {
    payer: { name: 'UnitedHealth', policy_id: 'CS-RHEUM-0077', policy_title: 'UHC Medical Policy: Rituximab (Rituxan) Non-Oncology', effective_date: '2025-10-01', revision_date: '2025-09-01' },
    drug: { brand_name: 'Rituxan / Truxima', generic_name: 'rituximab (intravenous)', j_codes: ['J9310'], hcpcs_codes: ['J9310'], drug_class: 'Anti-CD20 monoclonal antibody', route_of_administration: 'intravenous infusion', benefit_type: 'medical' },
    indications: [
      {
        name: 'ANCA-Associated Vasculitis — Induction',
        icd10_codes: ['M31.30', 'M31.31'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'diagnosis', description: 'Confirmed ANCA-associated vasculitis (GPA or MPA) by biopsy or ANCA serology', logic_operator: 'AND' },
            { criterion_type: 'step_therapy', description: 'Must have had inadequate response or intolerance to cyclophosphamide-based induction therapy, or cyclophosphamide is contraindicated', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Used in combination with high-dose glucocorticoids', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by a board-certified rheumatologist, nephrologist, or pulmonologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['rheumatologist', 'nephrologist', 'pulmonologist'],
        },
      },
      {
        name: 'Rheumatoid Arthritis',
        icd10_codes: ['M05.00', 'M05.10', 'M06.00'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'step_therapy', description: 'Documented failure of ≥2 TNF inhibitors (e.g., adalimumab, etanercept, certolizumab) after ≥3 months each, OR documented contraindication to TNF inhibitors', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Used in combination with methotrexate; not concurrent with other biologics or JAK inhibitors', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by a board-certified rheumatologist', logic_operator: 'AND' },
            { criterion_type: 'lab_value', description: 'Positive serology: RF or anti-CCP antibody positive', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['rheumatologist'],
        },
        reauthorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'clinical_response', description: 'Documented improvement in DAS28 score or physician assessment of clinical response', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Pemphigus Vulgaris',
        icd10_codes: ['L10.0'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'step_therapy', description: 'Inadequate response or intolerance to high-dose systemic corticosteroids and at least one immunosuppressant (azathioprine, mycophenolate, or dapsone)', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by a board-certified dermatologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['dermatologist'],
        },
      },
    ],
    exclusions: [
      { description: 'Not covered for RA without documented failure of ≥2 TNF inhibitors or contraindication to TNF therapy' },
      { description: 'Not covered concurrently with other biologic DMARDs or JAK inhibitors' },
    ],
    confidence_scores: { overall: 0.88, drug_identification: 0.99, pa_criteria_completeness: 0.85 },
  },
  {
    payer: { name: 'Blue Cross NC', policy_id: 'CP.PMN.175', policy_title: 'Clinical Policy Bulletin: Rituximab (Rituxan) — Non-Oncology', effective_date: '2025-04-01', revision_date: '2025-03-15' },
    drug: { brand_name: 'Rituxan / Truxima', generic_name: 'rituximab (intravenous)', j_codes: ['J9310'], hcpcs_codes: ['J9310'], drug_class: 'Anti-CD20 monoclonal antibody', route_of_administration: 'intravenous infusion', benefit_type: 'medical' },
    indications: [
      {
        name: 'ANCA-Associated Vasculitis',
        icd10_codes: ['M31.30', 'M31.31'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'diagnosis', description: 'GPA or MPA confirmed by clinical criteria and ANCA serology', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Used with glucocorticoids for induction; cyclophosphamide failure or contraindication not required', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Rheumatoid Arthritis',
        icd10_codes: ['M05.00', 'M06.00'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'step_therapy', description: 'Failure of ≥1 TNF inhibitor after adequate trial (≥3 months), unless contraindicated', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Used with methotrexate; not concurrent with other biologics', logic_operator: 'AND' },
            { criterion_type: 'prescriber', description: 'Prescribed by a rheumatologist', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['rheumatologist'],
        },
      },
      {
        name: 'Pemphigus Vulgaris',
        icd10_codes: ['L10.0'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'diagnosis', description: 'Confirmed pemphigus vulgaris by biopsy and direct immunofluorescence', logic_operator: 'AND' },
            { criterion_type: 'combination_restriction', description: 'Used with corticosteroids', logic_operator: 'AND' },
          ],
        },
      },
    ],
    exclusions: [],
    confidence_scores: { overall: 0.91, drug_identification: 0.99, pa_criteria_completeness: 0.88 },
  },
]

export const rituximabInsights: InsightCard[] = [
  {
    severity: 'high',
    text: 'UnitedHealth requires failure of ≥2 TNF inhibitors before rituximab for RA — a higher step therapy bar than Blue Cross NC (which requires only 1) and Cigna (which requires ≥1 DMARD or any prior biologic). This creates a significant access disparity for treatment-refractory RA patients.',
    action: 'For UHC RA patients, document 2 separate TNF inhibitor failures with specific dates, drug names, and duration. Cigna and BCNC can proceed with 1 TNF failure — route these patients accordingly.',
  },
  {
    severity: 'high',
    text: 'UHC requires step therapy for ANCA-associated vasculitis induction (cyclophosphamide failure or contraindication first). Cigna and Blue Cross NC allow rituximab as first-line induction without a step therapy requirement.',
    action: 'For UHC ANCA vasculitis patients, confirm cyclophosphamide contraindication documentation before submission. Route newly diagnosed patients to Cigna or BCNC when coverage permits to avoid induction delays.',
  },
  {
    severity: 'low',
    text: 'Authorization duration varies 6× across payers for ANCA vasculitis: Cigna grants 1-month auth per course, BCNC and UHC grant 6–12 month auth windows. This affects how frequently patients need reauthorization.',
    action: 'Cigna ANCA-vasculitis patients require per-course authorization — build this into hub scheduling so reauth requests are submitted before each treatment course.',
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
      {
        payerName: 'Florida Blue',
        history: [
          { quarter: 'Q2 2025', score: 58 },
          { quarter: 'Q3 2025', score: 61 },
          { quarter: 'Q4 2025', score: 66 },
          { quarter: 'Q1 2026', score: 72 },
        ],
        delta: 14,
        direction: 'tightening',
      },
    ],
    livesAtRisk: '~3.2M commercially insured',
    lastUpdated: '2026-01-01',
    changeCount: 5,
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
    lastUpdated: '2025-11-01',
    changeCount: 4,
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
  {
    id: 'rituximab',
    brandName: 'Rituxan',
    genericName: 'rituximab',
    drugClass: 'Anti-CD20 antibody',
    jCode: 'J9310',
    policies: rituximabPolicies,
    insights: rituximabInsights,
    trends: [
      {
        payerName: 'Blue Cross NC',
        history: [
          { quarter: 'Q2 2025', score: 58 },
          { quarter: 'Q3 2025', score: 60 },
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
          { quarter: 'Q3 2025', score: 76 },
          { quarter: 'Q4 2025', score: 79 },
          { quarter: 'Q1 2026', score: 83 },
        ],
        delta: 11,
        direction: 'tightening',
      },
      {
        payerName: 'Cigna',
        history: [
          { quarter: 'Q2 2025', score: 50 },
          { quarter: 'Q3 2025', score: 49 },
          { quarter: 'Q4 2025', score: 47 },
          { quarter: 'Q1 2026', score: 44 },
        ],
        delta: -6,
        direction: 'loosening',
      },
    ],
    livesAtRisk: '~2.1M commercially insured',
    lastUpdated: '2025-10-01',
    changeCount: 2,
  },
]
