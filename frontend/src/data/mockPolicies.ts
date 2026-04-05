import type { PolicyRecord, InsightCard } from '../types/policy'

export const mockPolicies: PolicyRecord[] = [
  {
    payer: {
      name: 'Blue Cross NC',
      policy_id: null,
      policy_title: 'Corporate Medical Policy: Preferred Injectable Oncology Program',
      effective_date: '2026-01-01',
      revision_date: '2026-01-01',
    },
    drug: {
      brand_name: 'Avastin',
      generic_name: 'bevacizumab',
      j_codes: ['J9035'],
      hcpcs_codes: ['J9035'],
      drug_class: 'Monoclonal antibody (VEGF inhibitor)',
      route_of_administration: 'intravenous (IV) infusion',
      benefit_type: 'medical',
      limitations_of_use: 'Not for adjuvant treatment of colon cancer',
    },
    indications: [
      {
        name: 'Metastatic colorectal cancer',
        description: 'Metastatic colorectal cancer, in combination with intravenous fluorouracil-based chemotherapy for first- or second-line treatment',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with intravenous fluorouracil-based chemotherapy', logic_operator: 'AND' },
            { criterion_type: 'line_of_therapy', description: 'first- or second-line treatment', logic_operator: 'OR' },
          ],
        },
      },
      {
        name: 'Metastatic colorectal cancer (2nd line, post-bevacizumab)',
        description: 'Metastatic colorectal cancer for second-line treatment in patients who have progressed on a first-line bevacizumab product-containing regimen',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with fluoropyrimidine-irinotecan- or fluoropyrimidine-oxaliplatin-based chemotherapy', logic_operator: 'AND' },
            { criterion_type: 'line_of_therapy', description: 'second-line treatment', logic_operator: 'AND' },
            { criterion_type: 'prior_therapy', description: 'patients who have progressed on a first-line bevacizumab product-containing regimen', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Non-squamous NSCLC',
        description: 'Unresectable, locally advanced, recurrent or metastatic non-squamous non-small cell lung cancer, first-line',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with carboplatin and paclitaxel', logic_operator: 'AND' },
            { criterion_type: 'line_of_therapy', description: 'first-line', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Recurrent glioblastoma',
        description: 'Recurrent glioblastoma in adults',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: { criteria: [] },
      },
      {
        name: 'Metastatic renal cell carcinoma',
        description: 'Metastatic renal cell carcinoma in combination with interferon alfa',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with interferon alfa', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Cervical cancer',
        description: 'Persistent, recurrent, or metastatic cervical cancer',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with paclitaxel and cisplatin, or paclitaxel and topotecan', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Epithelial ovarian cancer (initial)',
        description: 'Epithelial ovarian, fallopian tube, or primary peritoneal cancer — stage III or IV following initial surgical resection',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with carboplatin and paclitaxel, followed by bevacizumab as single agent', logic_operator: 'AND' },
            { criterion_type: 'disease_severity', description: 'stage III or IV disease following initial surgical resection', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Epithelial ovarian cancer (platinum-resistant)',
        description: 'Platinum-resistant recurrent epithelial ovarian, fallopian tube, or peritoneal cancer',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with paclitaxel, pegylated liposomal doxorubicin, or topotecan', logic_operator: 'AND' },
            { criterion_type: 'prior_therapy', description: 'no more than 2 prior chemotherapy regimens', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Epithelial ovarian cancer (platinum-sensitive)',
        description: 'Platinum-sensitive recurrent epithelial ovarian, fallopian tube, or peritoneal cancer',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with carboplatin and paclitaxel or carboplatin and gemcitabine', logic_operator: 'AND' },
            { criterion_type: 'disease_severity', description: 'platinum-sensitive recurrent disease', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Hepatocellular carcinoma',
        description: 'Unresectable or metastatic hepatocellular carcinoma, in combination with atezolizumab',
        icd10_codes: [],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with atezolizumab', logic_operator: 'AND' },
            { criterion_type: 'prior_therapy', description: 'patients who have not received prior systemic therapy', logic_operator: 'AND' },
          ],
        },
      },
    ],
    exclusions: [
      { description: 'Not covered for adjuvant treatment of colon cancer' },
    ],
    confidence_scores: { overall: 1.0 },
  },

  {
    payer: {
      name: 'UnitedHealth',
      policy_id: 'CS-ONCO-0042',
      policy_title: 'UnitedHealthcare Medical Policy: Bevacizumab (Avastin)',
      effective_date: '2025-10-01',
      revision_date: '2025-09-15',
    },
    drug: {
      brand_name: 'Avastin',
      generic_name: 'bevacizumab',
      j_codes: ['J9035'],
      hcpcs_codes: ['J9035'],
      drug_class: 'Monoclonal antibody (VEGF inhibitor)',
      route_of_administration: 'intravenous (IV) infusion',
      benefit_type: 'medical',
    },
    indications: [
      {
        name: 'Metastatic colorectal cancer',
        description: 'Metastatic colorectal cancer, first- or second-line',
        icd10_codes: ['C18.9', 'C19', 'C20'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with FOLFOX or FOLFIRI regimen', logic_operator: 'AND' },
            { criterion_type: 'line_of_therapy', description: 'first- or second-line', logic_operator: 'AND' },
            { criterion_type: 'prior_therapy', description: 'must have documented failure of at least one prior fluorouracil-based regimen for second-line use', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['oncology'],
        },
        reauthorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'clinical_response', description: 'documented tumor response or stable disease per imaging', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Non-squamous NSCLC',
        description: 'Non-squamous non-small cell lung cancer, first-line',
        icd10_codes: ['C34.10', 'C34.11', 'C34.12'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with carboplatin and paclitaxel', logic_operator: 'AND' },
            { criterion_type: 'line_of_therapy', description: 'first-line only', logic_operator: 'AND' },
            { criterion_type: 'prior_therapy', description: 'must have failed or be ineligible for EGFR/ALK-targeted therapy if applicable', logic_operator: 'AND' },
            { criterion_type: 'diagnosis', description: 'confirmed non-squamous histology by pathology report', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['oncology', 'pulmonology'],
        },
      },
      {
        name: 'Recurrent glioblastoma',
        description: 'Recurrent glioblastoma in adults after prior treatment',
        icd10_codes: ['C71.9'],
        pa_required: true,
        step_therapy_required: true,
        initial_authorization: {
          authorization_duration_months: 4,
          criteria: [
            { criterion_type: 'prior_therapy', description: 'must have documented progression after temozolomide and radiation', logic_operator: 'AND' },
            { criterion_type: 'diagnosis', description: 'confirmed recurrent GBM by MRI', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['neurology', 'oncology'],
        },
      },
      {
        name: 'Epithelial ovarian cancer',
        description: 'Ovarian, fallopian tube, or primary peritoneal cancer',
        icd10_codes: ['C56.1', 'C56.2', 'C57.00'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with carboplatin and paclitaxel', logic_operator: 'AND' },
            { criterion_type: 'disease_severity', description: 'stage III or IV disease', logic_operator: 'AND' },
            { criterion_type: 'lab_value', description: 'CA-125 elevated or radiographic evidence of disease', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['gynecologic oncology'],
        },
      },
      {
        name: 'Cervical cancer',
        description: 'Persistent, recurrent, or metastatic cervical cancer',
        icd10_codes: ['C53.9'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with paclitaxel/cisplatin or paclitaxel/topotecan', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['gynecologic oncology'],
        },
      },
      {
        name: 'Hepatocellular carcinoma',
        description: 'Unresectable or metastatic HCC in combination with atezolizumab',
        icd10_codes: ['C22.0'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 6,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with atezolizumab only', logic_operator: 'AND' },
            { criterion_type: 'prior_therapy', description: 'no prior systemic therapy for HCC', logic_operator: 'AND' },
            { criterion_type: 'lab_value', description: 'Child-Pugh score A', logic_operator: 'AND' },
          ],
          required_prescriber_specialties: ['oncology', 'hepatology'],
        },
      },
    ],
    exclusions: [
      { description: 'Not covered for adjuvant colon cancer' },
      { description: 'Not covered in combination with anthracycline-based regimens for breast cancer' },
    ],
    confidence_scores: { overall: 0.94, drug_identification: 0.99, pa_criteria_completeness: 0.91 },
  },

  {
    payer: {
      name: 'Cigna',
      policy_id: 'CPO-BEV-2025',
      policy_title: 'Cigna Coverage Policy: Bevacizumab (Avastin)',
      effective_date: '2025-07-01',
      revision_date: '2025-06-15',
    },
    drug: {
      brand_name: 'Avastin',
      generic_name: 'bevacizumab',
      j_codes: ['J9035'],
      hcpcs_codes: ['J9035'],
      drug_class: 'Monoclonal antibody (VEGF inhibitor)',
      route_of_administration: 'intravenous (IV) infusion',
      benefit_type: 'medical',
    },
    indications: [
      {
        name: 'Metastatic colorectal cancer',
        description: 'Metastatic or unresectable colorectal cancer, any line',
        icd10_codes: ['C18.9', 'C19', 'C20'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with fluorouracil-based chemotherapy', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Non-squamous NSCLC',
        description: 'Non-squamous NSCLC, first-line',
        icd10_codes: ['C34.10'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with carboplatin and paclitaxel', logic_operator: 'AND' },
            { criterion_type: 'line_of_therapy', description: 'first-line', logic_operator: 'AND' },
          ],
        },
      },
      {
        name: 'Recurrent glioblastoma',
        description: 'Recurrent glioblastoma in adults',
        icd10_codes: ['C71.9'],
        pa_required: false,
        step_therapy_required: false,
        initial_authorization: { criteria: [] },
      },
      {
        name: 'Hepatocellular carcinoma',
        description: 'Unresectable or metastatic HCC with atezolizumab',
        icd10_codes: ['C22.0'],
        pa_required: true,
        step_therapy_required: false,
        initial_authorization: {
          authorization_duration_months: 12,
          criteria: [
            { criterion_type: 'combination_restriction', description: 'in combination with atezolizumab', logic_operator: 'AND' },
            { criterion_type: 'prior_therapy', description: 'no prior systemic therapy', logic_operator: 'AND' },
          ],
        },
      },
    ],
    exclusions: [],
    confidence_scores: { overall: 0.97, drug_identification: 1.0, pa_criteria_completeness: 0.95 },
  },
]

export const mockInsights: InsightCard[] = [
  {
    severity: 'high',
    text: 'UnitedHealth is the most restrictive payer — requires step therapy on 3 of 6 indications and mandates specialist prescriber for all covered uses.',
    action: 'Prioritize Cigna for patient access programs — UHC step therapy requirement doubles time-to-treatment for mCRC and NSCLC. Brief field access teams on sequencing documentation requirements.',
  },
  {
    severity: 'medium',
    text: 'Cigna is the only payer that does not require PA for recurrent glioblastoma, making it the least restrictive path for that indication.',
    action: 'Alert field access team: Cigna glioblastoma access requires no PA. Position Cigna as preferred pathway in pull-through messaging for neuro-oncology accounts.',
  },
  {
    severity: 'low',
    text: 'All 3 payers cover metastatic colorectal cancer with bevacizumab, but UnitedHealth requires documented prior regimen failure for second-line use.',
    action: 'Update reauthorization scheduling systems — UHC 6-month auth requires 2× annual reauth burden vs BCNC 12-month. Flag for hub services to proactively initiate renewals at month 5.',
  },
]
