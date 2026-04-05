# Pipeline Accuracy Report
**Date:** 2026-04-05  
**Branch:** `dev/pranjal`  
**Model:** `claude-sonnet-4-6` (via instructor + Anthropic API)  
**Corpus:** 4 commercial payer PA policy documents (PDF)  
**Method:** Manual cross-validation — extracted JSON compared against source documents

---

## 1. Executive Summary

| Document | Payer | Drug | Pages | Source Indications | Extracted Indications | Accuracy |
|---|---|---|---|---|---|---|
| BCBS NC Preferred Injectable Oncology | BCBS NC | Bevacizumab / Rituximab / Trastuzumab | 26 | ~14 distinct | 33 (with duplicates) | **58%** |
| Cigna Rituximab Non-Oncology | Cigna | Rituximab IV | 32 | 18 | ~18 | **87%** |
| Florida Blue Bevacizumab | Florida Blue | Bevacizumab | 15 | 17 | 31 (with duplicates) | **70%** |
| UHC Botulinum Toxins A & B | UHC | OnabotulinumtoxinA / Abobotulinumtoxin / Rima / Inco / Daxi | 28 | 20+ | 5 | **25%** |

**Overall pipeline accuracy: ~60%** across the test corpus.  
**Critical blocker:** `max_tokens=4096` causes truncation on dense documents. UHC (28 pages) is the clearest failure case.

---

## 2. Per-Document Findings

---

### 2.1 BCBS NC — Preferred Injectable Oncology Program
**File:** `BCBS NC - Corporate Medical Policy_ Preferred Injectable Oncology Program (Avastin example).pdf`  
**Accuracy Score: 58%**

#### Metrics
| Field | Expected | Actual |
|---|---|---|
| Indications | ~14 distinct | 33 entries (duplicated) |
| Total criteria nodes | ~30 | 74 |
| Reauthorization entries | Present | 0 |
| `drug.hcpcs_codes` | J9035, J9312, J9355, Q5113, Q5117, Q5126, Q5129, Q5146, Q5160 | `null` |
| `drug.j_codes` | J9035, J9312, J9355 | `null` |
| `payer.effective_date` | 2026-01 | 2026-01 ✓ |
| `confidence_scores.overall` | — | 0.72 |

#### Issues Found

**[CRITICAL] Indication deduplication failure**  
The source document is structured as a bundled multi-drug policy covering bevacizumab, rituximab, and trastuzumab. The PA criteria section and the drug-specific section both list the same cancer indications. The pipeline captured both passes, resulting in near-identical indication entries appearing 2–3× (e.g. "Metastatic Colorectal Cancer (first- or second-line)" appears 3 times with overlapping but slightly different criteria).

**[HIGH] No per-drug-class separation**  
The source is effectively three separate PA policies in one document. The output should segment by drug class (bevacizumab / rituximab / trastuzumab) but instead merges all indications under a single drug object with `generic_name: "bevacizumab"`. Rituximab and trastuzumab indications are present but misattributed.

**[HIGH] HCPCS/J-codes not populated**  
9 HCPCS codes are explicitly listed in the source document. `drug.hcpcs_codes` and `drug.j_codes` are `null` in the output.

**[MEDIUM] Reauthorization criteria missing**  
Source contains continuation criteria for several indications. No `reauthorization` block present in any of the 33 extracted indication entries.

**[LOW] Confidence review_flags misleading**  
`review_flags` states "full PA clinical criteria not present in provided text." The document is a complete PA policy — this flag is incorrect and was generated because the LLM saw the preamble section first.

#### Criteria Accuracy (Sampled)
| Indication | Source | JSON | Match |
|---|---|---|---|
| Metastatic Colorectal Cancer (1st/2nd line) | 5 mg/kg q2w with IFL; 10 mg/kg q2w with FOLFOX4 | Correctly stated | ✓ |
| Recurrent Glioblastoma | 10 mg/kg q2w | Correctly stated | ✓ |
| Step therapy (non-preferred bevacizumab) | Documented SAE to both Mvasi AND Zirabev + FDA MedWatch form | Correctly captured | ✓ |

---

### 2.2 Cigna — Rituximab Intravenous Products for Non-Oncology Indications
**File:** `Cigna Rituximab Intravenous Products for Non-Oncology Indications.pdf`  
**Accuracy Score: 87%**

#### Metrics
| Field | Expected | Actual |
|---|---|---|
| Indications | 18 | ~18 (inferred — JSON too large to fully verify) |
| Total criteria nodes | ~90 | 91 |
| Reauthorization entries | Present for most | 10 ✓ |
| `drug.hcpcs_codes` | J9312, Q5115, Q5119, Q5123 | `null` |
| `drug.j_codes` | J9312 | `null` |
| `payer.effective_date` | 02/01/2026 | `null` |
| `confidence_scores.overall` | — | 0.82 |

#### Issues Found

**[HIGH] HCPCS/J-codes not populated**  
J9312 (Rituxan), Q5115 (Truxima), Q5119 (Ruxience), Q5123 (Riabni) are explicitly listed in source. `drug.hcpcs_codes` and `drug.j_codes` are `null`.

**[MEDIUM] `effective_date` not extracted**  
Source cover page states "02/01/2026". `payer.effective_date` is `null`.

**[MEDIUM] Preferred product hierarchy underrepresented**  
Source has explicit tables showing Riabni/Ruxience/Truxima as preferred over Rituxan for most indications. This tiering is mentioned in criteria text but not structured as a dedicated `preferred_products` field.

**[LOW] RA continuation criteria partially truncated**  
`review_flags` notes RA Section C (2+ courses) and subsequent indications are truncated. Minor impact — core initial/reauth criteria for RA are correctly captured.

#### Criteria Accuracy (Sampled)
| Indication | Source | JSON | Match |
|---|---|---|---|
| ANCA-Associated Vasculitis (induction) | 375 mg/m² ×4 doses q7d OR two 1,000 mg doses q2w; requires glucocorticoids | Correctly stated including dosing variants | ✓ |
| Pemphigus Vulgaris (initial) | Two 1,000 mg doses ≥2 weeks apart; relapse same, min 16 weeks between courses; maintenance 500 mg q6mo | Correctly captured | ✓ |
| Multiple Sclerosis | Inadequate efficacy to ≥2 other disease-modifying agents | Correctly stated as "at least TWO other disease-modifying agents" | ✓ |

---

### 2.3 Florida Blue — Bevacizumab MCG Policy
**File:** `Florida Blue MCG Bevecizumab policy.pdf`  
**Accuracy Score: 70%**

#### Metrics
| Field | Expected | Actual |
|---|---|---|
| Indications | 17 named | 31 entries (ICD-10 groups + named duplicates) |
| Total criteria nodes | ~17–34 | 33 |
| Reauthorization entries | 2 (180-day initial / 1-year continuation) | 0 |
| `drug.hcpcs_codes` | J9035, Q5107, Q5118, Q5126, Q5129, Q5160 | `null` |
| `payer.effective_date` | 01/01/2026 | `null` |
| ICD-10 codes | ~60 codes across indication table | Partially captured at indication level |
| `confidence_scores.overall` | — | 0.82 |

#### Source Indications (Table 1 — ground truth)
Ampullary Cancer, Cervical Cancer, CNS Cancer, Colon & Rectal Cancer, Hepatocellular Carcinoma, Hereditary Hemorrhagic Telangiectasia, Kidney Cancer, Mesothelioma, NSCLC, Ovarian Cancer, Radiation Necrosis of the Brain, Small Bowel Adenocarcinoma, Soft Tissue Sarcoma, Uterine Neoplasm, Vaginal Cancer, Vulvar Cancer, Other FDA/NCCN-supported diagnosis.

#### Issues Found

**[CRITICAL] Structural duplication from ICD-10 table extraction**  
The source document contains both a named indication table (Table 1 with per-indication criteria) and a separate ICD-10 diagnosis code table. The pipeline extracted both, creating ~14 duplicate entries — e.g. "Malignant Neoplasm of Colon, Rectosigmoid Junction, Rectum, and Anus" (from ICD-10 table) and "Colon Cancer" (from Table 1) appear as separate indications with different criteria.

**[HIGH] Per-indication criteria from Table 1 not fully captured**  
Table 1 has specific multi-part criteria per indication (e.g. CNS Cancer has 6 sub-conditions covering glioblastoma, meningioma, NF2, pediatric glioma etc.). The JSON collapses most of these into single-line criteria, losing specificity.

**[HIGH] Auth duration missing**  
Source explicitly states "Approval Duration: 180 days" (initial) and "Approval duration: 1 year" (continuation with step therapy). Neither appears in any indication's `authorization_duration_months`.

**[HIGH] HCPCS codes not populated**  
6 codes in source (J9035, Q5107, Q5118, Q5126, Q5129, Q5160). All `null`.

**[MEDIUM] ICD-10 coverage incomplete**  
Source has ~60 ICD-10 codes in a dedicated table. Only a subset appears at the indication level in the JSON.

**[MEDIUM] `effective_date` not extracted**  
Source header states "Revised: 01/01/26". `payer.effective_date` is `null`.

---

### 2.4 UHC — Botulinum Toxins A and B Commercial Medical Benefit Drug Policy
**File:** `UHC Botulinum Toxins A and B – Commercial Medical Benefit Drug Policy.pdf`  
**Accuracy Score: 25%**

#### Metrics
| Field | Expected | Actual |
|---|---|---|
| Indications | 20+ named indications | 5 |
| Total criteria nodes | ~100+ | 15 |
| Reauthorization entries | Present for all indications | 4 (only for extracted 5) |
| `drug.hcpcs_codes` | J0585, J0586, J0587, J0588, J0589 | `null` |
| `drug.j_codes` | J0585, J0586, J0587, J0588, J0589 | `null` |
| `payer.effective_date` | 2026-01-01 | 2026-01-01 ✓ |
| `confidence_scores.overall` | — | 0.62 |

#### Missing Indications (not captured at all)
The following proven indications are stated in the source document but absent from the extracted JSON:
- Chronic migraine (Botox only)
- Upper limb spasticity
- Lower limb spasticity
- Spasticity (general)
- Blepharospasm
- Primary axillary hyperhidrosis
- Overactive bladder
- Neurogenic detrusor overactivity (Botox)
- Achalasia (Botox, Dysport)
- Hemifacial spasm
- Anal fissure (Dysport)
- Laryngeal spasm
- All Dysport-specific proven indications
- All Xeomin-specific proven indications
- Entire Daxxify exclusion criteria
- ICD-10 mapping tables (pages 4–25, ~200+ codes)

#### Root Cause Analysis

**[CRITICAL] `max_tokens=4096` hard limit**  
The document is 28 pages. The LLM hit the output token limit after generating the General Requirements section plus 4 diagnosis-specific indications. The `review_flags` in the output explicitly states: *"Document is truncated at page 2 of 28."*

**[CRITICAL] Heading detection failure**  
Only 3 headings were detected in this document (`document_format.heading_count: 3`) vs 27 for Florida Blue. UHC uses a flat document structure with no standard heading hierarchy — the segmenter produced very few sections, sending the entire 28-page policy as one large section block which worsens the token pressure.

**[HIGH] HCPCS codes not populated**  
5 HCPCS codes (J0585–J0589) are in an explicit table on page 4. All `null`.

---

## 3. Cross-Cutting Issues

| Issue | Affected Documents | Severity |
|---|---|---|
| `drug.hcpcs_codes` always `null` — codes extracted into criteria text but not mapped to schema field | All 4 | HIGH |
| `drug.j_codes` always `null` | All 4 | HIGH |
| `payer.effective_date` unpopulated despite being in source | Cigna, Florida Blue | MEDIUM |
| Reauthorization criteria missing entirely | BCBS NC, Florida Blue | MEDIUM |
| `max_tokens=4096` causes truncation on long documents | UHC (critical), Cigna (minor) | CRITICAL |
| Duplication from multi-pass extraction (criteria section + drug-specific section) | BCBS NC, Florida Blue | HIGH |
| Poor heading detection on flat-structured documents reduces segmentation quality | UHC | HIGH |

---

## 4. Prioritised Fix List

### P0 — Fix before any further testing
1. **Raise `max_tokens` to `8192`** in `api/pipeline/extract.py`  
   Location: `extract_policy_record()`, `max_tokens=4096`  
   Impact: Directly fixes UHC truncation; partially helps BCBS NC and Florida Blue.

### P1 — High impact, achievable quickly
2. **Map HCPCS/J-codes to schema fields**  
   The LLM sees these codes in the document but writes them into criteria `description` text instead of `drug.hcpcs_codes` / `drug.j_codes`. The extraction prompt needs to explicitly instruct the model to populate these fields.

3. **Fix `effective_date` extraction**  
   The date is present in page headers / revision lines. Prompt needs to explicitly ask for it from headers, not just body text.

4. **Deduplicate indications in multi-pass documents**  
   Add a post-processing deduplication step that merges indication entries with >80% name similarity and combines their criteria lists.

### P2 — Meaningful quality improvements
5. **Improve heading detection for flat-structured UHC-style documents**  
   Current heading detection relies on font size / bold flags. UHC uses plain body text for section breaks. Add a regex-based fallback that detects patterns like `"Proven" / "Unproven" / "General Requirements"`.

6. **Per-drug-class splitting for bundled policies**  
   Detect when a document covers multiple drugs (BCBS NC pattern) and split extraction into one `PolicyRecord` per drug class.

7. **Auth duration extraction from approval duration statements**  
   Add explicit prompt instruction to extract `authorization_duration_months` from "Approval Duration: 180 days / 1 year" patterns.

---

## 5. Test Environment

| Parameter | Value |
|---|---|
| Extraction model | `claude-sonnet-4-6` |
| `max_tokens` | 4096 |
| `max_retries` | 3 |
| Instructor mode | Tool use (`PolicyRecord` schema) |
| Segmentation | `segment_sections()` → `_prune_sections()` |
| Heading detection | Font size + bold flag heuristic |
| Document formats tested | PDF only (DOCX path not yet validated end-to-end) |
| Pipeline run time | ~267s for full corpus (5 documents) |

---

## 6. Retest Checklist

After each fix, rerun and verify:

- [ ] `docker compose exec api python -c "import asyncio; from pipeline.orchestrator import run_pipeline; asyncio.run(run_pipeline())"` completes without truncation warnings
- [ ] UHC Botulinum JSON has ≥15 indications
- [ ] `drug.hcpcs_codes` populated for all 4 documents
- [ ] `payer.effective_date` populated for Cigna and Florida Blue
- [ ] No indication name appears more than once per document output
- [ ] BCBS NC output is split into 3 separate records (bevacizumab / rituximab / trastuzumab) or clearly labelled
