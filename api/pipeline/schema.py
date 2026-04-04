"""
Canonical PolicyRecord schema for LLM tool-use extraction.

Fields requiring external API calls (RxNorm CUI, LOINC codes, validated ICD-10
lookups) are intentionally absent, will be populated by the normalization
service downstream.

Import POLICY_RECORD_SCHEMA wherever a structured extraction call is made.
"""

_CRITERION_ITEM = {
    "type": "object",
    "properties": {
        "criterion_type": {
            "type": "string",
            "enum": [
                "diagnosis",
                "disease_severity",
                "step_therapy",
                "lab_value",
                "prescriber",
                "combination_restriction",
                "age_weight",
                "clinical_response",
                "other",
            ],
        },
        "description": {
            "type": "string",
            "description": "Criterion text exactly as stated in the document",
        },
        "logic_operator": {
            "type": "string",
            "enum": ["AND", "OR"],
            "description": "How this criterion combines with the next",
        },
    },
    "required": ["criterion_type", "description", "logic_operator"],
}

_AUTH_BLOCK = {
    "type": "object",
    "properties": {
        "criteria": {"type": "array", "items": _CRITERION_ITEM},
        "authorization_duration_months": {"type": "integer"},
        "required_prescriber_specialties": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["criteria"],
}

POLICY_RECORD_SCHEMA = {
    "type": "object",
    "properties": {
        "payer": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "policy_id": {
                    "type": "string",
                    "description": "Payer's internal policy/program number, e.g. CP.PMN.112 or 2025 P 2116-22",
                },
                "policy_title": {"type": "string"},
                "effective_date": {
                    "type": "string",
                    "description": "ISO 8601 if parseable, otherwise as written",
                },
                "revision_date": {"type": "string"},
            },
            "required": ["name", "policy_title"],
        },
        "drug": {
            "type": "object",
            "properties": {
                "brand_name": {"type": "string"},
                "generic_name": {"type": "string"},
                "j_codes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "HCPCS J-codes explicitly stated in the document, e.g. J0222",
                },
                "hcpcs_codes": {"type": "array", "items": {"type": "string"}},
                "drug_class": {
                    "type": "string",
                    "description": "Mechanism of action, e.g. IL-4/IL-13 receptor antagonist",
                },
                "route_of_administration": {"type": "string"},
                "benefit_type": {
                    "type": "string",
                    "description": "medical or pharmacy",
                },
                "limitations_of_use": {
                    "type": "string",
                    "description": "Any explicitly stated limitations of use",
                },
            },
            "required": ["brand_name", "generic_name"],
        },
        "indications": {
            "type": "array",
            "description": "One entry per covered indication (disease/condition)",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Short indication name, e.g. Atopic Dermatitis",
                    },
                    "description": {"type": "string"},
                    "icd10_codes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Only codes EXPLICITLY stated in the document — do not infer",
                    },
                    "pa_required": {"type": "boolean"},
                    "step_therapy_required": {"type": "boolean"},
                    "initial_authorization": _AUTH_BLOCK,
                    "reauthorization": _AUTH_BLOCK,
                },
                "required": ["name", "pa_required", "initial_authorization"],
            },
        },
        "exclusions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"description": {"type": "string"}},
                "required": ["description"],
            },
        },
        "confidence_scores": {
            "type": "object",
            "properties": {
                "overall": {
                    "type": "number",
                    "description": "0.0–1.0 confidence in the overall extraction",
                },
                "drug_identification": {"type": "number"},
                "pa_criteria_completeness": {"type": "number"},
                "review_flags": {
                    "type": "string",
                    "description": "Flag ambiguous extractions, missing data, or low-confidence fields",
                },
            },
            "required": ["overall"],
        },
    },
    "required": ["payer", "drug", "indications", "confidence_scores"],
}
