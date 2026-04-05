"""
Seed MongoDB with normalized policy JSONs derived from outputs/policy_records/.
Run from inside the api/ directory:  python seed_mongo.py
Or from project root:                python api/seed_mongo.py
"""
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pipeline.normalize import normalize_policy_record

load_dotenv()

# Works both locally (outputs/ at project root) and in Docker (mounted at /outputs)
_local = Path(__file__).parent.parent / "outputs" / "policy_records"
_docker = Path("/outputs/policy_records")
RECORDS_DIR = _docker if _docker.exists() else _local

# Map filename → (drug_id, payer_canonical)
FILE_MAP = {
    "BCBS NC - Corporate Medical Policy_ Preferred Injectable Oncology Program (Avastin example).json": (
        "bevacizumab",
        "Blue Cross NC",
    ),
    "Florida Blue MCG Bevecizumab policy.json": (
        "bevacizumab",
        "Florida Blue",
    ),
    "Cigna Rituximab Intravenous Products for Non-Oncology Indications.json": (
        "rituximab",
        "Cigna",
    ),
    "UHC Botulinum Toxins A and B – Commercial Medical Benefit Drug Policy.json": (
        "onabotulinumtoxina",
        "UnitedHealth",
    ),
    "Priority Health 2026 MDL - Priority Health Commercial (Employer Group) and MyPriority.json": (
        "formulary",
        "Priority Health",
    ),
}

# Also seed the three mock drugs (Avastin/Dupixent/Keytruda × 3 payers) so the
# frontend portfolio view works immediately with real DB data.
# These are taken directly from the frontend mock files, serialised here as dicts.
MOCK_POLICIES = [
    # ── Avastin ──────────────────────────────────────────────────────────────
    {
        "drug_id": "bevacizumab",
        "payer_canonical": "UnitedHealth",
        "filename": "__mock__uhc_avastin",
        "policy_record": {
            "payer": {
                "name": "UnitedHealth",
                "policy_id": "CS-ONCO-0044",
                "policy_title": "UHC: Bevacizumab (Avastin)",
                "effective_date": "2025-10-01",
            },
            "drug": {
                "brand_name": "Avastin",
                "generic_name": "bevacizumab",
                "j_codes": ["J9035"],
                "drug_class": "VEGF inhibitor",
                "route_of_administration": "intravenous infusion",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Metastatic colorectal cancer",
                    "icd10_codes": ["C18.9"],
                    "pa_required": True,
                    "step_therapy_required": True,
                    "initial_authorization": {
                        "criteria": [
                            {
                                "criterion_type": "diagnosis",
                                "description": "Metastatic colorectal cancer confirmed",
                                "logic_operator": "AND",
                            },
                            {
                                "criterion_type": "step_therapy",
                                "description": "Failure of prior fluorouracil-based regimen for second-line use",
                                "logic_operator": "AND",
                            },
                            {
                                "criterion_type": "lab_value",
                                "description": "Child-Pugh score A required for HCC indication",
                                "logic_operator": "AND",
                            },
                        ]
                    },
                }
            ],
            "exclusions": [],
            "confidence_scores": {"overall": 0.88},
        },
    },
    {
        "drug_id": "bevacizumab",
        "payer_canonical": "Cigna",
        "filename": "__mock__cigna_avastin",
        "policy_record": {
            "payer": {
                "name": "Cigna",
                "policy_id": "CPO-BEV-2025",
                "policy_title": "Cigna Coverage Policy: Bevacizumab",
                "effective_date": "2025-07-01",
            },
            "drug": {
                "brand_name": "Avastin",
                "generic_name": "bevacizumab",
                "j_codes": ["J9035"],
                "drug_class": "VEGF inhibitor",
                "route_of_administration": "intravenous infusion",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Metastatic colorectal cancer",
                    "icd10_codes": ["C18.9"],
                    "pa_required": True,
                    "step_therapy_required": False,
                    "initial_authorization": {
                        "authorization_duration_months": 12,
                        "criteria": [
                            {
                                "criterion_type": "diagnosis",
                                "description": "Metastatic colorectal cancer",
                                "logic_operator": "AND",
                            }
                        ],
                    },
                }
            ],
            "exclusions": [],
            "confidence_scores": {"overall": 0.92},
        },
    },
    # ── Dupixent ─────────────────────────────────────────────────────────────
    {
        "drug_id": "dupilumab",
        "payer_canonical": "Blue Cross NC",
        "filename": "__mock__bcnc_dupixent",
        "policy_record": {
            "payer": {
                "name": "Blue Cross NC",
                "policy_id": "CP.PMN.112",
                "policy_title": "Clinical Policy Bulletin: Dupilumab (Dupixent)",
                "effective_date": "2025-01-01",
            },
            "drug": {
                "brand_name": "Dupixent",
                "generic_name": "dupilumab",
                "j_codes": ["J0222"],
                "drug_class": "IL-4/IL-13 receptor antagonist",
                "route_of_administration": "subcutaneous injection",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Atopic dermatitis",
                    "icd10_codes": ["L20.0"],
                    "pa_required": True,
                    "step_therapy_required": True,
                    "initial_authorization": {
                        "authorization_duration_months": 6,
                        "criteria": [
                            {
                                "criterion_type": "diagnosis",
                                "description": "Confirmed moderate-to-severe atopic dermatitis",
                                "logic_operator": "AND",
                            },
                            {
                                "criterion_type": "step_therapy",
                                "description": "Inadequate response to ≥2 topical therapies for ≥12 weeks",
                                "logic_operator": "AND",
                            },
                        ],
                    },
                }
            ],
            "exclusions": [{"description": "Not covered for mild atopic dermatitis"}],
            "confidence_scores": {"overall": 0.96},
        },
    },
    {
        "drug_id": "dupilumab",
        "payer_canonical": "UnitedHealth",
        "filename": "__mock__uhc_dupixent",
        "policy_record": {
            "payer": {
                "name": "UnitedHealth",
                "policy_id": "CS-DERM-0018",
                "policy_title": "UnitedHealthcare: Dupilumab (Dupixent)",
                "effective_date": "2025-10-01",
            },
            "drug": {
                "brand_name": "Dupixent",
                "generic_name": "dupilumab",
                "j_codes": ["J0222"],
                "drug_class": "IL-4/IL-13 receptor antagonist",
                "route_of_administration": "subcutaneous injection",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Atopic dermatitis",
                    "icd10_codes": ["L20.0"],
                    "pa_required": True,
                    "step_therapy_required": True,
                    "initial_authorization": {
                        "authorization_duration_months": 6,
                        "criteria": [
                            {
                                "criterion_type": "step_therapy",
                                "description": "Failure of ≥2 conventional systemic therapies (methotrexate or cyclosporine) for ≥12 weeks",
                                "logic_operator": "AND",
                            },
                            {
                                "criterion_type": "prescriber",
                                "description": "Must be prescribed by dermatologist or allergist",
                                "logic_operator": "AND",
                            },
                            {
                                "criterion_type": "lab_value",
                                "description": "eGFR >30 mL/min/1.73m² at baseline",
                                "logic_operator": "AND",
                            },
                        ],
                    },
                }
            ],
            "exclusions": [
                {"description": "Not covered concurrently with other biologics"}
            ],
            "confidence_scores": {"overall": 0.91},
        },
    },
    {
        "drug_id": "dupilumab",
        "payer_canonical": "Cigna",
        "filename": "__mock__cigna_dupixent",
        "policy_record": {
            "payer": {
                "name": "Cigna",
                "policy_id": "CPO-DUP-2025",
                "policy_title": "Cigna Coverage Policy: Dupilumab",
                "effective_date": "2025-07-01",
            },
            "drug": {
                "brand_name": "Dupixent",
                "generic_name": "dupilumab",
                "j_codes": ["J0222"],
                "drug_class": "IL-4/IL-13 receptor antagonist",
                "route_of_administration": "subcutaneous injection",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Atopic dermatitis",
                    "icd10_codes": ["L20.0"],
                    "pa_required": True,
                    "step_therapy_required": False,
                    "initial_authorization": {
                        "authorization_duration_months": 12,
                        "criteria": [
                            {
                                "criterion_type": "diagnosis",
                                "description": "Moderate-to-severe atopic dermatitis",
                                "logic_operator": "AND",
                            },
                            {
                                "criterion_type": "prior_therapy",
                                "description": "Inadequate response to topical corticosteroids",
                                "logic_operator": "AND",
                            },
                        ],
                    },
                },
                {
                    "name": "CRS with nasal polyps",
                    "icd10_codes": ["J33.0"],
                    "pa_required": False,
                    "step_therapy_required": False,
                    "initial_authorization": {"criteria": []},
                },
            ],
            "exclusions": [],
            "confidence_scores": {"overall": 0.98},
        },
    },
    # ── Keytruda ─────────────────────────────────────────────────────────────
    {
        "drug_id": "pembrolizumab",
        "payer_canonical": "Blue Cross NC",
        "filename": "__mock__bcnc_keytruda",
        "policy_record": {
            "payer": {
                "name": "Blue Cross NC",
                "policy_id": "CPO-PEM-2025",
                "policy_title": "Corporate Medical Policy: Pembrolizumab (Keytruda)",
                "effective_date": "2025-04-01",
            },
            "drug": {
                "brand_name": "Keytruda",
                "generic_name": "pembrolizumab",
                "j_codes": ["J9271"],
                "drug_class": "PD-1 checkpoint inhibitor",
                "route_of_administration": "intravenous (IV) infusion",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Melanoma",
                    "icd10_codes": ["C43.9"],
                    "pa_required": True,
                    "step_therapy_required": False,
                    "initial_authorization": {
                        "criteria": [
                            {
                                "criterion_type": "diagnosis",
                                "description": "Unresectable or metastatic melanoma",
                                "logic_operator": "AND",
                            }
                        ]
                    },
                },
                {
                    "name": "NSCLC",
                    "icd10_codes": ["C34.10"],
                    "pa_required": True,
                    "step_therapy_required": False,
                    "initial_authorization": {
                        "authorization_duration_months": 6,
                        "criteria": [
                            {
                                "criterion_type": "lab_value",
                                "description": "PD-L1 TPS ≥1%",
                                "logic_operator": "AND",
                            }
                        ],
                    },
                },
            ],
            "exclusions": [],
            "confidence_scores": {"overall": 0.93},
        },
    },
    {
        "drug_id": "pembrolizumab",
        "payer_canonical": "UnitedHealth",
        "filename": "__mock__uhc_keytruda",
        "policy_record": {
            "payer": {
                "name": "UnitedHealth",
                "policy_id": "CS-ONCO-0091",
                "policy_title": "UHC Medical Policy: Pembrolizumab (Keytruda)",
                "effective_date": "2025-10-01",
            },
            "drug": {
                "brand_name": "Keytruda",
                "generic_name": "pembrolizumab",
                "j_codes": ["J9271"],
                "drug_class": "PD-1 checkpoint inhibitor",
                "route_of_administration": "intravenous (IV) infusion",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Melanoma",
                    "icd10_codes": ["C43.9"],
                    "pa_required": True,
                    "step_therapy_required": True,
                    "initial_authorization": {
                        "authorization_duration_months": 6,
                        "criteria": [
                            {
                                "criterion_type": "diagnosis",
                                "description": "Unresectable stage III/IV melanoma",
                                "logic_operator": "AND",
                            },
                            {
                                "criterion_type": "prior_therapy",
                                "description": "Must have failed BRAF inhibitor if BRAF V600 mutant",
                                "logic_operator": "AND",
                            },
                        ],
                    },
                }
            ],
            "exclusions": [
                {
                    "description": "Not covered for MSI-H/dMMR tumors without prior chemotherapy"
                }
            ],
            "confidence_scores": {"overall": 0.89},
        },
    },
    {
        "drug_id": "pembrolizumab",
        "payer_canonical": "Cigna",
        "filename": "__mock__cigna_keytruda",
        "policy_record": {
            "payer": {
                "name": "Cigna",
                "policy_id": "CPO-KEY-2025",
                "policy_title": "Cigna Coverage Policy: Pembrolizumab (Keytruda)",
                "effective_date": "2025-01-01",
            },
            "drug": {
                "brand_name": "Keytruda",
                "generic_name": "pembrolizumab",
                "j_codes": ["J9271"],
                "drug_class": "PD-1 checkpoint inhibitor",
                "route_of_administration": "intravenous (IV) infusion",
                "benefit_type": "medical",
            },
            "indications": [
                {
                    "name": "Melanoma",
                    "icd10_codes": ["C43.9"],
                    "pa_required": True,
                    "step_therapy_required": False,
                    "initial_authorization": {
                        "authorization_duration_months": 12,
                        "criteria": [
                            {
                                "criterion_type": "diagnosis",
                                "description": "Unresectable or metastatic melanoma",
                                "logic_operator": "AND",
                            }
                        ],
                    },
                },
                {
                    "name": "TMB-H solid tumors",
                    "icd10_codes": [],
                    "pa_required": False,
                    "step_therapy_required": False,
                    "initial_authorization": {"criteria": []},
                },
            ],
            "exclusions": [],
            "confidence_scores": {"overall": 0.97},
        },
    },
]

# Change log entries (mirrors mockChanges.ts)
MOCK_CHANGES = [
    {
        "severity": "HIGH",
        "payer": "UnitedHealth",
        "drug": "Avastin (bevacizumab)",
        "drug_id": "bevacizumab",
        "change_type": "ADDED_STEP_THERAPY",
        "summary": "Step therapy added for metastatic colorectal cancer — must document failure of prior fluorouracil-based regimen for second-line use",
        "date": "2025-10-01",
    },
    {
        "severity": "HIGH",
        "payer": "UnitedHealth",
        "drug": "Avastin (bevacizumab)",
        "drug_id": "bevacizumab",
        "change_type": "ADDED_CRITERION",
        "summary": "Lab value criterion added for HCC: Child-Pugh score A now required before authorization",
        "date": "2025-10-01",
    },
    {
        "severity": "MED",
        "payer": "Cigna",
        "drug": "Avastin (bevacizumab)",
        "drug_id": "bevacizumab",
        "change_type": "MODIFIED_THRESHOLD",
        "summary": "Authorization duration reduced for colorectal cancer: 18 months → 12 months",
        "date": "2025-07-01",
    },
    {
        "severity": "LOW",
        "payer": "Blue Cross NC",
        "drug": "Avastin (bevacizumab)",
        "drug_id": "bevacizumab",
        "change_type": "MODIFIED_WORDING",
        "summary": "Effective date updated: 2025-01-01 → 2026-01-01. No clinical criteria changes.",
        "date": "2026-01-01",
    },
]


async def seed():
    client = AsyncIOMotorClient(os.getenv("MONGODB_URI"))
    db = client.antonrx
    now = datetime.now(timezone.utc)

    print("=== Seeding policies from extracted JSONs ===")
    for filename, (drug_id, payer_canonical) in FILE_MAP.items():
        path = RECORDS_DIR / filename
        if not path.exists():
            print(f"  [skip] {filename} — file not found")
            continue
        with open(path) as f:
            policy_record = json.load(f)
        doc = {
            "status": "extracted",
            "drug_id": drug_id,
            "payer_canonical": payer_canonical,
            "filename": filename,
            "policy_record": policy_record,
            "extracted_at": now,
            "source": "pipeline",
        }
        result = await db.policies.replace_one(
            {"filename": filename}, doc, upsert=True
        )
        action = "inserted" if result.upserted_id else "updated"
        print(f"  [{action}] {payer_canonical} / {drug_id}")

    print("\n=== Seeding mock portfolio policies ===")
    for entry in MOCK_POLICIES:
        doc = {**entry, "status": "extracted", "extracted_at": now, "source": "mock"}
        result = await db.policies.replace_one(
            {"filename": entry["filename"]}, doc, upsert=True
        )
        action = "inserted" if result.upserted_id else "updated"
        print(f"  [{action}] {entry['payer_canonical']} / {entry['drug_id']}")

    print("\n=== Seeding change log ===")
    for entry in MOCK_CHANGES:
        doc = {**entry, "logged_at": now}
        result = await db.policy_changelogs.replace_one(
            {"payer": entry["payer"], "drug_id": entry["drug_id"], "change_type": entry["change_type"], "date": entry["date"]},
            doc,
            upsert=True,
        )
        action = "inserted" if result.upserted_id else "updated"
        print(f"  [{action}] {entry['severity']} — {entry['payer']} {entry['change_type']}")

    print("\n=== Creating indexes ===")
    await db.policies.create_index("drug_id")
    await db.policies.create_index("payer_canonical")
    await db.policies.create_index([("drug_id", 1), ("payer_canonical", 1)])
    await db.policy_changelogs.create_index("drug_id")
    await db.policy_changelogs.create_index("severity")
    print("  [ok] indexes created")

    client.close()
    print("\nDone.")


def _normalized_drug_id(record: dict) -> str:
    drug = record.get("drug") or {}
    generic = str(drug.get("normalized_generic_name") or "").strip().lower()
    if generic:
        return generic

    display = str(drug.get("display_name") or "").strip().lower()
    if display:
        return display

    return "unknown"


async def seed_normalized():
    client = AsyncIOMotorClient(os.getenv("MONGODB_URI"))
    db = client.antonrx
    now = datetime.now(timezone.utc)

    print("=== Seeding normalized policies from extracted JSONs ===")
    for filename, (fallback_drug_id, fallback_payer_canonical) in FILE_MAP.items():
        path = RECORDS_DIR / filename
        if not path.exists():
            print(f"  [skip] {filename} â€” file not found")
            continue

        with open(path) as f:
            extracted_record = json.load(f)

        normalized_record = normalize_policy_record(
            extracted_record,
            source_filename=filename,
        ).model_dump(mode="json", exclude_none=True)

        drug_id = _normalized_drug_id(normalized_record)
        if drug_id == "unknown":
            drug_id = fallback_drug_id

        payer_canonical = (normalized_record.get("payer") or {}).get(
            "name",
            fallback_payer_canonical,
        )

        doc = {
            "status": "normalized",
            "drug_id": drug_id,
            "payer_canonical": payer_canonical,
            "filename": filename,
            "policy_record": normalized_record,
            "normalized_at": now,
            "source": "pipeline",
        }
        result = await db.policies.replace_one(
            {"filename": filename}, doc, upsert=True
        )
        action = "inserted" if result.upserted_id else "updated"
        print(f"  [{action}] {payer_canonical} / {drug_id}")

    print("\n=== Seeding normalized mock portfolio policies ===")
    for entry in MOCK_POLICIES:
        normalized_record = normalize_policy_record(
            entry["policy_record"],
            source_filename=entry["filename"],
        ).model_dump(mode="json", exclude_none=True)
        normalized_drug_id = _normalized_drug_id(normalized_record)
        doc = {
            **entry,
            "status": "normalized",
            "drug_id": (
                normalized_drug_id if normalized_drug_id != "unknown" else entry["drug_id"]
            ),
            "payer_canonical": (normalized_record.get("payer") or {}).get(
                "name",
                entry["payer_canonical"],
            ),
            "policy_record": normalized_record,
            "normalized_at": now,
            "source": "mock",
        }
        result = await db.policies.replace_one(
            {"filename": entry["filename"]}, doc, upsert=True
        )
        action = "inserted" if result.upserted_id else "updated"
        print(f"  [{action}] {doc['payer_canonical']} / {doc['drug_id']}")

    print("\n=== Seeding change log ===")
    for entry in MOCK_CHANGES:
        doc = {**entry, "logged_at": now}
        result = await db.policy_changelogs.replace_one(
            {
                "payer": entry["payer"],
                "drug_id": entry["drug_id"],
                "change_type": entry["change_type"],
                "date": entry["date"],
            },
            doc,
            upsert=True,
        )
        action = "inserted" if result.upserted_id else "updated"
        print(f"  [{action}] {entry['severity']} â€” {entry['payer']} {entry['change_type']}")

    print("\n=== Creating indexes ===")
    await db.policies.create_index("drug_id")
    await db.policies.create_index("payer_canonical")
    await db.policies.create_index([("drug_id", 1), ("payer_canonical", 1)])
    await db.policy_changelogs.create_index("drug_id")
    await db.policy_changelogs.create_index("severity")
    print("  [ok] indexes created")

    client.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(seed_normalized())
