from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, HTTPException

from db.mongo import policies, policy_versions
from pipeline.diff import diff_policy_records

router = APIRouter()


def _drug_display(policy_record: dict) -> str:
    drug = policy_record.get("drug") or {}
    brand = str(drug.get("display_name") or drug.get("brand_name") or "").strip()
    generic = str(
        drug.get("normalized_generic_name") or drug.get("generic_name") or ""
    ).strip()
    if brand and generic and brand.lower() != generic.lower():
        return f"{brand} ({generic})"
    return brand or generic or "Unknown drug"


async def _load_version_snapshot(current_doc: dict, version: int) -> tuple[dict, str]:
    current_version = current_doc.get("version", 1) or 1
    if version == current_version:
        return current_doc, "current"

    archived = await policy_versions.find_one(
        {"original_id": current_doc["_id"], "version": version}
    )
    if archived:
        return archived, "archived"

    raise HTTPException(
        status_code=404,
        detail=f"Version {version} not found for policy {current_doc['_id']}",
    )


@router.get("/{policy_id}")
async def diff(policy_id: str, from_version: int = 1, to_version: int = 2):
    if from_version < 1 or to_version < 1:
        raise HTTPException(status_code=400, detail="Versions must be positive integers")
    if from_version > to_version:
        raise HTTPException(
            status_code=400, detail="from_version must be less than or equal to to_version"
        )

    try:
        oid = ObjectId(policy_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid policy_id format")

    current_doc = await policies.find_one({"_id": oid})
    if not current_doc:
        raise HTTPException(status_code=404, detail="Policy not found")

    from_doc, from_source = await _load_version_snapshot(current_doc, from_version)
    to_doc, to_source = await _load_version_snapshot(current_doc, to_version)

    from_record = from_doc.get("policy_record") or {}
    to_record = to_doc.get("policy_record") or {}
    payer = (
        to_doc.get("payer_canonical")
        or from_doc.get("payer_canonical")
        or (to_record.get("payer") or {}).get("name")
        or (from_record.get("payer") or {}).get("name")
        or "Unknown payer"
    )
    drug_id = (
        to_doc.get("drug_id")
        or from_doc.get("drug_id")
        or "unknown"
    )
    drug = _drug_display(to_record or from_record)

    changes = diff_policy_records(
        old=from_record,
        new=to_record,
        payer=payer,
        drug=drug,
        drug_id=drug_id,
    )

    return {
        "policy_id": policy_id,
        "payer": payer,
        "drug": drug,
        "drug_id": drug_id,
        "from_version": from_version,
        "to_version": to_version,
        "from_snapshot": {
            "version": from_version,
            "source": from_source,
            "filename": from_doc.get("filename"),
            "status": from_doc.get("status"),
        },
        "to_snapshot": {
            "version": to_version,
            "source": to_source,
            "filename": to_doc.get("filename"),
            "status": to_doc.get("status"),
        },
        "diff": changes,
        "count": len(changes),
    }
