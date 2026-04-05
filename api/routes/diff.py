from fastapi import APIRouter, HTTPException, Query

from db.mongo import policies, policy_versions
from pipeline.diff import diff_policy_records

router = APIRouter()


@router.get("")
async def diff_versions(
    drug_id: str = Query(..., description="Normalized drug id, e.g. 'dupilumab'"),
    payer: str = Query(..., description="Payer canonical name, e.g. 'UnitedHealth'"),
    from_version: int | None = Query(None, description="Version number to diff from (defaults to latest archived)"),
    to_version: int | None = Query(None, description="Version number to diff to (defaults to current)"),
):
    """
    Diff two versions of a policy for a given drug + payer.

    Defaults to current version vs. the most recent archived version.
    Returns the structured change list plus both policy records for display.
    """
    query = {"drug_id": {"$regex": drug_id, "$options": "i"}, "payer_canonical": {"$regex": payer, "$options": "i"}}

    # Resolve "to" record — current version in policies collection
    current = await policies.find_one({**query, "status": {"$in": ["normalized", "extracted"]}})
    if not current:
        raise HTTPException(status_code=404, detail=f"No policy found for drug_id='{drug_id}', payer='{payer}'")

    current_version = current.get("version", 1)

    # If a specific to_version is requested and it's not the current one, look in archives
    to_record = current
    resolved_to = current_version
    if to_version is not None and to_version != current_version:
        to_record = await policy_versions.find_one({**query, "version": to_version})
        if not to_record:
            raise HTTPException(status_code=404, detail=f"Version {to_version} not found in archives")
        resolved_to = to_version

    # Resolve "from" record — most recent archived version (or specific if requested)
    if from_version is not None:
        from_record = await policy_versions.find_one({**query, "version": from_version})
    else:
        # Default: most recently archived version
        results = await policy_versions.find(query).sort("version", -1).limit(1).to_list(1)
        from_record = results[0] if results else None

    if not from_record:
        return {
            "drug_id": drug_id,
            "payer": payer,
            "from_version": None,
            "to_version": resolved_to,
            "changes": [],
            "message": "Only one version exists — no diff available yet.",
            "current_record": current.get("policy_record"),
        }

    resolved_from = from_record.get("version")

    # Run the diff engine
    payer_name = current.get("payer_canonical", payer)
    drug_display = (
        (current.get("policy_record") or {}).get("drug", {}).get("brand_name")
        or drug_id
    )

    changes = diff_policy_records(
        old=from_record.get("policy_record") or {},
        new=to_record.get("policy_record") or {},
        payer=payer_name,
        drug=drug_display,
        drug_id=drug_id,
    )

    return {
        "drug_id": drug_id,
        "payer": payer_name,
        "from_version": resolved_from,
        "to_version": resolved_to,
        "change_count": len(changes),
        "changes": changes,
        "from_record": from_record.get("policy_record"),
        "to_record": to_record.get("policy_record"),
    }
