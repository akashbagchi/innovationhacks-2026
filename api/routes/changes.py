from fastapi import APIRouter

from db.mongo import policy_changelogs

router = APIRouter()


@router.get("")
async def list_changes(drug_id: str = "", severity: str = ""):
    query: dict = {}
    if drug_id:
        query["drug_id"] = {"$regex": drug_id, "$options": "i"}
    if severity:
        query["severity"] = severity.upper()

    docs = await policy_changelogs.find(
        query, {"_id": 0}
    ).sort("date", -1).to_list(length=200)

    return {"changes": docs, "count": len(docs)}
