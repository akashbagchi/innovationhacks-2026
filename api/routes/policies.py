from fastapi import APIRouter, HTTPException, Query
from bson import ObjectId
from bson.errors import InvalidId

from db.mongo import policies

router = APIRouter()


def _serialize(doc: dict) -> dict:
    doc["_id"] = str(doc["_id"])
    return doc


@router.get("/document-url")
async def get_document_url(
    payer: str = Query(""),
    policy_id: str = Query(""),
):
    """Return a presigned S3 URL for the source policy document."""
    query: dict = {}
    if payer:
        query["payer"] = {"$regex": payer, "$options": "i"}
    if policy_id:
        query["policy_id"] = policy_id

    if not query:
        raise HTTPException(status_code=400, detail="Provide at least one of: payer, policy_id")

    doc = await policies.find_one(query)
    if not doc or not doc.get("s3_key"):
        raise HTTPException(status_code=404, detail="Document not found or no source PDF stored")

    from db.s3 import generate_presigned_url
    try:
        url = generate_presigned_url(doc["s3_key"])
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to generate document URL: {exc}")

    return {"url": url, "s3_key": doc["s3_key"], "payer": doc.get("payer"), "policy_id": doc.get("policy_id")}


@router.get("/search")
async def search(drug: str = "", payer: str = ""):
    query: dict = {}
    if drug:
        query["drug_id"] = {"$regex": drug, "$options": "i"}
    if payer:
        query["payer_canonical"] = {"$regex": payer, "$options": "i"}

    results = await policies.find(query).to_list(length=200)
    return {"results": [_serialize(r) for r in results], "count": len(results)}


@router.get("/by-drug/{drug_id}")
async def get_by_drug(drug_id: str):
    """Return all payer policies for a single drug (used by ComparisonMatrix)."""
    results = await policies.find({"drug_id": drug_id}).to_list(length=50)
    return {
        "drug_id": drug_id,
        "policies": [_serialize(r) for r in results],
        "count": len(results),
    }


@router.get("/{policy_id}")
async def get_policy(policy_id: str):
    try:
        oid = ObjectId(policy_id)
    except InvalidId:
        raise HTTPException(status_code=400, detail="Invalid policy_id format")

    doc = await policies.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Policy not found")
    return _serialize(doc)
