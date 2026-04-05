from fastapi import APIRouter

from db.mongo import policies

router = APIRouter()


@router.get("/")
async def compare(drug: str = "", payers: str = ""):
    """
    Return all payer policy records for a drug, optionally filtered by payers
    (comma-separated payer names).
    """
    query: dict = {}
    if drug:
        query["drug_id"] = {"$regex": drug, "$options": "i"}
    if payers:
        payer_list = [p.strip() for p in payers.split(",") if p.strip()]
        query["payer_canonical"] = {"$in": payer_list}

    docs = await policies.find(query).to_list(length=50)

    return {
        "drug": drug,
        "count": len(docs),
        "payers": [
            {
                "payer": d["payer_canonical"],
                "drug_id": d.get("drug_id"),
                "mongo_id": str(d["_id"]),
                "policy_record": d.get("policy_record"),
            }
            for d in docs
        ],
    }
