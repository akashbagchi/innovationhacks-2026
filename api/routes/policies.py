from fastapi import APIRouter

router = APIRouter()

@router.get("/search")
async def search(drug: str = "", payer: str = ""):
    return {"results": [], "query": {"drug": drug, "payer": payer}}

@router.get("/{policy_id}")
async def get_policy(policy_id: str):
    return {"policy_id": policy_id, "status": "stub"}
