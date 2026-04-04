from fastapi import APIRouter

router = APIRouter()

@router.get("/{policy_id}")
async def diff(policy_id: str, from_version: int = 1, to_version: int = 2):
    return {"diff": [], "policy_id": policy_id}
