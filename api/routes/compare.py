from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def compare(drug_rxcui: str = "", payers: str = ""):
    return {"comparison": [], "drug_rxcui": drug_rxcui}
