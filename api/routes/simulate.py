from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class PatientProfile(BaseModel):
    drug: str
    payer: str
    diagnoses: list[str]
    prior_treatments: list[str]

@router.post("/")
async def simulate(profile: PatientProfile):
    return {"decision": "stub", "profile": profile}
