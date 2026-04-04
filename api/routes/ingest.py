from fastapi import APIRouter, UploadFile, File

router = APIRouter()

@router.post("/")
async def ingest_pdf(file: UploadFile = File(...)):
    return {"status": "pipeline not yet connected", "filename": file.filename}
