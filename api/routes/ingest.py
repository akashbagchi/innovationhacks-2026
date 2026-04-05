import hashlib
from datetime import datetime, timezone

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from botocore.exceptions import ClientError

from db.s3 import upload_pdf, hash_exists
from db.mongo import policies, extraction_audit_log

router = APIRouter()


@router.post("/", status_code=201)
async def ingest_pdf(
    file: UploadFile = File(...),
    payer: str = Form(...),
    policy_id: str = Form(...),
):
    """
    Ingest a policy PDF:
      1. Compute SHA-256 hash
      2. Reject duplicates (same hash already in S3 for this payer)
      3. Upload to S3
      4. Write stub document to MongoDB (status=pending_extraction)
      5. Write audit log entry
    """
    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted.")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    doc_hash = hashlib.sha256(pdf_bytes).hexdigest()

    # ── Deduplication check ───────────────────────────────────────────────
    try:
        if hash_exists(doc_hash, payer):
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "duplicate",
                    "message": "This exact PDF has already been ingested for this payer.",
                    "doc_hash": doc_hash,
                    "payer": payer,
                    "policy_id": policy_id,
                },
            )
    except HTTPException:
        raise
    except ClientError as e:
        # S3 unavailable — log and continue rather than blocking ingest
        print(f"[ingest] S3 hash check failed: {e}")

    # ── Upload to S3 ──────────────────────────────────────────────────────
    try:
        s3_result = upload_pdf(pdf_bytes, payer, policy_id)
    except ClientError as e:
        raise HTTPException(status_code=502, detail=f"S3 upload failed: {str(e)}")

    s3_key  = s3_result["s3_key"]
    now_utc = datetime.now(timezone.utc)

    # ── Write stub record to MongoDB ──────────────────────────────────────
    stub = {
        "status": "pending_extraction",
        "payer": payer,
        "policy_id": policy_id,
        "filename": file.filename,
        "doc_hash": doc_hash,
        "s3_key": s3_key,
        "uploaded_at": now_utc,
        "extracted_at": None,
        "policy_record": None,   # populated by extraction pipeline
    }
    insert_result = await policies.insert_one(stub)
    mongo_id = str(insert_result.inserted_id)

    # ── Audit log ─────────────────────────────────────────────────────────
    await extraction_audit_log.insert_one({
        "event": "pdf_uploaded",
        "mongo_id": mongo_id,
        "doc_hash": doc_hash,
        "s3_key": s3_key,
        "payer": payer,
        "policy_id": policy_id,
        "filename": file.filename,
        "timestamp": now_utc,
    })

    return {
        "status": "uploaded",
        "mongo_id": mongo_id,
        "s3_key": s3_key,
        "doc_hash": doc_hash,
        "payer": payer,
        "policy_id": policy_id,
        "filename": file.filename,
        "next_step": "pending_extraction",
    }
