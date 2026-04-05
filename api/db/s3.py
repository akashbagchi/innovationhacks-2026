import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os
import hashlib

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "us-east-1")
)

BUCKET = os.getenv("S3_BUCKET_NAME")

_SUPPORTED_EXTENSIONS = {".pdf", ".docx"}
_CONTENT_TYPES = {
    ".pdf": "application/pdf",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


def upload_document(file_bytes: bytes, payer: str, policy_id: str, filename: str = "") -> dict:
    """Upload a policy document (PDF or DOCX) to S3."""
    import os
    ext = os.path.splitext(filename)[1].lower() if filename else ".pdf"
    if ext not in _SUPPORTED_EXTENSIONS:
        ext = ".pdf"  # safe fallback
    doc_hash = hashlib.sha256(file_bytes).hexdigest()
    key = f"{payer.lower()}/{policy_id}/sha256-{doc_hash[:16]}{ext}"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=file_bytes,
        ContentType=_CONTENT_TYPES.get(ext, "application/octet-stream"),
        Metadata={
            "payer": payer,
            "policy_id": policy_id,
            "doc_hash": doc_hash,
        },
    )
    return {"s3_key": key, "doc_hash": doc_hash}


# Keep old name as alias for backward compatibility
def upload_pdf(pdf_bytes: bytes, payer: str, policy_id: str) -> dict:
    return upload_document(pdf_bytes, payer, policy_id, filename="document.pdf")


def download_document(s3_key: str) -> bytes:
    response = s3.get_object(Bucket=BUCKET, Key=s3_key)
    return response["Body"].read()


# Keep old name as alias
def download_pdf(s3_key: str) -> bytes:
    return download_document(s3_key)


def list_all_pdfs() -> list[dict]:
    """
    Return all supported document objects in the bucket as a list of:
      {"s3_key": str, "doc_hash": str | None, "payer": str | None, "size": int}
    Handles pagination automatically.
    """
    results = []
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=BUCKET):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not any(key.lower().endswith(ext) for ext in _SUPPORTED_EXTENSIONS):
                    continue
                # Try to read payer + doc_hash from S3 metadata
                try:
                    head = s3.head_object(Bucket=BUCKET, Key=key)
                    meta = head.get("Metadata", {})
                    doc_hash = meta.get("doc_hash") or _hash_from_key(key)
                    payer = meta.get("payer") or key.split("/")[0]
                except ClientError:
                    doc_hash = _hash_from_key(key)
                    payer = key.split("/")[0]
                results.append({
                    "s3_key": key,
                    "doc_hash": doc_hash,
                    "payer": payer,
                    "size": obj.get("Size", 0),
                })
    except ClientError:
        pass
    return results


def generate_presigned_url(s3_key: str, expiry_seconds: int = 3600) -> str:
    """Generate a presigned S3 URL to view/download a stored document."""
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": s3_key},
        ExpiresIn=expiry_seconds,
    )


def _hash_from_key(key: str) -> str | None:
    """Extract doc_hash from key pattern sha256-{hash[:16]}.pdf"""
    import re
    m = re.search(r"sha256-([a-f0-9]+)", key)
    return m.group(1) if m else None


def hash_exists(doc_hash: str, payer: str) -> bool:
    """Check if we've already ingested this exact PDF."""
    try:
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=f"{payer.lower()}/",
        )
        for obj in response.get("Contents", []):
            if doc_hash[:16] in obj["Key"]:
                return True
        return False
    except ClientError:
        return False