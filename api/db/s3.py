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

def upload_pdf(pdf_bytes: bytes, payer: str, policy_id: str) -> dict:
    doc_hash = hashlib.sha256(pdf_bytes).hexdigest()
    key = f"{payer.lower()}/{policy_id}/sha256-{doc_hash[:16]}.pdf"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=pdf_bytes,
        ContentType="application/pdf",
        Metadata={
            "payer": payer,
            "policy_id": policy_id,
            "doc_hash": doc_hash
        }
    )
    return {"s3_key": key, "doc_hash": doc_hash}

def download_pdf(s3_key: str) -> bytes:
    response = s3.get_object(Bucket=BUCKET, Key=s3_key)
    return response["Body"].read()

def list_all_pdfs() -> list[dict]:
    """
    Return all PDF objects in the bucket as a list of:
      {"s3_key": str, "doc_hash": str | None, "payer": str | None, "size": int}
    Handles pagination automatically.
    """
    results = []
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=BUCKET):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.lower().endswith(".pdf"):
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