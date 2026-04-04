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