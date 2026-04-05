"""
Pipeline orchestrator: scans for unprocessed PDFs (from MongoDB stubs and S3),
downloads them, runs extraction, writes results back to MongoDB, and detects
changes against prior versions.

Entry point: run_pipeline()
"""
import hashlib
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path

from bson import ObjectId

from db.mongo import (
    policies,
    policy_versions,
    policy_changelogs,
    extraction_audit_log,
)
from db.s3 import download_pdf, list_all_pdfs
from pipeline.extract import (
    classify_document,
    detect_drug_boundaries,
    detect_headings,
    extract_all,
    extract_policy_record,
    _is_formulary_table,
    _parse_formulary_tables,
    _split_sections,
    segment_sections,
)
from pipeline.diff import diff_policy_records
from pipeline.normalize import normalize_policy_record


# ── Result dataclass ──────────────────────────────────────────────────────────

class PipelineResult:
    def __init__(self):
        self.processed: int = 0
        self.skipped: int = 0
        self.errors: list[dict] = []
        self.changes_detected: int = 0
        self.started_at = datetime.now(timezone.utc)
        self.finished_at: datetime | None = None

    def to_dict(self) -> dict:
        return {
            "processed": self.processed,
            "skipped": self.skipped,
            "errors": self.errors,
            "changes_detected": self.changes_detected,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


# ── Main entry point ──────────────────────────────────────────────────────────

async def run_pipeline() -> PipelineResult:
    result = PipelineResult()
    now = datetime.now(timezone.utc)

    # ── Step 1: Collect work items ────────────────────────────────────────────

    # A) MongoDB stubs waiting for extraction (uploaded via /v1/ingest)
    pending_stubs = await policies.find(
        {"status": "pending_extraction", "s3_key": {"$exists": True}}
    ).to_list(length=500)

    # Build a set of doc_hashes already processed to avoid double-work
    already_done_hashes: set[str] = set()
    async for doc in policies.find(
        {"status": {"$in": ["normalized", "extracted"]}, "doc_hash": {"$exists": True}},
        {"doc_hash": 1}
    ):
        if doc.get("doc_hash"):
            already_done_hashes.add(doc["doc_hash"])

    # B) S3 objects not in MongoDB at all (externally added)
    all_s3 = list_all_pdfs()
    stub_s3_keys = {s["s3_key"] for s in pending_stubs}
    extra_s3 = [
        obj for obj in all_s3
        if obj["s3_key"] not in stub_s3_keys
        and obj.get("doc_hash") not in already_done_hashes
    ]

    # Combine: stubs first, then external S3 objects
    work_items: list[dict] = []
    for stub in pending_stubs:
        work_items.append({
            "mongo_id": stub["_id"],
            "s3_key": stub["s3_key"],
            "doc_hash": stub.get("doc_hash"),
            "payer_hint": stub.get("payer", ""),
            "filename": stub.get("filename", stub["s3_key"].split("/")[-1]),
            "source": "stub",
        })
    for obj in extra_s3:
        work_items.append({
            "mongo_id": None,
            "s3_key": obj["s3_key"],
            "doc_hash": obj.get("doc_hash"),
            "payer_hint": obj.get("payer", ""),
            "filename": obj["s3_key"].split("/")[-1],
            "source": "s3_scan",
        })

    if not work_items:
        result.finished_at = datetime.now(timezone.utc)
        await _log_run(result, now)
        return result

    # ── Step 2: Process each PDF ──────────────────────────────────────────────
    for item in work_items:
        s3_key = item["s3_key"]
        filename = item["filename"]
        print(f"[pipeline] processing: {filename}")

        try:
            await _process_one(item, result)
        except Exception as exc:
            tb = traceback.format_exc()
            print(f"[pipeline] error on {filename}: {exc}\n{tb}")
            result.errors.append({"s3_key": s3_key, "error": str(exc)})
            # Mark stub as failed so it doesn't loop forever
            if item.get("mongo_id"):
                await policies.update_one(
                    {"_id": item["mongo_id"]},
                    {"$set": {"status": "extraction_failed", "error": str(exc)}}
                )
            await extraction_audit_log.insert_one({
                "event": "extraction_failed",
                "s3_key": s3_key,
                "error": str(exc),
                "timestamp": datetime.now(timezone.utc),
            })

    result.finished_at = datetime.now(timezone.utc)
    await _log_run(result, now)
    return result


# ── Process a single PDF ──────────────────────────────────────────────────────

async def _process_one(item: dict, result: PipelineResult) -> None:
    s3_key = item["s3_key"]
    filename = item["filename"]
    now = datetime.now(timezone.utc)

    # Download PDF bytes
    pdf_bytes = download_pdf(s3_key)
    doc_hash = hashlib.sha256(pdf_bytes).hexdigest()

    # Skip if already extracted (e.g. race condition or duplicate S3 key)
    existing_extracted = await policies.find_one(
        {"doc_hash": doc_hash, "status": {"$in": ["normalized", "extracted"]}}
    )
    if existing_extracted:
        print(f"  [skip] already extracted: {filename}")
        result.skipped += 1
        if item.get("mongo_id"):
            await policies.update_one(
                {"_id": item["mongo_id"]},
                {"$set": {"status": "skipped_duplicate", "doc_hash": doc_hash}}
            )
        return

    # Write to temp file — extraction functions need a file path
    import os as _os
    file_ext = _os.path.splitext(filename)[1].lower() or ".pdf"
    with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp.flush()
        tmp_path = tmp.name

    try:
        extracted_records = _extract_from_path(tmp_path, filename)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    if not extracted_records:
        raise RuntimeError("Extraction returned no records")

    # Normalize each extracted record before writing to the golden layer.
    for extracted_record in extracted_records:
        normalized_record = normalize_policy_record(
            extracted_record,
            source_filename=filename,
        ).model_dump(mode="json", exclude_none=True)

        drug_id = _normalized_drug_id(normalized_record)
        payer_canonical = (normalized_record.get("payer") or {}).get(
            "name",
            item["payer_hint"],
        )
        drug_display = _drug_display(normalized_record)

        # Check for a prior normalized version of the same (drug, payer)
        prior = await policies.find_one(
            {
                "drug_id": drug_id,
                "payer_canonical": payer_canonical,
                "status": {"$in": ["normalized", "extracted"]},
            }
        )

        # Archive old version
        version_number = 1
        if prior:
            version_number = (prior.get("version", 1) or 1) + 1
            await policy_versions.insert_one({
                "original_id": prior["_id"],
                "drug_id": drug_id,
                "payer_canonical": payer_canonical,
                "version": prior.get("version", 1),
                "policy_record": prior.get("policy_record"),
                "doc_hash": prior.get("doc_hash"),
                "archived_at": now,
            })

            # Diff old vs new
            changes = diff_policy_records(
                old=prior.get("policy_record") or {},
                new=normalized_record,
                payer=payer_canonical,
                drug=drug_display,
                drug_id=drug_id,
            )
            if changes:
                await policy_changelogs.insert_many(changes)
                result.changes_detected += len(changes)
                print(f"  [diff] {len(changes)} changes detected for {payer_canonical}/{drug_id}")

        new_doc = {
            "status": "normalized",
            "drug_id": drug_id,
            "payer_canonical": payer_canonical,
            "filename": filename,
            "doc_hash": doc_hash,
            "s3_key": s3_key,
            "policy_record": normalized_record,
            "normalized_at": now,
            "version": version_number,
            "source": item["source"],
        }

        if item.get("mongo_id") and len(extracted_records) == 1:
            # Update the existing stub
            await policies.replace_one({"_id": item["mongo_id"]}, new_doc)
        elif prior:
            # Replace the prior extracted doc
            await policies.replace_one({"_id": prior["_id"]}, new_doc)
        else:
            # New doc (omnibus secondary drug, or externally added PDF)
            await policies.insert_one(new_doc)

        result.processed += 1
        print(f"  [ok] {payer_canonical} / {drug_id} (v{version_number})")

    await extraction_audit_log.insert_one({
        "event": "normalization_complete",
        "s3_key": s3_key,
        "filename": filename,
        "doc_hash": doc_hash,
        "records_written": len(extracted_records),
        "timestamp": now,
    })


# ── Extraction dispatch ───────────────────────────────────────────────────────

def _extract_from_path(pdf_path: str, filename: str) -> list[dict]:
    """
    Run the full extraction pipeline on a local file path.
    Returns a list of PolicyRecord dicts (one per drug for omnibus docs).
    """
    blocks, tables, page_count = extract_all(pdf_path)
    blocks = detect_headings(blocks)
    doc_format = classify_document(blocks, page_count)

    print(
        f"  format={doc_format['type']}  pages={page_count}  "
        f"headings={doc_format['heading_count']}  tables={len(tables)}"
    )

    results: list[dict] = []

    if doc_format["type"] == "omnibus":
        drug_slices = detect_drug_boundaries(blocks)
        known_slices = [s for s in drug_slices if s["drug"] != "unknown"]

        if known_slices:
            # Per-drug extraction from omnibus PA doc
            all_sections = segment_sections(blocks)
            for drug_slice in known_slices:
                drug_sections = [
                    s for s in all_sections
                    if s.get("drug_context") == drug_slice["drug"]
                ]
                if not drug_sections:
                    drug_sections = segment_sections(drug_slice["blocks"])
                record = _extract_chunked_record(drug_sections, filename, drug_slice["drug"])
                if record:
                    results.append(record)
            return results

        if _is_formulary_table(tables):
            # Formulary/MDL list — no LLM needed
            formulary = _parse_formulary_tables(tables, filename)
            return [formulary]

    # Per-drug or flat doc
    sections = segment_sections(blocks)
    record = _extract_chunked_record(sections, filename)
    if record:
        results.append(record)
    return results


def _extract_chunked_record(
    sections: list[dict],
    filename: str,
    drug_hint: str | None = None,
) -> dict | None:
    """
    Extract a PolicyRecord from sections, splitting into chunks if needed.
    Returns the merged dict (not Pydantic model) or None on failure.
    """
    chunks = _split_sections(sections)

    if len(chunks) == 1:
        try:
            record, _output_tokens = extract_policy_record(
                sections, filename, drug_hint=drug_hint
            )
            return record.model_dump(exclude_none=True)
        except Exception as e:
            print(f"  [error] extraction failed: {e}")
            return None

    # Multi-chunk: merge indications and exclusions
    import json as _json
    base: dict | None = None
    all_indications: list = []
    all_exclusions: list = []

    for chunk in chunks:
        try:
            record, _output_tokens = extract_policy_record(
                chunk, filename, drug_hint=drug_hint
            )
            partial = record.model_dump(exclude_none=True)
        except Exception as e:
            print(f"  [warn] chunk failed: {e}")
            continue

        if base is None:
            base = partial
        all_indications.extend(partial.get("indications") or [])
        all_exclusions.extend(partial.get("exclusions") or [])

    if base is None:
        return None

    base["indications"] = all_indications
    base["exclusions"] = all_exclusions
    return base


# ── Helpers ───────────────────────────────────────────────────────────────────

def _drug_display(record: dict) -> str:
    drug = record.get("drug") or {}
    brand = drug.get("brand_name", "")
    generic = drug.get("generic_name", "")
    if brand and generic:
        return f"{brand} ({generic})"
    return brand or generic or "Unknown drug"


def _normalized_drug_id(record: dict) -> str:
    drug = record.get("drug") or {}
    generic = str(drug.get("normalized_generic_name") or "").strip().lower()
    if generic:
        return generic

    display = str(drug.get("display_name") or "").strip().lower()
    if display:
        return display

    return "unknown"


async def _log_run(result: PipelineResult, started_at: datetime) -> None:
    await extraction_audit_log.insert_one({
        "event": "pipeline_run",
        "processed": result.processed,
        "skipped": result.skipped,
        "errors": len(result.errors),
        "changes_detected": result.changes_detected,
        "started_at": started_at,
        "finished_at": result.finished_at,
        "timestamp": result.finished_at or datetime.now(timezone.utc),
    })
