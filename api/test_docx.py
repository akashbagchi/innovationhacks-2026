"""
One-off test: download the EmblemHealth DOCX from S3 and run extraction.
Usage (inside container): python test_docx.py
"""
import json
import tempfile
from pathlib import Path

from db.s3 import download_document
from pipeline.extract import (
    extract_all,
    detect_headings,
    classify_document,
    segment_sections,
    extract_policy_record,
)

S3_KEY = "EmblemHealth_MPS_Denosumab_11_25_hcpcs.docx"

print(f"Downloading {S3_KEY} ...")
file_bytes = download_document(S3_KEY)
print(f"  {len(file_bytes):,} bytes")

with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
    tmp.write(file_bytes)
    tmp.flush()
    tmp_path = tmp.name

try:
    print("Extracting blocks ...")
    blocks, tables, page_count = extract_all(tmp_path)
    print(f"  {len(blocks)} blocks, {page_count} approx pages")

    blocks = detect_headings(blocks)
    doc_format = classify_document(blocks, page_count)
    print(f"  format={doc_format['type']}  headings={doc_format['heading_count']}")

    sections = segment_sections(blocks)
    print(f"  {len(sections)} sections after pruning")
    for s in sections[:5]:
        print(f"    [{s['heading']}] {s['content'][:80]!r}")

    print("\nRunning LLM extraction ...")
    record, output_tokens = extract_policy_record(sections, S3_KEY)
    print(f"  output_tokens={output_tokens}")
    result = record.model_dump(exclude_none=True)
    print(json.dumps(result, indent=2))

finally:
    Path(tmp_path).unlink(missing_ok=True)
