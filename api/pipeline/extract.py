import contextlib
import io
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
import anthropic

import pymupdf


def extract_text_blocks(pdf_path: str) -> list[dict]:
    """PyMuPDF | font/position metadata. Primary extractor."""
    doc = pymupdf.open(pdf_path)
    blocks = []
    for page_num, page in enumerate(doc):
        for block in page.get_text("dict")["blocks"]:
            if block["type"] == 0:
                blocks.append(
                    {
                        "page_num": page_num + 1,
                        "bbox": block["bbox"],
                        "x0": block["bbox"][0],
                        "text": " ".join(
                            span["text"]
                            for line in block["lines"]
                            for span in line["spans"]
                        ),
                        "font_size": block["lines"][0]["spans"][0]["size"],
                        "is_bold": "Bold" in block["lines"][0]["spans"][0]["font"],
                    }
                )
    return blocks


def extract_with_pdfplumber(pdf_path: str) -> list[dict]:
    """
    pdfplumber | Falls back to this when PyMuPDF yield is low.
    """
    import pdfplumber

    blocks = []
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            words = page.extract_words(
                extra_attrs=["fontname", "size"],
                use_text_flow=True,
            )
            if not words:
                continue

            # Group words into lines (within 3pt vertically)
            lines: list[list[dict]] = []
            for word in words:
                if lines and abs(word["top"] - lines[-1][0]["top"]) < 3:
                    lines[-1].append(word)
                else:
                    lines.append([word])

            # Group lines into blocks (gap > 12pt = new block)
            block_groups: list[list[list[dict]]] = []
            for line in lines:
                if (
                    block_groups
                    and abs(line[0]["top"] - block_groups[-1][-1][0]["top"]) < 12
                ):
                    block_groups[-1].append(line)
                else:
                    block_groups.append([line])

            for bg in block_groups:
                all_words = [w for line in bg for w in line]
                first = all_words[0]
                blocks.append(
                    {
                        "page_num": page_num + 1,
                        "bbox": (
                            min(w["x0"] for w in all_words),
                            min(w["top"] for w in all_words),
                            max(w["x1"] for w in all_words),
                            max(w["bottom"] for w in all_words),
                        ),
                        "x0": min(w["x0"] for w in all_words),
                        "text": " ".join(w["text"] for w in all_words),
                        "font_size": first.get("size", 10.0),
                        "is_bold": "bold" in first.get("fontname", "").lower(),
                    }
                )
    return blocks


def extract_tables(pdf_path: str) -> list[dict]:
    """
    Camelot | table extraction.
    """
    try:
        import camelot

        tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
        results = [
            {
                "page": t.page,
                "data": t.df.to_dict(orient="records"),
                "accuracy": t.accuracy,
            }
            for t in tables
            if t.accuracy > 70
        ]
        if not results:
            tables = camelot.read_pdf(pdf_path, pages="all", flavor="stream")
            results = [
                {
                    "page": t.page,
                    "data": t.df.to_dict(orient="records"),
                    "accuracy": t.accuracy,
                }
                for t in tables
                if t.accuracy > 70
            ]
        return results
    except Exception:
        return []


def ocr_fallback(pdf_path: str) -> list[dict]:
    """
    Last resort for scanned PDFs where both PyMuPDF and pdfplumber return near-zero text.
    Requires: pip install pdf2image pytesseract  (+ system Tesseract binary)
    """
    try:
        import pytesseract
        from pdf2image import convert_from_path
    except ImportError:
        raise RuntimeError(
            "OCR dependencies missing. Install with: pip install pdf2image pytesseract\n"
            "Also ensure the Tesseract binary is installed on your system."
        )

    images = convert_from_path(pdf_path, dpi=300)
    blocks = []
    for page_num, image in enumerate(images):
        text = pytesseract.image_to_string(image).strip()
        if text:
            blocks.append(
                {
                    "page_num": page_num + 1,
                    "bbox": (0, 0, image.width, image.height),
                    "x0": 0,
                    "text": text,
                    "font_size": 10.0,  # unknown from OCR
                    "is_bold": False,  # unknown from OCR
                }
            )
    return blocks


_LOW_YIELD_THRESHOLD = 50


def extract_all(pdf_path: str) -> tuple[list[dict], list[dict], int]:
    """
    Orchestrates multi-library extraction.
    Returns (text_blocks, tables, page_count).
    """
    doc = pymupdf.open(pdf_path)
    page_count = len(doc)
    doc.close()

    blocks = extract_text_blocks(pdf_path)
    total_chars = sum(len(b["text"].strip()) for b in blocks)
    chars_per_page = total_chars / max(page_count, 1)

    if chars_per_page < _LOW_YIELD_THRESHOLD:
        plumber_blocks = extract_with_pdfplumber(pdf_path)
        plumber_chars = sum(len(b["text"].strip()) for b in plumber_blocks)

        if plumber_chars > total_chars:
            blocks = plumber_blocks

        if plumber_chars / max(page_count, 1) < _LOW_YIELD_THRESHOLD:
            print(
                f"  [warn] Low text yield ({chars_per_page:.0f} chars/page) — routing to OCR"
            )
            blocks = ocr_fallback(pdf_path)

    tables = extract_tables(pdf_path)
    return blocks, tables, page_count


# Seed list of common medical benefit drug names (brand + generic).
# In production, build this from the HCPCS flat file + extracted corpus.
# This only needs to be a classification signal, not exhaustive.
KNOWN_DRUG_NAMES = {
    # Dermatology / Allergy
    "dupilumab",
    "dupixent",
    "tralokinumab",
    "adbry",
    "lebrikizumab",
    "ebglyss",
    "nemolizumab",
    "nemluvio",
    "omalizumab",
    "xolair",
    # Rheumatology / Immunology
    "adalimumab",
    "humira",
    "hadlima",
    "hyrimoz",
    "abrilada",
    "cyltezo",
    "etanercept",
    "enbrel",
    "erelzi",
    "eticovo",
    "infliximab",
    "remicade",
    "inflectra",
    "renflexis",
    "avsola",
    "tocilizumab",
    "actemra",
    "sarilumab",
    "kevzara",
    "upadacitinib",
    "rinvoq",
    "tofacitinib",
    "xeljanz",
    "abrocitinib",
    "cibinqo",
    "secukinumab",
    "cosentyx",
    "ixekizumab",
    "taltz",
    "bimekizumab",
    "bimzelx",
    "guselkumab",
    "tremfya",
    "risankizumab",
    "skyrizi",
    "tildrakizumab",
    "ilumya",
    "ustekinumab",
    "stelara",
    # Asthma / Pulmonology
    "mepolizumab",
    "nucala",
    "benralizumab",
    "fasenra",
    "reslizumab",
    "cinqair",
    "tezepelumab",
    "tezspire",
    # Oncology
    "rituximab",
    "rituxan",
    "truxima",
    "ruxience",
    "bevacizumab",
    "avastin",
    "mvasi",
    "zirabev",
    "trastuzumab",
    "herceptin",
    "ogivri",
    "herzuma",
    "pembrolizumab",
    "keytruda",
    "nivolumab",
    "opdivo",
    "atezolizumab",
    "tecentriq",
    "ipilimumab",
    "yervoy",
    "cetuximab",
    "erbitux",
    "ramucirumab",
    "cyramza",
    # Gastroenterology
    "vedolizumab",
    "entyvio",
    # Neurology
    "natalizumab",
    "tysabri",
    "ocrelizumab",
    "ocrevus",
    "ofatumumab",
    "kesimpta",
    # Ophthalmology
    "ranibizumab",
    "lucentis",
    "aflibercept",
    "eylea",
    "faricimab",
    "vabysmo",
    # Bone / Endocrine
    "denosumab",
    "prolia",
    "xgeva",
    "romosozumab",
    "evenity",
    # Neurotoxins
    "onabotulinumtoxina",
    "botox",
    "abobotulinumtoxina",
    "dysport",
    "incobotulinumtoxina",
    "xeomin",
    "rimabotulinumtoxinb",
    "myobloc",
}


def classify_document(blocks: list[dict], page_count: int) -> dict:
    """
    | Format   | Characteristic                                          |
    |----------|---------------------------------------------------------|
    | per_drug | Short (≤40 pages), title names a single drug            |
    | omnibus  | Long (>40 pages) OR TOC contains 3+ drug names         |
    | flat     | Fewer than 3 distinct heading blocks (continuous prose) |
    """
    first_page_text = " ".join(b["text"] for b in blocks if b["page_num"] == 1).lower()

    toc_text = " ".join(
        b["text"]
        for b in blocks
        if b["page_num"] <= 3 and b["is_bold"] and len(b["text"]) < 120
    ).lower()

    drug_hits_title = sum(1 for d in KNOWN_DRUG_NAMES if d in first_page_text)
    drug_hits_toc = sum(1 for d in KNOWN_DRUG_NAMES if d in toc_text)
    heading_count = sum(1 for b in blocks if b.get("heading_level") is not None)

    if heading_count < 3:
        doc_type = "flat"
    elif page_count > 40 or drug_hits_toc >= 3:
        doc_type = "omnibus"
    else:
        doc_type = "per_drug"

    if drug_hits_title >= 1 and page_count <= 40:
        doc_type = "per_drug"

    return {
        "type": doc_type,
        "page_count": page_count,
        "drug_hits_title": drug_hits_title,
        "drug_hits_toc": drug_hits_toc,
        "heading_count": heading_count,
    }


def detect_drug_boundaries(blocks: list[dict]) -> list[dict]:
    """Finds top-level headings that name a known drug"""
    slices: list[dict] = []
    current_drug: str | None = None
    current_start = 0
    unmatched_headings: list[str] = []

    for i, block in enumerate(blocks):
        if block.get("heading_level") != 1:
            continue
        heading = block["text"].strip()
        matched = next((d for d in KNOWN_DRUG_NAMES if d in heading.lower()), None)
        if matched:
            if current_drug is not None:
                slices.append({"drug": current_drug, "blocks": blocks[current_start:i]})
            current_drug = matched
            current_start = i
        else:
            unmatched_headings.append(heading)

    if current_drug is not None:
        slices.append({"drug": current_drug, "blocks": blocks[current_start:]})

    if not slices:
        slices = [{"drug": "unknown", "blocks": blocks}]

    if unmatched_headings:
        print(
            f"  [warn] {len(unmatched_headings)} unmatched H1 headings in omnibus doc "
            f"(potential missed drugs): {unmatched_headings[:5]}"
        )

    return slices


def detect_headings(blocks: list[dict]) -> list[dict]:
    """
    Classify blocks as headings using absolute left-margin thresholding.
    """
    body_sizes = [
        b["font_size"] for b in blocks if not b["is_bold"] and b["text"].strip()
    ]
    body_median = statistics.median(body_sizes) if body_sizes else 10.0

    # Estimate page width from the rightmost x1 coordinate seen across all blocks
    page_width = max((b["bbox"][2] for b in blocks if b.get("bbox")), default=612.0)
    center_guard = page_width * 0.45  # blocks right of this are centered/right-aligned

    # Candidate pool: bold, short, and not right-of-center
    candidates = [
        b
        for b in blocks
        if b["is_bold"]
        and b["text"].strip()
        and len(b["text"].strip()) < 120
        and b["x0"] < center_guard
    ]

    if not candidates:
        for b in blocks:
            b["heading_level"] = None
        return blocks

    sorted_x0s = sorted(b["x0"] for b in candidates)
    left_margin = sorted_x0s[max(0, len(sorted_x0s) // 10)]

    HEADING_MAX_INDENT = 60

    for block in blocks:
        text = block["text"].strip()

        if (
            not block["is_bold"]
            or not text
            or len(text) > 120
            or block["x0"] >= center_guard  # centered/right-aligned
            or block["x0"] > left_margin + HEADING_MAX_INDENT  # too indented
        ):
            block["heading_level"] = None
            continue

        size_ratio = block["font_size"] / body_median
        if size_ratio >= 1.15 or block["x0"] <= left_margin + 5:
            block["heading_level"] = 1
        else:
            block["heading_level"] = 2

    return blocks


def segment_sections(blocks: list[dict]) -> list[dict]:
    sections: list[dict] = []
    current: dict = {"heading": "__preamble__", "level": 0, "page": 1, "content": []}

    for block in blocks:
        if block.get("heading_level") is not None:
            sections.append(
                {**current, "content": "\n".join(current["content"]).strip()}
            )
            current = {
                "heading": block["text"].strip(),
                "level": block["heading_level"],
                "page": block["page_num"],
                "content": [],
            }
        else:
            text = block["text"].strip()
            if text:
                current["content"].append(text)

    sections.append({**current, "content": "\n".join(current["content"]).strip()})
    return _prune_sections(sections)


def _prune_sections(sections: list[dict]) -> list[dict]:
    """
    Drop sections with negligible content (page numbers, footers, copyright lines)
    """
    return [s for s in sections if len(s["content"].strip()) > 50]


from .schema import POLICY_RECORD_SCHEMA


def _render_sections(sections: list[dict]) -> str:
    """Format segmented sections into a single prompt-ready string."""
    parts = []
    for s in sections:
        parts.append(f"\n[SECTION: {s['heading']} | page {s['page']}]\n{s['content']}")
    return "\n".join(parts)


def extract_policy_record(sections: list[dict], source_filename: str) -> dict:
    """
    Send pre-segmented document text to Claude and return a structured
    PolicyRecord conforming to schema.POLICY_RECORD_SCHEMA.

    model: claude-haiku-4-5-20251001
    """
    client = anthropic.Anthropic()

    response = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=4096,
        tools=[
            {
                "name": "extract_policy_record",
                "description": (
                    "Extract structured prior authorization policy data from a health plan document. "
                    "Capture every covered indication and all PA criteria exactly as stated. "
                    "Do NOT hallucinate. Omit fields absent from the document rather than guessing."
                ),
                "input_schema": POLICY_RECORD_SCHEMA,
            }
        ],
        tool_choice={"type": "tool", "name": "extract_policy_record"},
        messages=[
            {
                "role": "user",
                "content": (
                    f"Extract structured policy data from this health plan PA policy document: {source_filename}\n\n"
                    "The document has been pre-segmented — each [SECTION] block contains raw text "
                    "from that portion of the document.\n\n"
                    f"{_render_sections(sections)}"
                ),
            }
        ],
    )

    for block in response.content:
        if block.type == "tool_use" and block.name == "extract_policy_record":
            return block.input

    raise ValueError("LLM did not return a tool_use block")


class _Tee(io.TextIOBase):
    """Mirrors writes to both a real stream and a file."""

    def __init__(self, real_stream, log_file):
        self._real = real_stream
        self._log = log_file

    def write(self, s):
        self._real.write(s)
        self._log.write(s)
        return len(s)

    def flush(self):
        self._real.flush()
        self._log.flush()


@contextlib.contextmanager
def _run_log(logs_dir: Path, keep: int = 3):
    """Context manager that tees stdout to a timestamped log file in logs_dir."""
    logs_dir.mkdir(exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"run_{ts}.log"

    with open(log_path, "w") as log_file:
        tee = _Tee(sys.__stdout__, log_file)
        old_stdout = sys.stdout
        sys.stdout = tee
        try:
            yield log_path
        finally:
            sys.stdout = old_stdout

    # Prune: keep only the `keep` most recent log files
    logs = sorted(logs_dir.glob("run_*.log"), key=lambda p: p.stat().st_mtime)
    for old in logs[:-keep]:
        old.unlink()


_PROJECT_ROOT = Path(__file__).parent.parent.parent


def main():
    with _run_log(_PROJECT_ROOT / ".logs"):
        _run()


def _run():
    input_dir = _PROJECT_ROOT / "policy_data"
    sections_dir = _PROJECT_ROOT / "outputs" / "sections"
    records_dir = _PROJECT_ROOT / "outputs" / "policy_records"
    sections_dir.mkdir(parents=True, exist_ok=True)
    records_dir.mkdir(parents=True, exist_ok=True)

    for pdf_file in sorted(input_dir.glob("*.pdf")):
        print(f"\nProcessing: {pdf_file.name}")

        blocks, tables, page_count = extract_all(str(pdf_file))
        blocks = detect_headings(blocks)
        doc_format = classify_document(blocks, page_count)

        print(
            f"  format={doc_format['type']}  pages={page_count}  "
            f"headings={doc_format['heading_count']}  tables={len(tables)}"
        )

        if doc_format["type"] == "omnibus":
            drug_slices = detect_drug_boundaries(blocks)
            all_sections = []
            for drug_slice in drug_slices:
                slice_sections = segment_sections(drug_slice["blocks"])
                for s in slice_sections:
                    s["drug_context"] = drug_slice["drug"]
                all_sections.extend(slice_sections)
        else:
            all_sections = segment_sections(blocks)

        # Write raw segmentation output for inspection
        sections_out = sections_dir / f"{pdf_file.stem}.json"
        with open(sections_out, "w") as f:
            json.dump(
                {
                    "source": pdf_file.name,
                    "document_format": doc_format,
                    "tables": tables,
                    "sections": all_sections,
                },
                f,
                indent=2,
            )
        print(f"  → {len(all_sections)} sections → {sections_out.name}")

        # LLM extraction — retry with backoff on rate limit
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                record = extract_policy_record(all_sections, pdf_file.name)
                record_out = records_dir / f"{pdf_file.stem}.json"
                with open(record_out, "w") as f:
                    json.dump(record, f, indent=2)
                print(f"  → policy record → {record_out.name}")
                break
            except anthropic.RateLimitError:
                if attempt < max_attempts - 1:
                    wait = 60 * (attempt + 1)
                    print(
                        f"  [rate limit] waiting {wait}s before retry {attempt + 1}/{max_attempts}..."
                    )
                    time.sleep(wait)
                else:
                    print(
                        f"  [error] LLM extraction failed after {max_attempts} attempts: rate limit exhausted"
                    )
            except Exception as e:
                print(f"  [error] LLM extraction failed: {e}")
                break


if __name__ == "__main__":
    main()
