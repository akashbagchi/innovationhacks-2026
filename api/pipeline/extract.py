import contextlib
import io
import json
import collections
import re
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
import anthropic
import instructor

import pymupdf

_HCPCS_RE = re.compile(r"\b([A-Z]\d{4}|\d{5})\b")
_COVERAGE_RE = re.compile(
    r"(Non-Specialty|Specialty\s+with\s+PA|Specialty|Not\s+Covered|Non-Covered|Covered)",
    re.IGNORECASE,
)
_TABLE_HEADER_RE = re.compile(
    r"HCPCS|CPT.?Code|Drug.?Name|Coverage.?Level", re.IGNORECASE
)


def _clean(text: str) -> str:
    """Normalize whitespace from tab/newline-heavy Camelot output."""
    return re.sub(r"\s+", " ", text).strip()


def _is_formulary_table(tables: list[dict]) -> bool:
    """Return True if the extracted tables look like an HCPCS formulary list."""
    if not tables:
        return False
    hcpcs_rows = 0
    for table in tables[:10]:
        for row in table.get("data", []):
            col0 = _clean(str(row.get("0", "")))
            if _HCPCS_RE.match(col0):
                hcpcs_rows += 1
    return hcpcs_rows >= 5


def _parse_formulary_tables(tables: list[dict], source_filename: str) -> dict:
    """
    Parse HCPCS formulary tables (MDL-style docs) into a structured coverage list.
    """
    drugs: list[dict] = []
    seen_codes: set[str] = set()
    current_category = "Unknown"

    for table in tables:
        for row in table.get("data", []):
            col0 = _clean(str(row.get("0", "")))
            col1 = _clean(str(row.get("1", "")))
            col2 = _clean(str(row.get("2", "")))
            col3 = _clean(str(row.get("3", "")))
            col4 = _clean(str(row.get("4", "")))

            if not col0:
                continue

            if _TABLE_HEADER_RE.search(col0):
                continue

            if not col1 and not col2 and not col3:
                hcpcs_match = _HCPCS_RE.search(col0)
                if not hcpcs_match:
                    current_category = col0
                    continue
                hcpcs_code = hcpcs_match.group(1)
                coverage_match = _COVERAGE_RE.search(col0)
                coverage_level = coverage_match.group(1) if coverage_match else ""
                pre_code = col0[: hcpcs_match.start()].strip()
                post_code = col0[hcpcs_match.end() :].strip()
                if coverage_match:
                    desc_end = col0.find(coverage_match.group(1))
                    description = _clean(col0[hcpcs_match.end() : desc_end])
                else:
                    description = post_code
                if hcpcs_code not in seen_codes:
                    seen_codes.add(hcpcs_code)
                    drugs.append(
                        {
                            "hcpcs_code": hcpcs_code,
                            "drug_name": pre_code,
                            "description": description,
                            "coverage_level": coverage_level,
                            "category": current_category,
                            "notes": "",
                        }
                    )
                continue

            hcpcs_match = _HCPCS_RE.match(col0)
            if hcpcs_match and col0 not in seen_codes:
                seen_codes.add(col0)
                drugs.append(
                    {
                        "hcpcs_code": col0,
                        "drug_name": col1,
                        "description": col2,
                        "coverage_level": col3,
                        "category": current_category,
                        "notes": col4,
                    }
                )

    base = (
        source_filename.split(" - ")[0].strip()
        if " - " in source_filename
        else source_filename
    )
    payer_name = re.sub(r"\s+\d{4}.*$", "", base).strip() or base

    return {
        "document_type": "formulary_list",
        "source": source_filename,
        "payer": {"name": payer_name, "policy_title": source_filename},
        "drugs": drugs,
    }


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


def extract_docx_blocks(docx_path: str) -> list[dict]:
    """
    python-docx | Extract text blocks from a Word document.
    Approximates page numbers based on paragraph count (~50 paras/page).
    """
    from docx import Document as DocxDocument

    try:
        doc = DocxDocument(docx_path)
    except Exception as e:
        print(f"  [warn] DOCX parse failed ({e}), skipping file")
        return []

    blocks = []
    para_count = 0
    PARAS_PER_PAGE = 50

    for para in doc.paragraphs:
        text = para.text.strip()
        if not text:
            continue

        para_count += 1
        page_num = (para_count // PARAS_PER_PAGE) + 1

        # Derive font size and bold from runs
        font_size = 10.0
        is_bold = False
        for run in para.runs:
            if run.font.size:
                font_size = run.font.size.pt
            if run.bold:
                is_bold = True

        # Word heading styles are reliable signals
        style_name = para.style.name.lower() if para.style else ""
        if "heading" in style_name:
            is_bold = True
            if "1" in style_name:
                font_size = max(font_size, 14.0)
            elif "2" in style_name:
                font_size = max(font_size, 12.0)

        blocks.append({
            "page_num": page_num,
            "bbox": (0.0, para_count * 12.0, 500.0, (para_count + 1) * 12.0),
            "x0": 0.0,
            "text": text,
            "font_size": font_size,
            "is_bold": is_bold,
        })

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
                "data": t.df.rename(columns=str).to_dict(orient="records"),
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
                    "data": t.df.rename(columns=str).to_dict(orient="records"),
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
                    "font_size": 10.0,
                    "is_bold": False,
                }
            )
    return blocks


_LOW_YIELD_THRESHOLD = 50


def _sniff_doctype(path: str) -> str:
    """Return 'pdf' or 'docx' based on file magic bytes, falling back to extension."""
    try:
        with open(path, "rb") as f:
            header = f.read(8)
        if header[:4] == b"%PDF":
            return "pdf"
        if header[:2] == b"PK":  # ZIP-based — DOCX, XLSX, etc.
            return "docx"
    except OSError:
        pass
    ext = Path(path).suffix.lower()
    return "docx" if ext == ".docx" else "pdf"


def extract_all(pdf_path: str) -> tuple[list[dict], list[dict], int]:
    """
    Orchestrates multi-library extraction. Supports PDF and DOCX.
    Detects actual file type from magic bytes — not filename extension.
    """
    if _sniff_doctype(pdf_path) == "docx":
        blocks = extract_docx_blocks(pdf_path)
        page_count = max((b["page_num"] for b in blocks), default=1)
        return blocks, [], page_count  # Camelot table extraction not applicable to docx

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


from .models import PolicyRecord


def _render_sections(sections: list[dict]) -> str:
    """Format segmented sections into a single prompt-ready string."""
    parts = []
    for s in sections:
        parts.append(f"\n[SECTION: {s['heading']} | page {s['page']}]\n{s['content']}")
    return "\n".join(parts)


def extract_policy_record(
    sections: list[dict], source_filename: str, drug_hint: str | None = None
) -> tuple[PolicyRecord, int]:
    """
    Send pre-segmented document text to Claude and return a validated PolicyRecord.
    """
    client = instructor.from_anthropic(anthropic.Anthropic())

    drug_scope = (
        f"\nFocus ONLY on the drug: {drug_hint}. Ignore PA criteria for any other drug.\n"
        if drug_hint
        else ""
    )

    record, completion = client.messages.create_with_completion(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        max_retries=3,
        response_model=PolicyRecord,
        tool_choice={"type": "tool", "name": "PolicyRecord"},
        messages=[
            {
                "role": "user",
                "content": (
                    "Extract structured prior authorization policy data from this health plan document. "
                    "Capture every covered indication and all PA criteria exactly as stated. "
                    "Do NOT hallucinate. Omit fields absent from the document rather than guessing.\n\n"
                    f"Document: {source_filename}\n"
                    f"{drug_scope}"
                    "The document has been pre-segmented — each [SECTION] block contains raw text "
                    "from that portion of the document.\n\n"
                    f"{_render_sections(sections)}"
                ),
            }
        ],
    )

    return record, completion.usage.output_tokens


def _extract_and_write(
    sections: list[dict],
    source_filename: str,
    record_out: Path,
    drug_hint: str | None = None,
    max_attempts: int = 3,
) -> None:
    """LLM extraction with exponential-backoff retry, writes result to record_out."""
    for attempt in range(max_attempts):
        try:
            record, output_tokens = extract_policy_record(
                sections, source_filename, drug_hint=drug_hint
            )
            if _token_bucket:
                _token_bucket.consume(output_tokens)
            with open(record_out, "w") as f:
                json.dump(record.model_dump(exclude_none=True), f, indent=2)
            print(f"  → policy record → {record_out.name}")
            return
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
            msg = str(e)
            if ("429" in msg or "rate_limit" in msg) and attempt < max_attempts - 1:
                wait = 60 * (attempt + 1)
                print(f"  [rate limit] waiting {wait}s before retry {attempt + 1}...")
                time.sleep(wait)
                continue
            print(f"  [error] LLM extraction failed: {e}")
            return


class _TokenBucket:
    """Sliding 60-second window output-token rate limiter."""

    def __init__(self, limit: int):
        self._limit = limit
        self._window: collections.deque[tuple[float, int]] = collections.deque()

    def consume(self, tokens: int) -> None:
        now = time.time()
        while self._window and self._window[0][0] < now - 60.0:
            self._window.popleft()
        used = sum(t for _, t in self._window)
        if used + tokens > self._limit:
            sleep_for = (self._window[0][0] + 60.0) - now + 0.5
            if sleep_for > 0:
                print(
                    f" [throttle] output window at {used}/{self._limit}, sleeping {sleep_for:.0f}s"
                )
                time.sleep(sleep_for)
        self._window.append((time.time(), tokens))


_token_bucket: _TokenBucket | None = None

_CHUNK_CHAR_BUDGET = 8_000


def _split_sections(sections: list[dict]) -> list[list[dict]]:
    """Split sections into chunks that each fit within _CHUNK_CHAR_BUDGET chars."""
    chunks: list[list[dict]] = []
    current: list[dict] = []
    size = 0
    for s in sections:
        s_size = len(s.get("content", "")) + len(s.get("heading", ""))
        if current and size + s_size > _CHUNK_CHAR_BUDGET:
            chunks.append(current)
            current, size = [s], s_size
        else:
            current.append(s)
            size += s_size
    if current:
        chunks.append(current)
    return chunks


def _coerce_list(val) -> list:
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return []
    return []


def _extract_chunked(
    sections: list[dict],
    source_filename: str,
    record_out: Path,
    drug_hint: str | None = None,
    max_attempts: int = 3,
) -> None:
    """
    Extraction for large documents: splits sections into token-budget chunks
    """
    chunks = _split_sections(sections)
    if len(chunks) == 1:
        _extract_and_write(
            sections,
            source_filename,
            record_out,
            drug_hint=drug_hint,
            max_attempts=max_attempts,
        )
        return

    print(f"  [chunked] {len(sections)} sections → {len(chunks)} chunks")
    base_record: dict | None = None
    all_indications: list = []
    all_exclusions: list = []

    for i, chunk in enumerate(chunks):
        tmp_out = record_out.parent / f"_tmp_{i}_{record_out.name}"
        _extract_and_write(
            chunk,
            source_filename,
            tmp_out,
            drug_hint=drug_hint,
            max_attempts=max_attempts,
        )

        if not tmp_out.exists():
            continue

        with open(tmp_out) as f:
            partial = json.load(f)
        tmp_out.unlink()

        if base_record is None:
            base_record = partial

        all_indications.extend(_coerce_list(partial.get("indications", [])))
        all_exclusions.extend(_coerce_list(partial.get("exclusions", [])))

    if base_record is None:
        print(f"  [error] all chunks failed for {record_out.name}")
        return

    base_record["indications"] = all_indications
    base_record["exclusions"] = all_exclusions

    with open(record_out, "w") as f:
        json.dump(base_record, f, indent=2)
    print(
        f"  → policy record ({len(all_indications)} indications"
        f" across {len(chunks)} chunks) → {record_out.name}"
    )


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

    global _token_bucket
    _token_bucket = _TokenBucket(limit=16_000)

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
            known_slices = [s for s in drug_slices if s["drug"] != "unknown"]
        else:
            all_sections = segment_sections(blocks)
            known_slices = []

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

        if known_slices:
            # Path A: omnibus PA policy doc with detected per-drug boundaries.
            print(
                f"  [omnibus] {len(known_slices)} known drug slices → per-drug extraction"
            )
            for drug_slice in known_slices:
                drug_sections = [
                    s
                    for s in all_sections
                    if s.get("drug_context") == drug_slice["drug"]
                ]
                safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", drug_slice["drug"])
                record_out = records_dir / f"{pdf_file.stem}__{safe_name}.json"
                _extract_chunked(
                    drug_sections,
                    pdf_file.name,
                    record_out,
                    drug_hint=drug_slice["drug"],
                )

        elif doc_format["type"] == "omnibus" and _is_formulary_table(tables):
            # Path B: omnibus formulary/drug-list doc (e.g. MDL).
            print("  [omnibus] formulary list detected → table-based extraction")
            formulary = _parse_formulary_tables(tables, pdf_file.name)
            record_out = records_dir / f"{pdf_file.stem}.json"
            with open(record_out, "w") as f:
                json.dump(formulary, f, indent=2)
            print(
                f"  → formulary record ({len(formulary['drugs'])} entries) → {record_out.name}"
            )

        else:
            # Path C: per-drug or flat doc — chunked if large, single-pass otherwise.
            record_out = records_dir / f"{pdf_file.stem}.json"
            _extract_chunked(all_sections, pdf_file.name, record_out)


if __name__ == "__main__":
    main()
