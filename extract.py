import json
import statistics
from pathlib import Path

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
    Classify each block as heading level (1, 2, 3) or None
    """
    body_sizes = [
        b["font_size"] for b in blocks if not b["is_bold"] and b["text"].strip()
    ]
    body_median = statistics.median(body_sizes) if body_sizes else 10.0

    candidate_x0s = [
        b["x0"] for b in blocks if b["is_bold"] and len(b["text"].strip()) < 120
    ]
    x0_sorted = sorted(set(round(x) for x in candidate_x0s))

    def x0_level(x0: float) -> int:
        if not x0_sorted:
            return 2
        percentile = x0_sorted.index(
            min(x0_sorted, key=lambda x: abs(x - round(x0)))
        ) / max(len(x0_sorted) - 1, 1)
        if percentile < 0.33:
            return 1
        elif percentile < 0.66:
            return 2
        else:
            return 3

    for block in blocks:
        text = block["text"].strip()
        if not block["is_bold"] or len(text) > 120 or not text:
            block["heading_level"] = None
            continue
        size_ratio = block["font_size"] / body_median
        block["heading_level"] = 1 if size_ratio >= 1.15 else x0_level(block["x0"])

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
    Post-segmentation cleanup in two passes.
    """
    merged: list[dict] = []

    for section in sections:
        if len(section["heading"].strip()) < 6:
            if merged and section["content"].strip():
                merged[-1]["content"] = (
                    merged[-1]["content"] + "\n" + section["content"]
                ).strip()
        else:
            merged.append(section)

    return [s for s in merged if len(s["content"].strip()) > 30]


# import anthropic
#
# POLICY_RECORD_SCHEMA = { ... }  # see earlier version of this file
#
# def extract_policy(section_text: str) -> dict:
#     client = anthropic.Anthropic()
#     response = client.messages.create(
#         model="claude-opus-4-6",
#         max_tokens=4096,
#         tools=[{"name": "extract_policy_record", ..., "input_schema": POLICY_RECORD_SCHEMA}],
#         tool_choice={"type": "tool", "name": "extract_policy_record"},
#         messages=[{"role": "user", "content": f"Extract policy data:\n{section_text}"}],
#     )
#     for block in response.content:
#         if block.type == "tool_use" and block.name == "extract_policy_record":
#             return block.input
#     raise ValueError("LLM did not return a tool_use block")


def main():
    input_dir = Path("policy_data")
    output_dir = Path("outputs/rough_json")
    output_dir.mkdir(parents=True, exist_ok=True)

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

        output = {
            "source": pdf_file.name,
            "document_format": doc_format,
            "tables": tables,
            "sections": all_sections,
        }

        out_file = output_dir / f"{pdf_file.stem}.json"
        with open(out_file, "w") as f:
            json.dump(output, f, indent=2)

        print(f"  → {len(all_sections)} sections written to {out_file.name}")

    # Once API key is available, wire in the LLM step per doc:
    #
    # for section in all_sections:
    #     if "criteria" in section["heading"].lower() or "coverage" in section["heading"].lower():
    #         policy_record = extract_policy(section["content"])
    #         ...


if __name__ == "__main__":
    main()
