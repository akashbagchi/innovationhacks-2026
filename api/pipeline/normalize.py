import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


_PROJECT_ROOT = Path(__file__).parent.parent.parent

_PAYER_NAME_MAP = {
    "bcbs nc": "Blue Cross NC",
    "blue cross nc": "Blue Cross NC",
    "bluecross nc": "Blue Cross NC",
    "uhc": "UnitedHealth",
    "united healthcare": "UnitedHealth",
    "unitedhealthcare": "UnitedHealth",
    "cigna healthcare": "Cigna",
    "florida blue": "Florida Blue",
    "priority health": "Priority Health",
}

_CRITERION_TYPE_MAP = {
    "diagnosis": "diagnosis",
    "disease_severity": "disease_severity",
    "step_therapy": "step_therapy",
    "lab_value": "lab_value",
    "prescriber": "prescriber",
    "combination_restriction": "combination_restriction",
    "age_weight": "age_weight",
    "clinical_response": "clinical_response",
    "prior_treatment": "prior_therapy",
    "prior_therapy": "prior_therapy",
    "line_of_therapy": "line_of_therapy",
    "other": "other",
}

_STEP_THERAPY_TERMS = (
    "step therapy",
    "fail-first",
    "fail first",
    "sequencing requirement",
    "sequencing requirements",
)

_HCPCS_CODE_PATTERN = re.compile(r"\b([A-Z]\d{4})\b")


class NormalizedCriterion(BaseModel):
    criterion_type: str
    original_criterion_type: str | None = None
    description: str
    logic_operator: str = "AND"
    normalized_tokens: list[str] = Field(default_factory=list)


class NormalizedAuthBlock(BaseModel):
    criteria: list[NormalizedCriterion] = Field(default_factory=list)
    authorization_duration_months: int | None = None
    required_prescriber_specialties: list[str] = Field(default_factory=list)


class EnrichmentStatus(BaseModel):
    rxnorm_cui: str | None = None
    loinc_codes: list[str] = Field(default_factory=list)
    validated_icd10_codes: list[str] = Field(default_factory=list)
    needs_rxnorm_lookup: bool = True
    needs_loinc_linking: bool = False
    needs_icd10_validation: bool = False


class NormalizedIndication(BaseModel):
    indication_id: str
    name: str
    description: str | None = None
    icd10_codes_explicit: list[str] = Field(default_factory=list)
    icd10_codes_inferred: list[str] = Field(default_factory=list)
    pa_required: bool = False
    step_therapy_required: bool = False
    initial_authorization: NormalizedAuthBlock = Field(
        default_factory=NormalizedAuthBlock
    )
    reauthorization: NormalizedAuthBlock | None = None
    enrichment: EnrichmentStatus = Field(default_factory=EnrichmentStatus)


class NormalizedFormularyEntry(BaseModel):
    entry_id: str
    hcpcs_code: str | None = None
    drug_name: str | None = None
    description: str | None = None
    coverage_level: str | None = None
    category: str | None = None
    notes: str | None = None
    pa_required: bool = False
    step_therapy_possible: bool = False


class NormalizedDrug(BaseModel):
    display_name: str
    brand_names: list[str] = Field(default_factory=list)
    generic_name: str | None = None
    normalized_generic_name: str | None = None
    j_codes: list[str] = Field(default_factory=list)
    hcpcs_codes: list[str] = Field(default_factory=list)
    benefit_type: str | None = None
    drug_class: str | None = None
    route_of_administration: str | None = None
    limitations_of_use: str | None = None
    enrichment: EnrichmentStatus = Field(default_factory=EnrichmentStatus)


class ReviewSummary(BaseModel):
    missing_fields: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    extractor_review_flags: list[str] = Field(default_factory=list)


class NormalizedPolicyRecord(BaseModel):
    normalization_version: str = "2026-04-04"
    source_filename: str | None = None
    document_type: str = "policy"
    payer: dict[str, Any]
    drug: NormalizedDrug
    indications: list[NormalizedIndication] = Field(default_factory=list)
    formulary_entries: list[NormalizedFormularyEntry] = Field(default_factory=list)
    exclusions: list[dict[str, str]] = Field(default_factory=list)
    confidence_scores: dict[str, Any] = Field(default_factory=dict)
    review: ReviewSummary = Field(default_factory=ReviewSummary)


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _slugify(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "unknown"


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _normalize_code_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    normalized: list[str] = []
    for value in values:
        text = _clean_string(value)
        if text:
            normalized.append(text.upper())
    return _dedupe_preserve_order(normalized)


def _extract_codes_from_text(value: Any) -> list[str]:
    text = _clean_string(value)
    if not text:
        return []
    return _dedupe_preserve_order(
        [match.upper() for match in _HCPCS_CODE_PATTERN.findall(text.upper())]
    )


def _normalize_date(value: Any) -> str | None:
    text = _clean_string(value)
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue

    for fmt in ("%Y-%m", "%m/%Y"):
        try:
            parsed = datetime.strptime(text, fmt)
            return date(parsed.year, parsed.month, 1).isoformat()
        except ValueError:
            continue

    return text


def _normalize_payer_name(name: Any, source_filename: str | None) -> str:
    cleaned = _clean_string(name)
    if cleaned:
        mapped = _PAYER_NAME_MAP.get(cleaned.lower())
        return mapped or cleaned

    if source_filename:
        stem = Path(source_filename).stem.lower()
        for alias, canonical in _PAYER_NAME_MAP.items():
            if alias in stem:
                return canonical

    return "Unknown Payer"


def _split_brand_names(value: Any) -> list[str]:
    text = _clean_string(value)
    if not text:
        return []
    parts = [part.strip() for part in text.split(",")]
    cleaned = [part for part in parts if part]
    return _dedupe_preserve_order(cleaned)


def _normalize_benefit_type(value: Any) -> str | None:
    text = _clean_string(value)
    if not text:
        return None
    lowered = text.lower()
    if "medical" in lowered:
        return "medical"
    if "pharmacy" in lowered:
        return "pharmacy"
    return lowered


def _extract_review_flags(confidence_scores: dict[str, Any]) -> list[str]:
    review_flags = confidence_scores.get("review_flags")
    if not review_flags:
        return []
    if isinstance(review_flags, list):
        return [_clean_string(flag) for flag in review_flags if _clean_string(flag)]
    text = _clean_string(review_flags)
    if not text:
        return []
    return [part.strip() for part in re.split(r"[;\n]", text) if part.strip()]


def _criterion_tokens(description: str) -> list[str]:
    lowered = description.lower()
    tokens: list[str] = []
    for term in _STEP_THERAPY_TERMS:
        if term in lowered:
            tokens.append("step_therapy")
            break
    if "specialist" in lowered or "prescriber" in lowered:
        tokens.append("prescriber_requirement")
    if re.search(r"\b(?:j|q)\d{4}\b", lowered):
        tokens.append("billing_code")
    if re.search(r"\bicd-?10\b", lowered):
        tokens.append("diagnosis_code")
    return _dedupe_preserve_order(tokens)


def _infer_policy_level_codes(raw_record: dict[str, Any]) -> list[str]:
    codes: list[str] = []

    drug_raw = raw_record.get("drug") if isinstance(raw_record.get("drug"), dict) else {}
    for key in ("brand_name", "generic_name", "drug_class", "limitations_of_use"):
        codes.extend(_extract_codes_from_text(drug_raw.get(key)))

    indications = raw_record.get("indications")
    if isinstance(indications, list):
        for indication in indications:
            if not isinstance(indication, dict):
                continue
            codes.extend(_extract_codes_from_text(indication.get("name")))
            codes.extend(_extract_codes_from_text(indication.get("description")))
            auth_blocks = [
                indication.get("initial_authorization"),
                indication.get("reauthorization"),
            ]
            for auth in auth_blocks:
                if not isinstance(auth, dict):
                    continue
                for criterion in auth.get("criteria") or []:
                    if isinstance(criterion, dict):
                        codes.extend(_extract_codes_from_text(criterion.get("description")))

    exclusions = raw_record.get("exclusions")
    if isinstance(exclusions, list):
        for exclusion in exclusions:
            if isinstance(exclusion, dict):
                codes.extend(_extract_codes_from_text(exclusion.get("description")))

    return _dedupe_preserve_order(codes)


def _normalize_formulary_entry(raw: dict[str, Any], index: int) -> NormalizedFormularyEntry:
    hcpcs_code = _clean_string(raw.get("hcpcs_code"))
    drug_name = _clean_string(raw.get("drug_name"))
    description = _clean_string(raw.get("description"))
    coverage_level = _clean_string(raw.get("coverage_level"))
    category = _clean_string(raw.get("category"))
    notes = _clean_string(raw.get("notes"))
    combined_text = " ".join(filter(None, [description, notes])).lower()

    return NormalizedFormularyEntry(
        entry_id=f"{_slugify(drug_name or hcpcs_code or f'entry-{index}')}-{index}",
        hcpcs_code=hcpcs_code.upper() if hcpcs_code else None,
        drug_name=None if drug_name == "N/A" else drug_name,
        description=description,
        coverage_level=coverage_level,
        category=category,
        notes=notes,
        pa_required="pa" in combined_text,
        step_therapy_possible=(
            "covered alternative" in combined_text
            or any(term in combined_text for term in _STEP_THERAPY_TERMS)
        ),
    )


def _normalize_criterion(raw: dict[str, Any]) -> NormalizedCriterion:
    original_type = _clean_string(raw.get("criterion_type")) or "other"
    normalized_type = _CRITERION_TYPE_MAP.get(original_type.lower(), "other")
    description = _clean_string(raw.get("description")) or ""
    logic_operator = (_clean_string(raw.get("logic_operator")) or "AND").upper()
    if logic_operator not in {"AND", "OR"}:
        logic_operator = "AND"

    if normalized_type == "other":
        lowered = description.lower()
        if any(term in lowered for term in _STEP_THERAPY_TERMS):
            normalized_type = "step_therapy"
        elif "specialist" in lowered or "prescriber" in lowered:
            normalized_type = "prescriber"

    return NormalizedCriterion(
        criterion_type=normalized_type,
        original_criterion_type=original_type,
        description=description,
        logic_operator=logic_operator,
        normalized_tokens=_criterion_tokens(description),
    )


def _normalize_auth_block(raw: Any) -> NormalizedAuthBlock:
    if not isinstance(raw, dict):
        return NormalizedAuthBlock()

    criteria = []
    for item in raw.get("criteria") or []:
        if isinstance(item, dict):
            criteria.append(_normalize_criterion(item))

    specialties = []
    for specialty in raw.get("required_prescriber_specialties") or []:
        cleaned = _clean_string(specialty)
        if cleaned:
            specialties.append(cleaned.lower())

    return NormalizedAuthBlock(
        criteria=criteria,
        authorization_duration_months=raw.get("authorization_duration_months"),
        required_prescriber_specialties=_dedupe_preserve_order(specialties),
    )


def _infer_step_therapy(indication: dict[str, Any], initial_auth: NormalizedAuthBlock) -> bool:
    raw_value = indication.get("step_therapy_required")
    if isinstance(raw_value, bool):
        return raw_value

    description = _clean_string(indication.get("description")) or ""
    if any(term in description.lower() for term in _STEP_THERAPY_TERMS):
        return True

    return any(c.criterion_type == "step_therapy" for c in initial_auth.criteria)


def _normalize_indication(
    raw: dict[str, Any],
    index: int,
    review: ReviewSummary,
) -> NormalizedIndication:
    name = _clean_string(raw.get("name")) or f"Unnamed indication {index}"
    description = _clean_string(raw.get("description"))
    initial_auth = _normalize_auth_block(raw.get("initial_authorization"))
    reauth_raw = raw.get("reauthorization")
    reauthorization = (
        _normalize_auth_block(reauth_raw) if isinstance(reauth_raw, dict) else None
    )

    explicit_icd10 = _normalize_code_list(raw.get("icd10_codes"))
    if raw.get("icd10_codes") and not explicit_icd10:
        review.warnings.append(f"{name}: unable to normalize one or more ICD-10 codes")

    return NormalizedIndication(
        indication_id=f"{_slugify(name)}-{index}",
        name=name,
        description=description,
        icd10_codes_explicit=explicit_icd10,
        icd10_codes_inferred=[],
        pa_required=bool(raw.get("pa_required", False)),
        step_therapy_required=_infer_step_therapy(raw, initial_auth),
        initial_authorization=initial_auth,
        reauthorization=reauthorization,
        enrichment=EnrichmentStatus(
            needs_rxnorm_lookup=False,
            needs_loinc_linking=any(
                c.criterion_type == "lab_value" for c in initial_auth.criteria
            ),
            needs_icd10_validation=bool(explicit_icd10),
        ),
    )


def _normalize_formulary_record(
    raw_record: dict[str, Any],
    *,
    source_filename: str | None,
) -> NormalizedPolicyRecord:
    review = ReviewSummary()
    payer_raw = raw_record.get("payer") if isinstance(raw_record.get("payer"), dict) else {}
    payer_name = _normalize_payer_name(payer_raw.get("name"), source_filename)
    if payer_name == "Unknown Payer":
        review.missing_fields.append("payer.name")

    raw_entries = raw_record.get("drugs")
    formulary_entries: list[NormalizedFormularyEntry] = []
    if isinstance(raw_entries, list):
        for index, entry in enumerate(raw_entries, start=1):
            if isinstance(entry, dict):
                formulary_entries.append(_normalize_formulary_entry(entry, index))
    else:
        review.missing_fields.append("drugs")

    aggregated_codes = _dedupe_preserve_order(
        [entry.hcpcs_code for entry in formulary_entries if entry.hcpcs_code]
    )

    if not formulary_entries:
        review.warnings.append("No formulary entries were available to normalize")

    return NormalizedPolicyRecord(
        source_filename=source_filename,
        document_type="formulary_list",
        payer={
            "name": payer_name,
            "policy_id": _clean_string(payer_raw.get("policy_id")),
            "policy_title": _clean_string(payer_raw.get("policy_title"))
            or (Path(source_filename).stem if source_filename else "Unknown Policy"),
            "effective_date": _normalize_date(payer_raw.get("effective_date")),
            "revision_date": _normalize_date(payer_raw.get("revision_date")),
        },
        drug=NormalizedDrug(
            display_name="Various",
            brand_names=[],
            generic_name="formulary",
            normalized_generic_name="formulary",
            j_codes=[],
            hcpcs_codes=aggregated_codes,
            benefit_type="medical",
            drug_class=None,
            route_of_administration=None,
            limitations_of_use=None,
            enrichment=EnrichmentStatus(
                needs_rxnorm_lookup=False,
                needs_loinc_linking=False,
                needs_icd10_validation=False,
            ),
        ),
        indications=[],
        formulary_entries=formulary_entries,
        exclusions=[],
        confidence_scores={},
        review=review,
    )


def normalize_policy_record(
    raw_record: dict[str, Any],
    *,
    source_filename: str | None = None,
) -> NormalizedPolicyRecord:
    if raw_record.get("document_type") == "formulary_list" or isinstance(
        raw_record.get("drugs"), list
    ):
        return _normalize_formulary_record(
            raw_record,
            source_filename=source_filename,
        )

    review = ReviewSummary()

    payer_raw = raw_record.get("payer") if isinstance(raw_record.get("payer"), dict) else {}
    drug_raw = raw_record.get("drug") if isinstance(raw_record.get("drug"), dict) else {}
    indications_raw = raw_record.get("indications")
    exclusions_raw = raw_record.get("exclusions")
    confidence_scores = (
        raw_record.get("confidence_scores")
        if isinstance(raw_record.get("confidence_scores"), dict)
        else {}
    )

    payer_name = _normalize_payer_name(payer_raw.get("name"), source_filename)
    if payer_name == "Unknown Payer":
        review.missing_fields.append("payer.name")

    brand_names = _split_brand_names(drug_raw.get("brand_name"))
    generic_name = _clean_string(drug_raw.get("generic_name"))
    display_name = brand_names[0] if brand_names else (generic_name or "Unknown Drug")

    if display_name == "Unknown Drug":
        review.missing_fields.append("drug.brand_name|drug.generic_name")

    inferred_codes = _infer_policy_level_codes(raw_record)
    structured_j_codes = _normalize_code_list(drug_raw.get("j_codes"))
    structured_hcpcs_codes = _normalize_code_list(drug_raw.get("hcpcs_codes"))
    all_hcpcs_codes = _dedupe_preserve_order(
        structured_hcpcs_codes + structured_j_codes + inferred_codes
    )
    all_j_codes = [code for code in all_hcpcs_codes if code.startswith("J")]

    normalized_drug = NormalizedDrug(
        display_name=display_name,
        brand_names=brand_names,
        generic_name=generic_name,
        normalized_generic_name=generic_name.lower() if generic_name else None,
        j_codes=all_j_codes,
        hcpcs_codes=all_hcpcs_codes,
        benefit_type=_normalize_benefit_type(drug_raw.get("benefit_type")),
        drug_class=_clean_string(drug_raw.get("drug_class")),
        route_of_administration=_clean_string(drug_raw.get("route_of_administration")),
        limitations_of_use=_clean_string(drug_raw.get("limitations_of_use")),
        enrichment=EnrichmentStatus(
            needs_rxnorm_lookup=bool(display_name and display_name != "Unknown Drug"),
            needs_loinc_linking=False,
            needs_icd10_validation=False,
        ),
    )

    if not normalized_drug.j_codes and not normalized_drug.hcpcs_codes:
        review.warnings.append("No HCPCS/J-codes were extracted for this policy")

    normalized_indications: list[NormalizedIndication] = []
    if isinstance(indications_raw, list):
        for index, indication in enumerate(indications_raw, start=1):
            if isinstance(indication, dict):
                normalized_indications.append(
                    _normalize_indication(indication, index, review)
                )
    else:
        review.missing_fields.append("indications")

    if not normalized_indications:
        review.warnings.append(
            "No normalized indications available; downstream compare/simulate features will be limited"
        )

    normalized_exclusions: list[dict[str, str]] = []
    if isinstance(exclusions_raw, list):
        for item in exclusions_raw:
            if not isinstance(item, dict):
                continue
            description = _clean_string(item.get("description"))
            if description:
                normalized_exclusions.append({"description": description})

    review.extractor_review_flags = _extract_review_flags(confidence_scores)

    return NormalizedPolicyRecord(
        source_filename=source_filename,
        document_type="policy",
        payer={
            "name": payer_name,
            "policy_id": _clean_string(payer_raw.get("policy_id")),
            "policy_title": _clean_string(payer_raw.get("policy_title"))
            or (Path(source_filename).stem if source_filename else "Unknown Policy"),
            "effective_date": _normalize_date(payer_raw.get("effective_date")),
            "revision_date": _normalize_date(payer_raw.get("revision_date")),
        },
        drug=normalized_drug,
        indications=normalized_indications,
        exclusions=normalized_exclusions,
        confidence_scores=confidence_scores,
        review=review,
    )


def normalize_record_file(input_path: Path, output_path: Path) -> NormalizedPolicyRecord:
    raw_text = input_path.read_text(encoding="utf-8")
    if not raw_text.strip():
        raise ValueError(f"Input file is empty: {input_path.name}")

    raw_record = json.loads(raw_text)
    normalized = normalize_policy_record(raw_record, source_filename=input_path.name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(normalized.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return normalized


def run_normalization(
    input_dir: Path | None = None,
    output_dir: Path | None = None,
) -> list[NormalizedPolicyRecord]:
    input_dir = input_dir or (_PROJECT_ROOT / "outputs" / "policy_records")
    output_dir = output_dir or (_PROJECT_ROOT / "outputs" / "normalized_records")
    output_dir.mkdir(parents=True, exist_ok=True)

    normalized_records: list[NormalizedPolicyRecord] = []
    for input_path in sorted(input_dir.glob("*.json")):
        output_path = output_dir / input_path.name
        try:
            normalized_records.append(normalize_record_file(input_path, output_path))
        except Exception as exc:
            print(f"[skip] {input_path.name}: {exc}")
    return normalized_records


def main():
    records = run_normalization()
    print(f"Normalized {len(records)} policy record(s)")


if __name__ == "__main__":
    main()
