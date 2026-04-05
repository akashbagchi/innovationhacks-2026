from __future__ import annotations

from collections import defaultdict
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DOCKER_OUTPUTS = Path("/outputs")
_LOCAL_OUTPUTS = _PROJECT_ROOT / "outputs"
_REPORTS_DIR = (
    _DOCKER_OUTPUTS / "data_quality_reports"
    if _DOCKER_OUTPUTS.exists()
    else _LOCAL_OUTPUTS / "data_quality_reports"
)
@dataclass
class QualityCheck:
    check_id: str
    severity: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "check_id": self.check_id,
            "severity": self.severity,
            "status": self.status,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def _rollup_status(error_failures: int, warning_failures: int) -> str:
    if error_failures > 0:
        return "FAIL"
    if warning_failures > 0:
        return "WARN"
    return "PASS"


def evaluate_normalized_record(
    normalized_record: dict[str, Any],
    *,
    filename: str,
    s3_key: str,
    payer_canonical: str,
    drug_id: str,
    source: str,
    version: int,
) -> dict[str, Any]:
    checks: list[QualityCheck] = []
    review = normalized_record.get("review") or {}
    payer = normalized_record.get("payer") or {}
    drug = normalized_record.get("drug") or {}
    confidence = normalized_record.get("confidence_scores") or {}
    document_type = normalized_record.get("document_type") or "policy"
    indications = normalized_record.get("indications") or []
    formulary_entries = normalized_record.get("formulary_entries") or []
    j_codes = drug.get("j_codes") or []
    hcpcs_codes = drug.get("hcpcs_codes") or []

    def add(
        check_id: str,
        severity: str,
        status: str,
        message: str,
        **details: Any,
    ) -> None:
        checks.append(
            QualityCheck(
                check_id=check_id,
                severity=severity,
                status=status,
                message=message,
                details=details,
            )
        )

    payer_name = str(payer.get("name") or "").strip()
    if payer_name:
        add("payer_name_present", "ERROR", "PASS", "Payer name is present")
    else:
        add("payer_name_present", "ERROR", "FAIL", "Payer name is missing")

    policy_title = str(payer.get("policy_title") or "").strip()
    if policy_title:
        add("policy_title_present", "ERROR", "PASS", "Policy title is present")
    else:
        add("policy_title_present", "ERROR", "FAIL", "Policy title is missing")

    if document_type == "formulary_list":
        if formulary_entries:
            add(
                "formulary_entries_present",
                "ERROR",
                "PASS",
                "Formulary entries are present",
                formulary_entry_count=len(formulary_entries),
            )
        else:
            add(
                "formulary_entries_present",
                "ERROR",
                "FAIL",
                "No formulary entries were normalized",
            )
        if payer.get("effective_date"):
            add("effective_date_present", "ERROR", "PASS", "Effective date is present")
        else:
            add("effective_date_present", "ERROR", "FAIL", "Effective date is missing")
    else:
        has_drug_identity = bool(
            str(drug.get("display_name") or "").strip()
            or str(drug.get("generic_name") or "").strip()
            or str(drug.get("brand_name") or "").strip()
        )
        if has_drug_identity:
            add("drug_identity_present", "ERROR", "PASS", "Drug identity is present")
        else:
            add("drug_identity_present", "ERROR", "FAIL", "Drug identity is missing")

        if indications:
            add(
                "indications_present",
                "ERROR",
                "PASS",
                "One or more indications were normalized",
                indication_count=len(indications),
            )
        else:
            add("indications_present", "ERROR", "FAIL", "No indications were normalized")

        if payer.get("effective_date"):
            add(
                "effective_date_present",
                "ERROR",
                "PASS",
                "Effective date is present",
            )
        else:
            add(
                "effective_date_present",
                "ERROR",
                "FAIL",
                "Effective date is missing",
            )

        total_codes = len(j_codes) + len(hcpcs_codes)
        if total_codes > 0:
            add(
                "codes_present",
                "WARN",
                "PASS",
                "HCPCS or J-codes are present",
                j_code_count=len(j_codes),
                hcpcs_code_count=len(hcpcs_codes),
            )
        else:
            add(
                "codes_present",
                "WARN",
                "FAIL",
                "No HCPCS or J-codes were normalized",
            )

        duplicate_indications = [
            name
            for name in {str(ind.get("name") or "").strip() for ind in indications}
            if name and sum(1 for ind in indications if str(ind.get("name") or "").strip() == name) > 1
        ]
        if duplicate_indications:
            add(
                "duplicate_indications",
                "WARN",
                "FAIL",
                "Duplicate indication names were found",
                duplicate_indications=duplicate_indications,
            )
        else:
            add(
                "duplicate_indications",
                "WARN",
                "PASS",
                "No duplicate indication names were found",
            )

        empty_initial_criteria = [
            str(ind.get("name") or f"indication_{idx + 1}")
            for idx, ind in enumerate(indications)
            if bool(ind.get("pa_required"))
            if not ((ind.get("initial_authorization") or {}).get("criteria") or [])
        ]
        if empty_initial_criteria:
            add(
                "initial_criteria_present",
                "ERROR",
                "FAIL",
                "Some indications have no initial authorization criteria",
                indications=empty_initial_criteria,
            )
        else:
            add(
                "initial_criteria_present",
                "ERROR",
                "PASS",
                "All indications have initial authorization criteria",
            )

        empty_reauthorization_blocks = [
            str(ind.get("name") or f"indication_{idx + 1}")
            for idx, ind in enumerate(indications)
            if (ind.get("reauthorization") is not None)
            and not ((ind.get("reauthorization") or {}).get("criteria") or [])
            and ((ind.get("reauthorization") or {}).get("authorization_duration_months") is None)
            and not ((ind.get("reauthorization") or {}).get("required_prescriber_specialties") or [])
        ]
        if empty_reauthorization_blocks:
            add(
                "reauthorization_blocks_nonempty",
                "ERROR",
                "FAIL",
                "Some reauthorization blocks are present but empty",
                indications=empty_reauthorization_blocks,
            )
        else:
            add(
                "reauthorization_blocks_nonempty",
                "WARN",
                "PASS",
                "No empty reauthorization blocks were found",
            )

    overall_confidence = confidence.get("overall")
    if isinstance(overall_confidence, (int, float)):
        if overall_confidence < 0.5:
            add(
                "confidence_overall",
                "ERROR",
                "FAIL",
                "Overall extraction confidence is critically low",
                overall=overall_confidence,
            )
        elif overall_confidence < 0.75:
            add(
                "confidence_overall",
                "WARN",
                "FAIL",
                "Overall extraction confidence is below the review threshold",
                overall=overall_confidence,
            )
        else:
            add(
                "confidence_overall",
                "WARN",
                "PASS",
                "Overall extraction confidence is acceptable",
                overall=overall_confidence,
            )

    missing_fields = review.get("missing_fields") or []
    if missing_fields:
        add(
            "review_missing_fields",
            "WARN",
            "FAIL",
            "Normalization review reported missing fields",
            missing_fields=missing_fields,
        )
    else:
        add(
            "review_missing_fields",
            "WARN",
            "PASS",
            "Normalization review reported no missing fields",
        )

    review_warnings = review.get("warnings") or []
    if review_warnings:
        add(
            "review_warnings",
            "WARN",
            "FAIL",
            "Normalization review emitted warnings",
            warnings=review_warnings,
        )
    else:
        add(
            "review_warnings",
            "WARN",
            "PASS",
            "Normalization review emitted no warnings",
        )

    extractor_flags = review.get("extractor_review_flags") or []
    if extractor_flags:
        add(
            "extractor_review_flags",
            "WARN",
            "FAIL",
            "Extractor review flags are present",
            extractor_review_flags=extractor_flags,
        )
    else:
        add(
            "extractor_review_flags",
            "WARN",
            "PASS",
            "Extractor review flags are absent",
        )

    error_count = sum(1 for check in checks if check.severity == "ERROR" and check.status == "FAIL")
    warning_count = sum(1 for check in checks if check.severity == "WARN" and check.status == "FAIL")

    return {
        "filename": filename,
        "s3_key": s3_key,
        "payer_canonical": payer_canonical,
        "drug_id": drug_id,
        "source": source,
        "version": version,
        "document_type": document_type,
        "quality_summary": {
            "error_count": error_count,
            "warning_count": warning_count,
            "status": _rollup_status(error_count, warning_count),
            "passed": error_count == 0,
        },
        "checks": [check.to_dict() for check in checks],
    }


def evaluate_portfolio_quality(current_documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[QualityCheck] = []

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    missing_keys: list[dict[str, Any]] = []
    for doc in current_documents:
        drug_id = str(doc.get("drug_id") or "").strip()
        payer_canonical = str(doc.get("payer_canonical") or "").strip()
        if not drug_id or drug_id == "unknown" or not payer_canonical or payer_canonical == "Unknown Payer":
            missing_keys.append(
                {
                    "_id": str(doc.get("_id")),
                    "drug_id": drug_id,
                    "payer_canonical": payer_canonical,
                    "filename": doc.get("filename"),
                    "version": doc.get("version"),
                }
            )
            continue
        grouped[(drug_id, payer_canonical)].append(
            {
                "_id": str(doc.get("_id")),
                "filename": doc.get("filename"),
                "version": doc.get("version"),
                "status": doc.get("status"),
            }
        )

    duplicate_current_records = [
        {
            "drug_id": drug_id,
            "payer_canonical": payer_canonical,
            "records": records,
        }
        for (drug_id, payer_canonical), records in grouped.items()
        if len(records) > 1
    ]

    if duplicate_current_records:
        checks.append(
            QualityCheck(
                check_id="current_policy_uniqueness",
                severity="ERROR",
                status="FAIL",
                message="Multiple current policy documents exist for the same payer and drug",
                details={"duplicates": duplicate_current_records},
            )
        )
    else:
        checks.append(
            QualityCheck(
                check_id="current_policy_uniqueness",
                severity="ERROR",
                status="PASS",
                message="Current payer and drug combinations are unique",
            )
        )

    if missing_keys:
        checks.append(
            QualityCheck(
                check_id="current_policy_keys_present",
                severity="ERROR",
                status="FAIL",
                message="Some current policy documents are missing stable payer or drug keys",
                details={"records": missing_keys},
            )
        )
    else:
        checks.append(
            QualityCheck(
                check_id="current_policy_keys_present",
                severity="ERROR",
                status="PASS",
                message="Current policy documents have stable payer and drug keys",
            )
        )

    return [check.to_dict() for check in checks]


def write_quality_report(report: dict[str, Any]) -> str:
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc)
    timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    report_name = f"pipeline-quality-{timestamp}.json"
    latest_name = "latest.json"

    payload = {
        **report,
        "generated_at": generated_at.isoformat(),
    }

    report_path = _REPORTS_DIR / report_name
    latest_path = _REPORTS_DIR / latest_name
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(report_path)


def build_empty_quality_report() -> dict[str, Any]:
    return {
        "summary": {
            "documents_evaluated": 0,
            "failed_documents": 0,
            "warning_documents": 0,
            "total_error_failures": 0,
            "total_warning_failures": 0,
            "portfolio_error_failures": 0,
            "portfolio_warning_failures": 0,
            "portfolio_checks_evaluated": 0,
            "gate_status": "PASS",
        },
        "documents": [],
        "portfolio_checks": [],
    }


def summarize_quality_documents(
    documents: list[dict[str, Any]],
    portfolio_checks: list[dict[str, Any]] | None = None,
) -> dict[str, int | str]:
    portfolio_checks = portfolio_checks or []
    failed_documents = sum(
        1 for doc in documents if (doc.get("quality_summary") or {}).get("error_count", 0) > 0
    )
    warning_documents = sum(
        1 for doc in documents if (doc.get("quality_summary") or {}).get("warning_count", 0) > 0
    )
    total_error_failures = sum(
        (doc.get("quality_summary") or {}).get("error_count", 0) for doc in documents
    )
    total_warning_failures = sum(
        (doc.get("quality_summary") or {}).get("warning_count", 0) for doc in documents
    )
    portfolio_error_failures = sum(
        1
        for check in portfolio_checks
        if check.get("severity") == "ERROR" and check.get("status") == "FAIL"
    )
    portfolio_warning_failures = sum(
        1
        for check in portfolio_checks
        if check.get("severity") == "WARN" and check.get("status") == "FAIL"
    )
    gate_status = _rollup_status(
        total_error_failures + portfolio_error_failures,
        total_warning_failures + portfolio_warning_failures,
    )
    return {
        "documents_evaluated": len(documents),
        "failed_documents": failed_documents,
        "warning_documents": warning_documents,
        "total_error_failures": total_error_failures,
        "total_warning_failures": total_warning_failures,
        "portfolio_error_failures": portfolio_error_failures,
        "portfolio_warning_failures": portfolio_warning_failures,
        "portfolio_checks_evaluated": len(portfolio_checks),
        "gate_status": gate_status,
    }
