"""
Pydantic models for LLM-extracted PolicyRecord validation.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Optional
import json

from pydantic import BaseModel, Field, field_validator, model_validator


class CriterionType(str, Enum):
    diagnosis = "diagnosis"
    disease_severity = "disease_severity"
    step_therapy = "step_therapy"
    lab_value = "lab_value"
    prescriber = "prescriber"
    combination_restriction = "combination_restriction"
    age_weight = "age_weight"
    clinical_response = "clinical_response"
    other = "other"


class LogicOperator(str, Enum):
    AND = "AND"
    OR = "OR"


_DURATION_RE = re.compile(r"\b(\d+)\b")
_TRUTHY = {"true", "yes", "1", "y"}
_FALSY = {"false", "no", "0", "n"}

_CRITERION_TYPE_VALUES = {e.value for e in CriterionType}
_LOGIC_OP_VALUES = {e.value for e in LogicOperator}


def _coerce_int_from_string(v: Any) -> Optional[int]:
    """Extract the first integer from a string like '12 months' or 'annually'."""
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if not s:
        return None
    m = _DURATION_RE.search(s)
    if m:
        return int(m.group(1))
    _NAMED = {
        "annually": 12,
        "annual": 12,
        "yearly": 12,
        "year": 12,
        "semi-annually": 6,
        "every 6 months": 6,
        "quarterly": 3,
        "every 3 months": 3,
        "biannually": 6,
        "indefinitely": None,
        "ongoing": None,
        "n/a": None,
    }
    lower = s.lower()
    for phrase, months in _NAMED.items():
        if phrase in lower:
            return months
    return None


def _coerce_bool(v: Any) -> Optional[bool]:
    """Coerce string-booleans from LLM output."""
    if v is None or isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return None


def _clamp_score(v: Any) -> Optional[float]:
    """Accept 0–1 scores; rescale 1–5 scale to 0–1; clamp everything else."""
    if v is None:
        return None
    f = float(v)
    if 0.0 <= f <= 1.0:
        return f
    if 1.0 < f <= 5.0:
        return (f - 1.0) / 4.0  # 5-point → 0-1
    if f > 5.0:
        return 1.0
    return 0.0  # negative


def _coerce_criterion_type(v: Any) -> str:
    """Map unknown criterion_type values to 'other'."""
    if v is None:
        return "other"
    s = str(v).strip().lower().replace(" ", "_").replace("-", "_")
    if s in _CRITERION_TYPE_VALUES:
        return s
    for known in _CRITERION_TYPE_VALUES:
        if known in s or s in known:
            return known
    return "other"


def _coerce_logic_operator(v: Any) -> str:
    """Default unknown logic_operator values to 'AND'."""
    if v is None:
        return "AND"
    s = str(v).strip().upper()
    if s in _LOGIC_OP_VALUES:
        return s
    if "OR" in s:
        return "OR"
    return "AND"


class CriterionItem(BaseModel):
    criterion_type: CriterionType
    description: str = Field(
        description="Criterion text exactly as stated in the document"
    )
    logic_operator: LogicOperator = Field(
        description="How this criterion combines with the next criterion"
    )

    @field_validator("criterion_type", mode="before")
    @classmethod
    def coerce_criterion_type(cls, v: Any) -> str:
        return _coerce_criterion_type(v)

    @field_validator("logic_operator", mode="before")
    @classmethod
    def coerce_logic_operator(cls, v: Any) -> str:
        return _coerce_logic_operator(v)

    @field_validator("description")
    @classmethod
    def description_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("description must not be empty")
        return v


class AuthBlock(BaseModel):
    criteria: list[CriterionItem]
    authorization_duration_months: Optional[int] = Field(
        default=None,
        description="Duration of the authorization period in months",
    )
    required_prescriber_specialties: Optional[list[str]] = None

    @field_validator("authorization_duration_months", mode="before")
    @classmethod
    def coerce_duration(cls, v: Any) -> Optional[int]:
        return _coerce_int_from_string(v)


class Indication(BaseModel):
    name: str = Field(description="Short indication name, e.g. Atopic Dermatitis")
    description: Optional[str] = None
    icd10_codes: Optional[list[str]] = Field(
        default=None,
        description="Only ICD-10 codes EXPLICITLY stated in the document — do not infer",
    )
    pa_required: bool
    step_therapy_required: Optional[bool] = None
    initial_authorization: AuthBlock
    reauthorization: Optional[AuthBlock] = None

    @field_validator("step_therapy_required", mode="before")
    @classmethod
    def coerce_step_therapy(cls, v: Any) -> Optional[bool]:
        return _coerce_bool(v)

    @field_validator("icd10_codes")
    @classmethod
    def strip_icd10(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is None:
            return v
        return [code.strip() for code in v if code.strip()]

    @field_validator("reauthorization", mode="before")
    @classmethod
    def coerce_reauthorization(cls, v: Any) -> Any:
        if isinstance(v, list):
            return v[0] if v else None
        return v


class Exclusion(BaseModel):
    description: str

    @field_validator("description")
    @classmethod
    def description_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("exclusion description must not be empty")
        return v


class ConfidenceScores(BaseModel):
    overall: float = Field(
        description="0.0–1.0 confidence in the overall extraction quality"
    )
    drug_identification: Optional[float] = None
    pa_criteria_completeness: Optional[float] = None
    review_flags: Optional[str] = Field(
        default=None,
        description="Flag ambiguous extractions, missing data, or low-confidence fields",
    )

    @field_validator(
        "overall", "drug_identification", "pa_criteria_completeness", mode="before"
    )
    @classmethod
    def coerce_score(cls, v: Any) -> Optional[float]:
        if v is None:
            return v
        return _clamp_score(v)


class PayerInfo(BaseModel):
    name: str
    policy_id: Optional[str] = Field(
        default=None,
        description="Payer's internal policy/program number, e.g. CP.PMN.112 or 2025 P 2116-22",
    )
    policy_title: str
    effective_date: Optional[str] = Field(
        default=None,
        description="ISO 8601 if parseable, otherwise as written in the document",
    )
    revision_date: Optional[str] = None


class DrugInfo(BaseModel):
    brand_name: str
    generic_name: str
    j_codes: Optional[list[str]] = Field(
        default=None,
        description="HCPCS J-codes explicitly stated in the document, e.g. J0222",
    )
    hcpcs_codes: Optional[list[str]] = None
    drug_class: Optional[str] = Field(
        default=None,
        description="Mechanism of action, e.g. IL-4/IL-13 receptor antagonist",
    )
    route_of_administration: Optional[str] = None
    benefit_type: Optional[str] = Field(
        default=None,
        description="medical or pharmacy",
    )
    limitations_of_use: Optional[str] = Field(
        default=None,
        description="Any explicitly stated limitations of use",
    )

    @field_validator("benefit_type")
    @classmethod
    def normalize_benefit_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        normalized = v.strip().lower()
        if normalized not in {"medical", "pharmacy"}:
            return v.strip()
        return normalized


class PolicyRecord(BaseModel):
    payer: PayerInfo
    drug: DrugInfo
    indications: list[Indication] = Field(
        min_length=1,
        description="One entry per covered indication (disease/condition)",
    )
    exclusions: Optional[list[Exclusion]] = None
    confidence_scores: ConfidenceScores

    @model_validator(mode="after")
    def at_least_one_indication(self) -> "PolicyRecord":
        if not self.indications:
            raise ValueError("PolicyRecord must contain at least one indication")
        return self

    @field_validator("indications", mode="before")
    @classmethod
    def coerce_indications(cls, v: Any) -> Any:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v


def validate_policy_record(raw: dict) -> tuple[PolicyRecord | None, list[str]]:
    """
    Validate a raw dict against PolicyRecord.
    """
    from pydantic import ValidationError

    try:
        model = PolicyRecord.model_validate(raw)
        return model, []
    except ValidationError as exc:
        errors = [
            f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        ]
        return None, errors
