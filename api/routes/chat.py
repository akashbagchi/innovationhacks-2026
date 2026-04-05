import asyncio
import json
import logging
from typing import Literal

import anthropic
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from db.mongo import policies, policy_changelogs
from pipeline.normalize import _lookup_rxnorm

logger = logging.getLogger(__name__)

# TTY values that represent a base ingredient concept in RxNorm
_INGREDIENT_TTYS = {"IN", "PIN", "MIN"}

router = APIRouter()

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class MessageTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    message: str
    history: list[MessageTurn] = []


class SourceRef(BaseModel):
    payer: str
    policy_title: str
    drug_id: str
    s3_key: str | None = None
    mongo_id: str


class ChatResponse(BaseModel):
    reply: str
    sources: list[SourceRef] = []


# ---------------------------------------------------------------------------
# Tool definitions for Claude
# ---------------------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "name": "search_drug_policy",
        "description": (
            "Search for drug prior-authorization policy records by drug name and optionally by payer. "
            "Returns PA criteria, indications, step therapy requirements, and other policy details."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "drug_id": {
                    "type": "string",
                    "description": "Drug name or ID to search for (e.g. 'dupixent', 'bevacizumab', 'adalimumab')",
                },
                "payer": {
                    "type": "string",
                    "description": "Optional payer name to filter by (e.g. 'UnitedHealth', 'Cigna', 'Blue Cross NC')",
                },
            },
            "required": ["drug_id"],
        },
    },
    {
        "name": "get_policy_changes",
        "description": (
            "Get recent policy changes / changelog entries for a drug. "
            "Returns severity, change type, and summary of each detected change."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "drug_id": {
                    "type": "string",
                    "description": "Drug name or ID to look up changes for",
                },
                "severity": {
                    "type": "string",
                    "enum": ["HIGH", "MED", "LOW"],
                    "description": "Optional severity filter",
                },
            },
            "required": ["drug_id"],
        },
    },
    {
        "name": "compare_payers",
        "description": (
            "Compare policy records across multiple payers for a specific drug. "
            "Returns all matching payer policies — useful for cross-payer PA criteria comparison."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "drug_id": {
                    "type": "string",
                    "description": "Drug name or ID to compare across payers",
                },
                "payers": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional list of specific payers (e.g. ['UnitedHealth', 'Cigna']). Omit for all payers.",
                },
            },
            "required": ["drug_id"],
        },
    },
]

SYSTEM_PROMPT = """\
You are a Policy Assistant embedded in a medical benefit drug policy tracking platform. \
You have access to structured prior-authorization (PA) policy data extracted from major health plan payers \
(UnitedHealth, Cigna, Aetna, Blue Cross NC, EmblemHealth, Florida Blue, Priority Health, and others).

Use the available tools to retrieve policy data before answering. Pass drug names exactly as the user \
stated them — the backend resolves brand names to generic names via RxNorm automatically. \
Be concise and clinically precise. Quote criteria exactly as extracted from the documents. \
If no data exists for a drug or payer, say so clearly — never speculate.\
"""


# ---------------------------------------------------------------------------
# RxNorm drug name resolution
# ---------------------------------------------------------------------------

async def _resolve_drug_name(raw_name: str) -> str:
    """
    Resolve a drug name (brand or generic) to its RxNorm base ingredient name.
    Returns the original name lowercased if resolution fails.
    Wraps the sync _lookup_rxnorm in a thread executor to avoid blocking.
    """
    loop = asyncio.get_event_loop()
    try:
        match = await loop.run_in_executor(None, _lookup_rxnorm, raw_name)
    except Exception as exc:
        logger.warning("RxNorm lookup failed for %r: %s", raw_name, exc)
        return raw_name.lower()

    if not match:
        return raw_name.lower()

    rxcui = match.get("rxnorm_cui")
    tty = match.get("rxnorm_tty", "")
    name = match.get("rxnorm_name") or raw_name

    # If already a base ingredient, we're done
    if tty in _INGREDIENT_TTYS:
        return name.lower()

    # Brand name / product — walk up to the ingredient via the RxNorm related endpoint
    if rxcui:
        try:
            import requests
            resp = await loop.run_in_executor(
                None,
                lambda: requests.get(
                    f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/related.json",
                    params={"tty": "IN"},
                    timeout=5,
                ),
            )
            resp.raise_for_status()
            related = resp.json()
            groups = related.get("relatedGroup", {}).get("conceptGroup", [])
            for group in groups:
                concepts = group.get("conceptProperties", [])
                if concepts:
                    return concepts[0]["name"].lower()
        except Exception as exc:
            logger.warning("RxNorm ingredient resolution failed for rxcui %s: %s", rxcui, exc)

    return name.lower()


# ---------------------------------------------------------------------------
# Tool execution helpers
# ---------------------------------------------------------------------------

def _policy_to_summary(doc: dict) -> dict:
    pr = doc.get("policy_record") or {}
    return {
        "payer": doc.get("payer_canonical"),
        "drug_id": doc.get("drug_id"),
        "policy_title": (pr.get("payer") or {}).get("policy_title"),
        "effective_date": (pr.get("payer") or {}).get("effective_date"),
        "benefit_type": (pr.get("drug") or {}).get("benefit_type"),
        "indications": [
            {
                "name": ind.get("name"),
                "pa_required": ind.get("pa_required"),
                "step_therapy_required": ind.get("step_therapy_required"),
                "icd10_codes": ind.get("icd10_codes"),
                "initial_auth_criteria": [
                    {"type": c.get("criterion_type"), "description": c.get("description")}
                    for c in ((ind.get("initial_authorization") or {}).get("criteria") or [])
                ],
                "reauth_criteria": [
                    {"type": c.get("criterion_type"), "description": c.get("description")}
                    for c in ((ind.get("reauthorization") or {}).get("criteria") or [])
                ],
            }
            for ind in (pr.get("indications") or [])
        ],
    }


def _collect_sources(docs: list[dict], sources: list[SourceRef], seen: set[str]) -> None:
    for doc in docs:
        mid = str(doc["_id"])
        if mid in seen:
            continue
        seen.add(mid)
        pr = doc.get("policy_record") or {}
        sources.append(SourceRef(
            payer=doc.get("payer_canonical", ""),
            policy_title=(pr.get("payer") or {}).get("policy_title", ""),
            drug_id=doc.get("drug_id", ""),
            s3_key=doc.get("s3_key"),
            mongo_id=mid,
        ))


def _drug_query(raw_name: str, resolved_name: str) -> dict:
    """
    Build a MongoDB $or query that matches on drug_id (normalized generic),
    policy_record.drug.generic_name, or policy_record.drug.brand_name.
    Using both the RxNorm-resolved name and the original input covers brand/generic variants.
    """
    candidates = list({resolved_name, raw_name.lower()})
    return {
        "$or": [
            {"drug_id": {"$regex": "|".join(candidates), "$options": "i"}},
            {"policy_record.drug.generic_name": {"$regex": "|".join(candidates), "$options": "i"}},
            {"policy_record.drug.brand_name": {"$regex": raw_name, "$options": "i"}},
        ]
    }


async def _execute_tool(
    name: str,
    args: dict,
    sources: list[SourceRef],
    seen: set[str],
) -> str:
    if name == "search_drug_policy":
        raw = args["drug_id"]
        resolved = await _resolve_drug_name(raw)
        query = _drug_query(raw, resolved)
        if args.get("payer"):
            query["payer_canonical"] = {"$regex": args["payer"], "$options": "i"}
        docs = await policies.find(query).to_list(length=10)
        _collect_sources(docs, sources, seen)
        if not docs:
            return json.dumps({"result": f"No policies found for '{raw}' (resolved: '{resolved}')."})
        return json.dumps([_policy_to_summary(d) for d in docs], default=str)

    if name == "get_policy_changes":
        raw = args["drug_id"]
        resolved = await _resolve_drug_name(raw)
        query: dict = _drug_query(raw, resolved)
        if args.get("severity"):
            query["severity"] = args["severity"]
        docs = await policy_changelogs.find(query, {"_id": 0}).sort("date", -1).to_list(length=20)
        if not docs:
            return json.dumps({"result": f"No changes found for '{raw}' (resolved: '{resolved}')."})
        return json.dumps(docs, default=str)

    if name == "compare_payers":
        raw = args["drug_id"]
        resolved = await _resolve_drug_name(raw)
        query = _drug_query(raw, resolved)
        if args.get("payers"):
            query["payer_canonical"] = {"$in": args["payers"]}
        docs = await policies.find(query).to_list(length=20)
        _collect_sources(docs, sources, seen)
        if not docs:
            return json.dumps({"result": f"No payer policies found for '{raw}' (resolved: '{resolved}')."})
        return json.dumps([_policy_to_summary(d) for d in docs], default=str)

    return json.dumps({"error": f"Unknown tool: {name}"})


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@router.post("")
async def chat(request: ChatRequest) -> ChatResponse:
    client = anthropic.Anthropic()
    sources: list[SourceRef] = []
    seen_ids: set[str] = set()

    messages: list[dict] = [{"role": m.role, "content": m.content} for m in request.history]
    messages.append({"role": "user", "content": request.message})

    reply = ""

    try:
        for _ in range(3):  # max 3 agentic iterations
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )

            if response.stop_reason == "end_turn":
                reply = next(
                    (b.text for b in response.content if hasattr(b, "text")),
                    "",
                )
                break

            if response.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                tool_results = []
                for block in response.content:
                    if block.type != "tool_use":
                        continue
                    result = await _execute_tool(block.name, block.input, sources, seen_ids)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
                messages.append({"role": "user", "content": tool_results})
            else:
                break

    except anthropic.RateLimitError:
        raise HTTPException(status_code=429, detail="Rate limit exceeded — try again shortly.")
    except anthropic.APIError as exc:
        raise HTTPException(status_code=502, detail=f"Upstream API error: {exc}")

    if not reply:
        reply = "I wasn't able to generate a response. Please try rephrasing your question."

    return ChatResponse(reply=reply, sources=sources)
