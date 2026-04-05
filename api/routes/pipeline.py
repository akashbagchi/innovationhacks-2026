from fastapi import APIRouter, BackgroundTasks

from db.mongo import extraction_audit_log
from pipeline.orchestrator import run_pipeline

router = APIRouter()

# Shared state for last-run summary (in-process only; resets on restart)
_last_result: dict | None = None


async def _run_and_store() -> None:
    global _last_result
    result = await run_pipeline()
    _last_result = result.to_dict()


@router.post("/run")
async def trigger_pipeline(background_tasks: BackgroundTasks):
    """
    Trigger the S3 → extraction → MongoDB pipeline.
    Runs in the background; returns immediately.
    """
    background_tasks.add_task(_run_and_store)
    return {
        "status": "started",
        "message": "Pipeline is running in the background. Check /v1/pipeline/status for results.",
    }


@router.get("/status")
async def pipeline_status():
    """
    Return the result of the most recent pipeline run.
    Falls back to the audit log if the in-process cache is empty (e.g. after restart).
    """
    if _last_result:
        return {"source": "memory", **_last_result}

    # Read from audit log
    doc = await extraction_audit_log.find_one(
        {"event": "pipeline_run"},
        sort=[("timestamp", -1)],
    )
    if not doc:
        return {"status": "never_run", "message": "No pipeline run recorded yet."}

    doc.pop("_id", None)
    return {"source": "audit_log", **doc}
