import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import ingest, policies, compare, diff, simulate, changes, pipeline, chat

app = FastAPI(title="Anton RX Policy Tracker API", redirect_slashes=False)


def _allowed_origins() -> list[str]:
    configured = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
    if configured:
        return [origin.strip() for origin in configured.split(",") if origin.strip()]
    return ["http://localhost:3000", "http://localhost:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins(),
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingest.router,   prefix="/v1/ingest",   tags=["ingest"])
app.include_router(policies.router, prefix="/v1/policies", tags=["policies"])
app.include_router(compare.router,  prefix="/v1/compare",  tags=["compare"])
app.include_router(diff.router,     prefix="/v1/diff",     tags=["diff"])
app.include_router(simulate.router, prefix="/v1/simulate", tags=["simulate"])
app.include_router(changes.router,  prefix="/v1/changes",  tags=["changes"])
app.include_router(pipeline.router, prefix="/v1/pipeline", tags=["pipeline"])
app.include_router(chat.router,     prefix="/v1/chat",     tags=["chat"])

@app.get("/health")
async def health():
    return {"status": "ok"}
