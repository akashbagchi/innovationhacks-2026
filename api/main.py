from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import ingest, policies, compare, diff, simulate

app = FastAPI(title="Anton RX Policy Tracker API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ingest.router,   prefix="/v1/ingest",   tags=["ingest"])
app.include_router(policies.router, prefix="/v1/policies", tags=["policies"])
app.include_router(compare.router,  prefix="/v1/compare",  tags=["compare"])
app.include_router(diff.router,     prefix="/v1/diff",     tags=["diff"])
app.include_router(simulate.router, prefix="/v1/simulate", tags=["simulate"])

@app.get("/health")
async def health():
    return {"status": "ok"}