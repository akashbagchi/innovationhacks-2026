# innovationhacks-2026

Anton RX is a policy intelligence workspace for medical benefit and specialty drug coverage. The repo combines a FastAPI backend, a React dashboard, MongoDB storage, and S3-backed document ingestion to help a team upload payer policy documents, extract structured prior authorization rules, normalize them into a common schema, detect changes between versions, and review the results in a portfolio-style UI.

The project is part demo, part working pipeline. The extraction and normalization flow is implemented, the dashboard can read live data from the backend, and the repo also includes seeded mock data so the experience is usable immediately.

## What the repo does

At a high level, the system supports five jobs:

1. Upload payer policy documents into S3 and create extraction stubs in MongoDB.
2. Run an extraction pipeline that reads PDFs or DOCX files, segments the content, and calls Claude through `instructor` to produce structured policy JSON.
3. Normalize those extracted records into a stable internal schema with canonical payer names, normalized drug identifiers, enriched RxNorm metadata, and review flags.
4. Compare new normalized records against prior versions and write change entries into a changelog collection.
5. Surface portfolio, comparison, and change-monitoring views in the frontend.

## Architecture

### Backend

The backend lives in [`api/`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api) and is a FastAPI app defined in [`api/main.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/main.py).

Main responsibilities:

- Accept uploads through `/v1/ingest`
- Read and write policy data in MongoDB
- Store source documents in S3
- Run the extraction and normalization pipeline
- Maintain changelog history between policy versions
- Serve policy comparison and change feed endpoints to the frontend

Key route groups:

| Route group | Purpose | Status |
| --- | --- | --- |
| `/v1/ingest` | Upload a PDF or DOCX, hash it, dedupe it, store it in S3, and create a pending extraction stub | Implemented |
| `/v1/policies` | Search policies, fetch one by id, or fetch all policies for a drug | Implemented |
| `/v1/compare` | Return payer policy records for a drug | Implemented |
| `/v1/changes` | Return detected policy changes, optionally filtered by drug or severity | Implemented |
| `/v1/pipeline` | Trigger the background extraction pipeline and inspect last run status | Implemented |
| `/v1/diff` | Compare two policy versions and return structured change entries | Implemented |
| `/v1/simulate` | Patient profile simulation endpoint | Stub |

### Frontend

The frontend lives in [`frontend/`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/frontend) and is a Vite + React + TypeScript app.

Main screens:

- Portfolio view
  - Displays tracked drugs, payer burden, trend indicators, recent changes, and portfolio-level summaries.
- Drug detail view
  - Shows cross-payer comparisons, criteria analysis, and a drug-scoped digest of changes.
- Coverage matrix
  - Compares payers side by side for a selected drug using the shared `PolicyRecord` shape.
- Change digest
  - Lists policy changes with severity, change type, before/after text, and criterion tags.

The frontend uses a hybrid data model today:

- Portfolio metadata and many rich demo records come from local mock files in [`frontend/src/data/`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/frontend/src/data).
- Live payer policies and live changelog entries are fetched from the backend through [`frontend/src/lib/api.ts`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/frontend/src/lib/api.ts).
- If the API is unavailable, the UI still renders from the mock dataset.

### Storage

The backend uses three storage layers:

- MongoDB
  - Collections are defined in [`api/db/mongo.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/db/mongo.py).
  - Main collections are `policies`, `policy_versions`, `policy_changelogs`, and `extraction_audit_log`.
- AWS S3
  - Managed through [`api/db/s3.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/db/s3.py).
  - Stores uploaded source documents using a payer/policy/hash-based key structure.
- Local outputs
  - The repo expects generated artifacts under `outputs/` for certain offline workflows and seed helpers.

## How the pipeline works

The extraction pipeline is orchestrated in [`api/pipeline/orchestrator.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/orchestrator.py).

### End to end flow

1. A user uploads a PDF or DOCX through `/v1/ingest`.
2. The backend validates file type, computes a SHA-256 hash, checks for duplicates in S3, uploads the file, and writes a `pending_extraction` stub into MongoDB.
3. `/v1/pipeline/run` starts the background pipeline.
4. The orchestrator collects work from:
   - Pending MongoDB stubs
   - Supported documents already present in S3 but not yet normalized in MongoDB
5. Each document is downloaded from S3 and written to a temporary local file for processing.
6. The extraction layer:
   - Reads text with PyMuPDF first
   - Falls back to `pdfplumber` if text yield is low
   - Falls back to OCR if both text extractors underperform
   - Uses `camelot` for table extraction
   - Supports DOCX input through `python-docx`
7. The document is classified as `per_drug`, `omnibus`, or `flat`.
8. Sections are segmented and sent to Claude Sonnet through Anthropic + `instructor`, which validates the response against the `PolicyRecord` Pydantic model.
9. The extracted record is normalized into a canonical `NormalizedPolicyRecord`.
10. If an older normalized policy exists for the same drug and payer, the old record is archived to `policy_versions` and diffed against the new record.
11. Any detected changes are written to `policy_changelogs`.
12. The current policy record in `policies` is updated or inserted with the new normalized payload.
13. The run result is written to the audit log and exposed through `/v1/pipeline/status`.

### Core pipeline modules

| File | Responsibility |
| --- | --- |
| [`api/pipeline/extract.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/extract.py) | Raw document parsing, sectioning, LLM extraction, OCR fallback, formulary table parsing |
| [`api/pipeline/models.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/models.py) | Pydantic validation for extracted `PolicyRecord` objects |
| [`api/pipeline/schema.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/schema.py) | JSON schema used for structured extraction |
| [`api/pipeline/normalize.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/normalize.py) | Canonicalization of payer names, drug identifiers, criteria, dates, and enrichment metadata |
| [`api/pipeline/diff.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/diff.py) | Version-to-version change detection and severity assignment |
| [`api/pipeline/orchestrator.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/orchestrator.py) | Work item discovery, extraction loop, versioning, changelog writes, audit logging |

### What gets normalized

The normalized policy layer standardizes:

- payer names
- drug display names and generic names
- normalized drug ids
- HCPCS and J-code lists when present
- indications and authorization blocks
- criterion types such as diagnosis, step therapy, lab value, prescriber, and prior therapy
- review warnings and extractor flags
- optional RxNorm enrichment

### Change detection

Change detection currently focuses on material policy deltas such as:

- new or removed indications
- prior authorization requirement changes
- step therapy additions or removals
- added or removed criteria
- authorization duration changes
- wording-only updates when clinical logic stays the same

The change feed returned by `/v1/changes` is what powers the dashboard digest panels.

## Data model and live UI behavior

The shared frontend and backend contract is the `PolicyRecord` shape.

A policy record includes:

- payer metadata
- drug metadata
- one or more covered indications
- initial authorization criteria
- optional reauthorization criteria
- exclusions
- extraction confidence scores

The frontend computes its own summary analytics on top of this data, such as:

- payer stringency scores
- PA burden
- outlier policy detection
- trend deltas over time

These calculations live mostly in the frontend helper layer, not in the API.

## Seed data and demo workflows

[`api/seed_mongo.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/seed_mongo.py) can populate MongoDB with:

- normalized records derived from files in `outputs/policy_records/`
- mock portfolio policies that mirror the frontend demo data
- mock change log entries
- indexes for the most common policy and change queries

This is useful if you want the dashboard to render meaningful content without uploading fresh documents first.

Run it from the repo root:

```bash
python api/seed_mongo.py
```

## Project layout

```text
innovationhacks-2026/
├── api/
│   ├── db/                  # Mongo and S3 adapters
│   ├── pipeline/            # Extraction, normalization, diffing, schemas
│   ├── routes/              # FastAPI route modules
│   ├── main.py              # API app and router registration
│   ├── requirements.txt
│   └── seed_mongo.py
├── frontend/
│   ├── src/
│   │   ├── components/      # Dashboard UI
│   │   ├── data/            # Mock portfolio and change data
│   │   ├── lib/             # API clients and formatters
│   │   └── types/           # Shared TS policy types
│   ├── package.json
│   ├── vite.config.ts
│   └── Dockerfile
├── outputs/                 # Generated extraction artifacts when used
├── docker-compose.yml
├── main.py                  # Small CLI entry point
└── README.md
```

## Environment variables

Create a root `.env` file. The Docker Compose setup loads it into both services.

Required for the main backend workflow:

```env
MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your_bucket_name
ANTHROPIC_API_KEY=your_anthropic_key
```

Optional:

```env
RXNORM_API_BASE=https://rxnav.nlm.nih.gov/REST
RXNORM_LOOKUP_ENABLED=true
RXNORM_TIMEOUT_SECONDS=5
VITE_API_URL=http://localhost:8000
```

## Running the repo

### Docker

This is the simplest path.

```bash
docker compose up --build
```

Services:

- Frontend: `http://localhost:3000`
- API: `http://localhost:8000`
- API docs: `http://localhost:8000/docs`
- Health check: `http://localhost:8000/health`

The compose file mounts:

- [`api/`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api) into the backend container
- [`outputs/`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/outputs) into `/outputs`

### Local development without Docker

Backend:

```bash
cd api
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

The Vite dev server proxies `/v1` requests to `http://localhost:8000`.

## API workflow examples

### 1. Upload a document

```bash
curl -X POST http://localhost:8000/v1/ingest/ \
  -F "file=@/absolute/path/to/policy.pdf" \
  -F "payer=UnitedHealth" \
  -F "policy_id=CS-DERM-0018"
```

### 2. Trigger extraction

```bash
curl -X POST http://localhost:8000/v1/pipeline/run
```

### 3. Check last run status

```bash
curl http://localhost:8000/v1/pipeline/status
```

### 4. Query policies for a drug

```bash
curl "http://localhost:8000/v1/compare?drug=dupilumab"
```

### 5. Query the change feed

```bash
curl "http://localhost:8000/v1/changes?severity=HIGH"
```

## CLI entry point

The root [`main.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/main.py) exposes a minimal CLI wrapper:

```bash
python main.py extract
```

This delegates into the extraction pipeline module. It is currently lightweight and not a full operator CLI.

## Current limitations

This repo is functional, but it is not feature-complete. The main constraints visible in the codebase today are:

- `/v1/simulate` returns a stub decision payload.
- The frontend still depends on mock portfolio metadata and uses live backend data only where available.
- The "Subscribe to alerts" action in the change digest is presentational only.
- The pipeline accuracy report in [`ACCURACY_REPORT.md`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/ACCURACY_REPORT.md) shows that dense or irregular payer documents still produce extraction errors, duplicated indications, missing HCPCS fields, and truncation on long documents.
- Anthropic extraction and RxNorm enrichment both depend on external services and environment setup.

## Where to start if you are extending the repo

If you are new to the codebase, the most useful order is:

1. Read [`api/main.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/main.py) to see the API surface.
2. Read [`api/routes/ingest.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/routes/ingest.py) and [`api/routes/pipeline.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/routes/pipeline.py) to understand the document lifecycle.
3. Read [`api/pipeline/orchestrator.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/orchestrator.py), [`api/pipeline/extract.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/extract.py), and [`api/pipeline/normalize.py`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/api/pipeline/normalize.py) to understand the backend core.
4. Read [`frontend/src/App.tsx`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/frontend/src/App.tsx) and [`frontend/src/components/PortfolioView.tsx`](/Users/pranjalpadakannaya/Desktop/Hackathon/innovationhacks-2026/frontend/src/components/PortfolioView.tsx) to understand how the dashboard is composed.
5. Seed Mongo and run the UI so you can inspect real payloads before changing schemas or route contracts.
