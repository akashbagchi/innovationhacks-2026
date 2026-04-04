# innovationhacks-2026
Team Hackerbeans submission to ASU's Largest Hackathon - Innovation Hacks 2026

---

## Setup Guide

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- Git

---

### 1. Clone the repo

```bash
git clone <repo-url>
cd innovationhacks-2026
```

### 2. Create your `.env` file

Create a `.env` file in the project root:

```env
# MongoDB
MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/

# AWS S3
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your_bucket_name

# Anthropic
ANTHROPIC_API_KEY=your_anthropic_key
```

### 3. Start everything

```bash
docker compose up -d
```

First run will build the images (takes ~3-4 minutes). Subsequent starts are instant.

| Service  | URL                          |
|----------|------------------------------|
| Frontend | http://localhost:3000        |
| API      | http://localhost:8000        |
| API docs | http://localhost:8000/docs   |

### 4. Verify it's running

```bash
docker compose ps        # all services should show "running"
curl localhost:8000/health  # should return {"status": "ok"}
```

---

### Workflow

```bash
# Start (after first build, or after docker compose down)
docker compose up -d

# Watch logs
docker compose logs -f

# Watch logs for one service
docker compose logs -f api

# Stop (containers removed, images + data kept)
docker compose down

# Rebuild after changing a Dockerfile or requirements.txt
docker compose up -d --build
```

---

### Running without Docker (optional)

**API**
```bash
cd api
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

**Frontend**
```bash
cd frontend
npm install
npm run dev
```

---

### Project structure

```
innovationhacks-2026/
├── api/
│   ├── main.py           # FastAPI app + router registration
│   ├── routes/           # One file per feature (ingest, policies, compare, diff, simulate)
│   ├── db/
│   │   ├── mongo.py      # MongoDB collections
│   │   └── s3.py         # S3 upload/download helpers
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   ├── src/
│   │   ├── components/ui/  # shadcn components
│   │   └── lib/utils.ts
│   ├── vite.config.ts    # /v1 requests proxied to API
│   └── Dockerfile
├── docker-compose.yml
└── .env                  # not committed
```
