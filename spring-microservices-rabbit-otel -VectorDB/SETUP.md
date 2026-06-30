# OteltoVectorDB — Local POC Setup Guide

## What this adds to your existing repo

| New file | Where it goes | What it does |
|---|---|---|
| `docker-compose.yaml` | repo root | Replaces `infra-docker-compose.yaml` — runs everything |
| `Dockerfile` | repo root | Builds all 5 Spring Boot services (one image, SERVICE_NAME arg) |
| `pom.xml` | repo root | Adds PostgreSQL driver alongside H2 |
| `postgres/init.sh` | repo root | Creates 6 databases + pgvector extension + embeddings table |
| `otel-collector-config.yaml` | repo root | Adds pipeline export to telemetry-pipeline service |
| `service-configs/*.yaml` | each service `src/main/resources/application.yaml` | Env-var-driven config (Postgres, RabbitMQ, OTel) |
| `telemetry-pipeline/` | repo root | Python service: polls Jaeger → embeds spans → pgvector |
| `rag-service/` | repo root | Python FastAPI: Claude RAG + demo UI |

---

## Step-by-step setup

### 1. Copy files into your repo

Your repo structure after copying:
```
spring-microservices-rabbit-otel -VectorDB/
├── docker-compose.yaml          ← NEW (replaces infra-docker-compose.yaml)
├── Dockerfile                   ← NEW
├── pom.xml                      ← UPDATED
├── otel-collector-config.yaml   ← UPDATED
├── postgres/
│   └── init.sh                  ← NEW
├── telemetry-pipeline/          ← NEW folder
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── rag-service/                 ← NEW folder
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── catalog-service/src/main/resources/application.yaml   ← REPLACE
├── order-service/src/main/resources/application.yaml     ← REPLACE
├── inventory-service/src/main/resources/application.yaml ← REPLACE
├── payment-service/src/main/resources/application.yaml   ← REPLACE
└── shipment-service/src/main/resources/application.yaml  ← REPLACE
```

Replace each service's `application.yaml` with the corresponding file from `service-configs/`.

### 2. Set your Claude API key

```bash
export ANTHROPIC_API_KEY=sk-ant-api03-...
```

On Windows (PowerShell):
```powershell
$env:ANTHROPIC_API_KEY = "sk-ant-api03-..."
```

### 3. Build and start

```bash
cd "spring-microservices-rabbit-otel -VectorDB"
docker compose up --build
```

First build takes ~10-15 minutes (Maven build + Python model download).
Subsequent starts: ~2-3 minutes.

### 4. Generate some telemetry

Once all services are up, hit the APIs to generate traces:

```bash
# Add a product to the catalog
curl -X POST http://localhost:8100/api/products \
  -H "Content-Type: application/json" \
  -d '{"name": "Laptop", "price": 999.99}'

# Place an order (use a product ID from above)
curl -X POST http://localhost:8102/api/orders \
  -H "Content-Type: application/json" \
  -d '{"productId": "<id>", "quantity": 1}'
```

Wait ~30 seconds for the telemetry pipeline to index the traces.

### 5. Open the RAG demo

→ **http://localhost:8200**

Try asking:
- "Are there any errors?"
- "Which service has the slowest operations?"
- "Show me the order placement flow and timings"
- "Give me a health summary of all services"

---

## All service URLs

| Service | URL |
|---|---|
| **RAG Demo UI** | http://localhost:8200 |
| Jaeger (traces) | http://localhost:16686 |
| Grafana | http://localhost:3000 (admin / verysecret) |
| RabbitMQ UI | http://localhost:15672 (guest / guest) |
| Prometheus | http://localhost:9090 |
| Catalog API | http://localhost:8100/swagger-ui.html |
| Order API | http://localhost:8102/swagger-ui.html |
| Inventory API | http://localhost:8101/swagger-ui.html |
| Payment API | http://localhost:8103/swagger-ui.html |
| Shipment API | http://localhost:8104/swagger-ui.html |
| Telemetry pipeline stats | http://localhost:9000/stats |

---

## Architecture diagram

```
                    ┌─────────────────────────────────┐
                    │        APP CLUSTER               │
                    │                                  │
  HTTP requests ──► │  catalog  order  inventory       │
                    │  payment  shipment               │
                    │       │ (OTel spans)             │
                    │       └──► PostgreSQL            │
                    └──────┬──────────────────────────┘
                           │ OTLP gRPC
                    ┌──────▼──────────────────────────┐
                    │     OBSERVABILITY CLUSTER        │
                    │                                  │
                    │  OTel Collector                  │
                    │   ├──► Jaeger   (traces UI)      │
                    │   ├──► Tempo    (trace storage)  │
                    │   ├──► Loki     (logs)           │
                    │   ├──► Prometheus (metrics)      │
                    │   └──► Grafana  (dashboards)     │
                    │         │                        │
                    │         └──► telemetry-pipeline  │
                    └──────────────┬──────────────────┘
                                   │ embed + store
                    ┌──────────────▼──────────────────┐
                    │          AI PIPELINE             │
                    │                                  │
                    │  pgvector (embeddings)           │
                    │       │                          │
                    │  rag-service ──► Claude API      │
                    │       │                          │
                    │  Demo UI (http://localhost:8200) │
                    └─────────────────────────────────┘
```

---

## Troubleshooting

**Services fail to start:** Postgres takes ~10s to init. The services have retry logic but if they crash, run `docker compose up` again — compose will restart only the failed services.

**No data in RAG UI:** Check `http://localhost:9000/stats`. If total is 0, make sure you've hit the service APIs to generate traces, and wait 30s for the pipeline poll cycle.

**Claude API errors:** Verify `ANTHROPIC_API_KEY` is set in your shell before running `docker compose up`.

**Out of memory:** The Python services each load a ~90MB embedding model. Ensure Docker Desktop has at least 6GB RAM allocated (Docker Desktop → Settings → Resources).
