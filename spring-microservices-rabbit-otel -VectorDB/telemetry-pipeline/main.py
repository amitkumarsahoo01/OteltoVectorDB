"""
Telemetry Pipeline Service
--------------------------
Receives OTLP trace data forwarded by the OTel Collector, embeds each span
as a searchable text using sentence-transformers, and stores it in pgvector.

Also polls the Jaeger HTTP API every POLL_INTERVAL seconds to catch any spans
that arrived before this service was ready.
"""

import asyncio
import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import psycopg2
from fastapi import FastAPI, Request, Response
from pgvector.psycopg2 import register_vector
from psycopg2.extras import Json
from sentence_transformers import SentenceTransformer

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL       = os.getenv("DB_URL",       "postgresql://postgres:postgres@postgres:5432/telemetrydb")
JAEGER_URL   = os.getenv("JAEGER_URL",   "http://jaeger:16686")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))   # seconds between Jaeger polls
STARTUP_DELAY = int(os.getenv("STARTUP_DELAY", "20"))   # wait for deps to start

SERVICES = [
    "catalog-service", "order-service", "inventory-service",
    "payment-service", "shipment-service",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("telemetry-pipeline")

# ── Embedding model (loaded once at startup) ──────────────────────────────────
log.info("Loading sentence-transformer model...")
EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
log.info("Model ready.")

# ── DB helpers ────────────────────────────────────────────────────────────────
def get_conn():
    conn = psycopg2.connect(DB_URL)
    register_vector(conn)
    return conn

def store_span(conn, trace_id: str, span: dict, service_name: str) -> bool:
    """Embed one span and upsert into telemetry_embeddings. Returns True if new."""
    span_id   = span.get("spanID", "")
    source_id = f"trace-{trace_id}-{span_id}"

    op          = span.get("operationName", "unknown")
    duration_us = span.get("duration", 0)
    start_us    = span.get("startTime", 0)
    ts          = datetime.fromtimestamp(start_us / 1e6, tz=timezone.utc) if start_us else None

    tags   = {t["key"]: t["value"] for t in span.get("tags", [])}
    error  = bool(tags.get("error", False))
    status = "ERROR" if error else tags.get("otel.status_code", "OK")

    # Build human-readable text for embedding
    parts = [
        f"Service: {service_name}",
        f"Operation: {op}",
        f"Status: {status}",
        f"Duration: {duration_us / 1000:.1f}ms",
    ]
    http_method = tags.get("http.method", "")
    http_target = tags.get("http.target", tags.get("http.url", ""))
    http_status = tags.get("http.status_code", "")
    if http_method:
        parts.append(f"HTTP: {http_method} {http_target} → {http_status}")

    exc_msg = tags.get("exception.message", tags.get("error.message", ""))
    if exc_msg:
        parts.append(f"Error: {exc_msg}")

    db_table = tags.get("db.sql.table", tags.get("db.name", ""))
    if db_table:
        parts.append(f"DB: {db_table}")

    mq_dest = tags.get("messaging.destination", "")
    if mq_dest:
        parts.append(f"Queue: {mq_dest}")

    raw_text  = " | ".join(parts)
    embedding = EMBED_MODEL.encode(raw_text).tolist()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO telemetry_embeddings
                (source_type, source_id, service_name, operation_name, status,
                 duration_ms, attributes, raw_text, embedding, telemetry_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id) DO NOTHING
            """,
            (
                "trace", source_id, service_name, op, status,
                duration_us // 1000, Json(tags), raw_text, embedding, ts,
            ),
        )
        inserted = cur.rowcount == 1
    conn.commit()
    return inserted

# ── OTLP HTTP receiver (called by OTel Collector) ─────────────────────────────
def decode_otlp_traces(body: bytes) -> list[dict]:
    """Parse OTLP protobuf ExportTraceServiceRequest → list of (service, span) dicts."""
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
    req = trace_service_pb2.ExportTraceServiceRequest()
    req.ParseFromString(body)

    spans_out = []
    for resource_span in req.resource_spans:
        # Extract service.name from resource attributes
        service_name = "unknown"
        for attr in resource_span.resource.attributes:
            if attr.key == "service.name":
                service_name = attr.value.string_value
                break

        for scope_span in resource_span.scope_spans:
            for span in scope_span.spans:
                trace_id_hex = span.trace_id.hex()
                span_id_hex  = span.span_id.hex()
                op_name      = span.name
                start_ns     = span.start_time_unix_nano
                end_ns       = span.end_time_unix_nano
                duration_us  = (end_ns - start_ns) // 1000
                start_us     = start_ns // 1000

                tags = []
                for attr in span.attributes:
                    val = attr.value
                    kind = val.WhichOneof("value")
                    if kind == "string_value":
                        tags.append({"key": attr.key, "value": val.string_value})
                    elif kind == "int_value":
                        tags.append({"key": attr.key, "value": val.int_value})
                    elif kind == "bool_value":
                        tags.append({"key": attr.key, "value": val.bool_value})
                    elif kind == "double_value":
                        tags.append({"key": attr.key, "value": val.double_value})

                spans_out.append({
                    "service": service_name,
                    "span": {
                        "traceID":       trace_id_hex,
                        "spanID":        span_id_hex,
                        "operationName": op_name,
                        "duration":      duration_us,
                        "startTime":     start_us,
                        "tags":          tags,
                    },
                })
    return spans_out

# ── Jaeger poll (catch historical / missed spans) ─────────────────────────────
async def poll_jaeger():
    end   = datetime.now(timezone.utc)
    start = end - timedelta(seconds=POLL_INTERVAL + 10)
    start_us = int(start.timestamp() * 1e6)
    end_us   = int(end.timestamp() * 1e6)

    conn    = get_conn()
    total   = 0
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            for svc in SERVICES:
                try:
                    r = await client.get(
                        f"{JAEGER_URL}/api/traces",
                        params={"service": svc, "start": start_us, "end": end_us, "limit": 500},
                    )
                    if r.status_code != 200:
                        continue
                    data = r.json().get("data", [])
                    for trace in data:
                        tid = trace.get("traceID", "")
                        for span in trace.get("spans", []):
                            if store_span(conn, tid, span, svc):
                                total += 1
                except Exception as exc:
                    log.warning(f"Jaeger poll error for {svc}: {exc}")
    finally:
        conn.close()

    if total:
        log.info(f"Jaeger poll: stored {total} new spans")

# ── Background polling loop ───────────────────────────────────────────────────
async def polling_loop():
    await asyncio.sleep(STARTUP_DELAY)
    log.info(f"Starting Jaeger polling every {POLL_INTERVAL}s")
    while True:
        try:
            await poll_jaeger()
        except Exception as exc:
            log.error(f"Polling loop error: {exc}")
        await asyncio.sleep(POLL_INTERVAL)

# ── FastAPI app ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(polling_loop())
    yield

app = FastAPI(title="Telemetry Pipeline", lifespan=lifespan)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/v1/traces")
async def receive_traces(request: Request):
    """OTLP HTTP traces endpoint — called by OTel Collector."""
    body = await request.body()
    try:
        spans_data = decode_otlp_traces(body)
        conn  = get_conn()
        count = 0
        try:
            for item in spans_data:
                if store_span(conn, item["span"]["traceID"], item["span"], item["service"]):
                    count += 1
        finally:
            conn.close()
        if count:
            log.info(f"OTLP push: stored {count} new spans")
    except Exception as exc:
        log.error(f"OTLP decode error: {exc}")
    # Always return 200 so OTel Collector doesn't retry endlessly
    return Response(status_code=200)

@app.get("/stats")
def stats():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT service_name, status, COUNT(*) AS cnt
                FROM telemetry_embeddings
                GROUP BY service_name, status
                ORDER BY service_name, cnt DESC
                """
            )
            rows = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM telemetry_embeddings")
            total = cur.fetchone()[0]
    finally:
        conn.close()
    return {
        "total": total,
        "breakdown": [{"service": r[0], "status": r[1], "count": r[2]} for r in rows],
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
