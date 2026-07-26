"""
RAG Service — Telemetry Q&A powered by Claude
----------------------------------------------
POST /query  →  embed question → search pgvector → ask Claude → return answer
GET  /       →  demo chat UI (great for leadership demo)
GET  /stats  →  telemetry database summary
GET  /health →  liveness check
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import anthropic
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pgvector.psycopg2 import register_vector
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL           = os.getenv("DB_URL",           "postgresql://postgres:postgres@postgres:5432/telemetrydb")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL     = os.getenv("CLAUDE_MODEL",     "claude-haiku-4-5-20251001")
TOP_K            = int(os.getenv("TOP_K",        "12"))
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "rag-service")

# ── OpenTelemetry setup (traces, metrics, logs → OTel Collector) ───────────────
# Endpoint / protocol are picked up from the standard OTEL_EXPORTER_OTLP_*
# env vars (set in docker-compose), same as the OTLP exporters used below.
resource = Resource.create({"service.name": OTEL_SERVICE_NAME})

tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer("rag-service")

meter_provider = MeterProvider(
    resource=resource,
    metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
)
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("rag-service")

logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
set_logger_provider(logger_provider)

# Inject trace_id/span_id into every log record's format and ship logs both to
# the console (for `docker logs`) and to the collector via OTLP.
LoggingInstrumentor().instrument(
    set_logging_format=True,
    logging_format="%(asctime)s [%(levelname)s] trace_id=%(otelTraceID)s span_id=%(otelSpanID)s %(name)s: %(message)s",
)
logging.getLogger().addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))
log = logging.getLogger("rag-service")

Psycopg2Instrumentor().instrument()

# Custom metrics — one instrument per stage of a /query transaction.
query_counter = meter.create_counter(
    "rag.queries", unit="1", description="Number of /query requests received")
query_error_counter = meter.create_counter(
    "rag.query.errors", unit="1", description="Number of /query requests that raised an error")
query_duration = meter.create_histogram(
    "rag.query.duration", unit="ms", description="End-to-end /query latency")
embedding_duration = meter.create_histogram(
    "rag.embedding.duration", unit="ms", description="Time to embed the question")
vector_search_duration = meter.create_histogram(
    "rag.vector_search.duration", unit="ms", description="pgvector similarity search latency")
claude_duration = meter.create_histogram(
    "rag.claude.duration", unit="ms", description="Claude completion latency")
sources_returned = meter.create_histogram(
    "rag.sources.count", unit="1", description="Number of telemetry sources returned per query")

log.info("Loading embedding model...")
EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
log.info("Model ready.")

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Flush any batched spans/metrics/logs before the process exits.
    tracer_provider.shutdown()
    meter_provider.shutdown()
    logger_provider.shutdown()

app = FastAPI(title="Telemetry RAG — powered by Claude", lifespan=lifespan)
FastAPIInstrumentor.instrument_app(app, excluded_urls="health")

# ── Schemas ───────────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str
    top_k: Optional[int] = None

class SourceItem(BaseModel):
    source_type: Optional[str]
    service: Optional[str]
    operation: Optional[str]
    status: Optional[str]
    duration_ms: Optional[int]
    timestamp: Optional[str]
    similarity: float

class QueryResponse(BaseModel):
    question: str
    answer: str
    sources: list[SourceItem]
    model: str

# ── DB helper ─────────────────────────────────────────────────────────────────
def get_conn():
    conn = psycopg2.connect(DB_URL)
    register_vector(conn)
    return conn

# ── Intent routing ───────────────────────────────────────────────────────────
# Top-k vector similarity search cannot answer "how many" (it only ever returns
# k rows, so counts silently cap at k) or "map the full flow" (the k rows most
# similar to the question are rarely the same rows that belong to one order's
# trace). Both need a real SQL query against the whole table instead.
COUNT_KEYWORDS = (
    "how many", "count of", "count the", " count?", "number of", "total number", "total count",
    "categorize", "category", "categories", "break down", "breakdown",
    "failure reason", "reasons for failure", "what reasons", "why are orders failing",
    "why orders fail", "why do orders fail",
)
FLOW_KEYWORDS = (
    "flow", "trace the", "each step", "step by step", "step-by-step",
    "journey", "end to end", "end-to-end", "walk through", "walk me through",
    "full lifecycle", "complete lifecycle",
)
SERVICE_ALIASES = {
    "catalog": "catalog-service", "product": "catalog-service",
    "order": "order-service",
    "inventory": "inventory-service", "stock": "inventory-service",
    "payment": "payment-service",
    "shipment": "shipment-service", "shipping": "shipment-service", "delivery": "shipment-service",
}

def detect_intent(question: str) -> str:
    q = question.lower()
    if any(k in q for k in FLOW_KEYWORDS):
        return "flow"
    if any(k in q for k in COUNT_KEYWORDS):
        return "count"
    return "semantic"

def extract_service_filter(question: str) -> Optional[str]:
    q = question.lower()
    for alias, svc in SERVICE_ALIASES.items():
        if alias in q:
            return svc
    return None

def run_count_query(conn, service: Optional[str]) -> list[str]:
    """Exact COUNT/GROUP BY over the whole table — not a top-k sample."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT trace_id)
            FROM   telemetry_embeddings
            WHERE  source_type = 'trace'
              AND  (%(service)s::text IS NULL OR service_name = %(service)s)
            """,
            {"service": service},
        )
        total_records, total_traces = cur.fetchone()

        cur.execute(
            """
            SELECT service_name, operation_name, status, COUNT(*) AS cnt,
                   COUNT(DISTINCT trace_id) AS distinct_traces
            FROM   telemetry_embeddings
            WHERE  source_type = 'trace'
              AND  (%(service)s::text IS NULL OR service_name = %(service)s)
            GROUP BY service_name, operation_name, status
            ORDER BY cnt DESC
            LIMIT 30
            """,
            {"service": service},
        )
        breakdown = cur.fetchall()

        # Business-level failure reasons (e.g. "Card declined", "INSUFFICIENT_STOCK")
        # only ever appear in log text, never in a trace span's OTel status — a
        # payment decline or an out-of-stock rejection is a normal return value,
        # not a Java exception, so the span itself stays status=OK. Extract the
        # reason out of the log body instead of relying on span/log status.
        # Deliberately NOT filtered by `service`: a "why are orders failing"
        # question maps to order-service via SERVICE_ALIASES, but the actual
        # failure reasons are logged by payment-service/inventory-service —
        # the per-row service_name in the output keeps that visible anyway.
        cur.execute(
            """
            SELECT service_name,
                   (regexp_match(raw_text, 'reason=([^\\]\\)]+)'))[1] AS reason,
                   COUNT(*) AS cnt
            FROM   telemetry_embeddings
            WHERE  source_type = 'log'
              AND  raw_text ~ 'reason='
            GROUP BY service_name, reason
            ORDER BY cnt DESC
            LIMIT 30
            """
        )
        reason_breakdown = cur.fetchall()

    lines = [
        "EXACT AGGREGATE COUNT — computed with SQL COUNT/GROUP BY over the "
        "entire telemetry_embeddings table (not a similarity-search sample).",
        f"Total matching trace records: {total_records}, across {total_traces} distinct traces.",
        "Breakdown by service | operation | status | record_count | distinct_trace_count:",
    ]
    for svc, op, status, cnt, distinct in breakdown:
        lines.append(f"- {svc} | {op} | {status} | records={cnt} | distinct_traces={distinct}")

    if reason_breakdown:
        lines.append("")
        lines.append(
            "FAILURE REASON BREAKDOWN — exact counts extracted from log records "
            "(business-level failure reasons like a declined card or an "
            "out-of-stock rejection never show up as trace/span errors, only in logs):"
        )
        lines.append("service | reason | count:")
        for svc, reason, cnt in reason_breakdown:
            lines.append(f"- {svc} | {reason} | count={cnt}")

    return lines

def run_flow_query(conn) -> tuple[Optional[str], list]:
    """Find the trace touching the most services and return every one of its
    spans/logs in time order — a complete, real order flow, not a sample."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trace_id
            FROM   telemetry_embeddings
            WHERE  source_type = 'trace' AND trace_id IS NOT NULL
            GROUP BY trace_id
            ORDER BY COUNT(DISTINCT service_name) DESC, MAX(telemetry_timestamp) DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None, []
        trace_id = row[0]

        cur.execute(
            """
            SELECT source_type, service_name, operation_name, status, duration_ms,
                   raw_text, telemetry_timestamp
            FROM   telemetry_embeddings
            WHERE  trace_id = %s
            ORDER BY telemetry_timestamp ASC
            """,
            (trace_id,),
        )
        rows = cur.fetchall()
    return trace_id, rows

# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "model": CLAUDE_MODEL}

@app.get("/stats")
def stats():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM telemetry_embeddings")
            total = cur.fetchone()[0]
            cur.execute("""
                SELECT source_type, COUNT(*) AS cnt
                FROM telemetry_embeddings
                GROUP BY source_type
                ORDER BY cnt DESC
            """)
            by_type = cur.fetchall()
            cur.execute("""
                SELECT source_type, service_name, status, COUNT(*) AS cnt,
                       ROUND(AVG(duration_ms)) AS avg_ms,
                       MAX(telemetry_timestamp) AS latest
                FROM telemetry_embeddings
                GROUP BY source_type, service_name, status
                ORDER BY service_name, cnt DESC
            """)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {
        "total_records": total,
        "by_type": {r[0]: r[1] for r in by_type},
        "breakdown": [
            {
                "source_type": r[0], "service": r[1], "status": r[2],
                "count":       r[3], "avg_duration_ms": r[4],
                "latest":      str(r[5]),
            }
            for r in rows
        ],
    }

@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    k = req.top_k or TOP_K
    query_counter.add(1)
    span = trace.get_current_span()
    span.set_attribute("rag.top_k", k)
    span.set_attribute("rag.question_length", len(req.question))

    request_start = time.perf_counter()
    try:
        intent = detect_intent(req.question)
        span.set_attribute("rag.intent", intent)
        log.info(f"Query received (intent={intent}, top_k={k}): {req.question!r}")

        sources: list[SourceItem] = []
        data_note = ""

        if intent == "count":
            # 1'. Exact SQL aggregate — bypasses vector search / top_k entirely.
            service = extract_service_filter(req.question)
            span.set_attribute("rag.count.service_filter", service or "")
            with tracer.start_as_current_span("count_query") as count_span:
                t0 = time.perf_counter()
                conn = get_conn()
                try:
                    context_lines = run_count_query(conn, service)
                finally:
                    conn.close()
                count_span.set_attribute("rag.count.duration_ms", (time.perf_counter() - t0) * 1000)

        elif intent == "flow":
            # 1'. Every span/log for one representative trace, in time order —
            # not the top-k rows most similar to the question text.
            with tracer.start_as_current_span("flow_query") as flow_span:
                t0 = time.perf_counter()
                conn = get_conn()
                try:
                    trace_id, rows = run_flow_query(conn)
                finally:
                    conn.close()
                flow_span.set_attribute("rag.flow.trace_id", trace_id or "")
                flow_span.set_attribute("rag.sources.count", len(rows))
                flow_span.set_attribute("rag.flow.duration_ms", (time.perf_counter() - t0) * 1000)

            if not trace_id:
                log.warning("No trace with a known trace_id found for flow query")
                sources_returned.record(0)
                return QueryResponse(
                    question=req.question,
                    answer="No complete order trace is available yet in telemetry.",
                    sources=[],
                    model=CLAUDE_MODEL,
                )

            data_note = f"COMPLETE ORDERED FLOW for representative trace_id={trace_id} — every span/log recorded for this one order, in chronological order (not a similarity-search sample):"
            context_lines = [data_note]
            for src_type, svc, op, status, dur_ms, raw_text, ts in rows:
                sources.append(SourceItem(
                    source_type=src_type, service=svc, operation=op, status=status,
                    duration_ms=dur_ms, timestamp=str(ts), similarity=1.0,
                ))
                context_lines.append(f"[{ts}] ({src_type}) {raw_text}")

        else:
            # 1. Embed the question
            with tracer.start_as_current_span("embed_question") as embed_span:
                t0 = time.perf_counter()
                q_vec = EMBED_MODEL.encode(req.question).tolist()
                embed_ms = (time.perf_counter() - t0) * 1000
                embed_span.set_attribute("rag.embedding.dimensions", len(q_vec))
            embedding_duration.record(embed_ms)
            log.info(f"Embedded question in {embed_ms:.1f}ms")

            # 2. Vector search in pgvector
            with tracer.start_as_current_span("vector_search") as search_span:
                t0 = time.perf_counter()
                conn = get_conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT source_type, service_name, operation_name, status, duration_ms,
                                   raw_text, telemetry_timestamp,
                                   1 - (embedding <=> %s::vector) AS similarity
                            FROM   telemetry_embeddings
                            ORDER  BY embedding <=> %s::vector
                            LIMIT  %s
                            """,
                            (q_vec, q_vec, k),
                        )
                        rows = cur.fetchall()
                finally:
                    conn.close()
                search_ms = (time.perf_counter() - t0) * 1000
                search_span.set_attribute("rag.sources.count", len(rows))
            vector_search_duration.record(search_ms)
            log.info(f"Vector search returned {len(rows)} rows in {search_ms:.1f}ms")

            if not rows:
                log.warning("No telemetry rows matched this question")
                sources_returned.record(0)
                return QueryResponse(
                    question=req.question,
                    answer=(
                        "No telemetry data is available yet. "
                        "Make sure the microservices are running and generating "
                        "traces, logs, and metrics, then wait ~30 seconds for the "
                        "pipeline to index them."
                    ),
                    sources=[],
                    model=CLAUDE_MODEL,
                )

            # 3. Build context string
            context_lines = []
            for src_type, svc, op, status, dur_ms, raw_text, ts, sim in rows:
                sources.append(SourceItem(
                    source_type=src_type, service=svc, operation=op, status=status,
                    duration_ms=dur_ms, timestamp=str(ts),
                    similarity=round(float(sim), 3),
                ))
                context_lines.append(f"[{ts}] ({src_type}) {raw_text}")

        context = "\n".join(context_lines)

        # 4. Ask Claude
        context_descriptions = {
            "count": (
                "The context below is an EXACT aggregate computed with SQL COUNT/GROUP BY "
                "over the entire telemetry table — it is authoritative, not a sample. Use "
                "these numbers directly; do not estimate or count context lines yourself."
            ),
            "flow": (
                "The context below is the COMPLETE set of spans/logs for one real, "
                "representative order trace, in chronological order — it is not a sample, "
                "and nothing from that trace has been left out."
            ),
            "semantic": (
                "The context below is the most semantically similar telemetry to the "
                "user's question; each line may be a trace span, a log record, or a "
                "metric data point. It is a sample (top matches only), not a full-table "
                "result — do not state exact totals/counts from it."
            ),
        }
        system_prompt = (
            "You are an expert Site Reliability Engineer (SRE) analysing distributed "
            "telemetry (traces, logs, and metrics) from a Spring Boot microservices "
            "application.\n\n"
            "The application is an online store with five services: "
            "catalog-service (products), order-service (order lifecycle), "
            "inventory-service (stock), payment-service (payments), "
            "shipment-service (delivery). They communicate asynchronously via RabbitMQ.\n\n"
            "Telemetry is collected with OpenTelemetry and stored as vector embeddings.\n\n"
            f"{context_descriptions[intent]}\n\n"
            "Instructions:\n"
            "- Answer directly and specifically using the telemetry data provided.\n"
            "- Call out error patterns, slow operations, or anomalies you notice.\n"
            "- If the data is insufficient to answer fully, say so.\n"
            "- Keep answers concise — this is for a leadership demo."
        )

        user_prompt = (
            f"Relevant telemetry context:\n\n{context}\n\n"
            f"Question: {req.question}"
        )

        with tracer.start_as_current_span("claude_completion") as claude_span:
            t0 = time.perf_counter()
            msg = claude.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1024,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            claude_ms = (time.perf_counter() - t0) * 1000
            claude_span.set_attribute("rag.claude.model", CLAUDE_MODEL)
            claude_span.set_attribute("rag.claude.input_tokens", msg.usage.input_tokens)
            claude_span.set_attribute("rag.claude.output_tokens", msg.usage.output_tokens)
        claude_duration.record(claude_ms)
        sources_returned.record(len(sources))
        log.info(
            f"Claude responded in {claude_ms:.1f}ms "
            f"({msg.usage.input_tokens} in / {msg.usage.output_tokens} out tokens)"
        )

        return QueryResponse(
            question=req.question,
            answer=msg.content[0].text,
            sources=sources,
            model=CLAUDE_MODEL,
        )
    except Exception:
        query_error_counter.add(1)
        log.exception("Query failed")
        raise
    finally:
        query_duration.record((time.perf_counter() - request_start) * 1000)

# ── Demo UI ───────────────────────────────────────────────────────────────────
DEMO_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Telemetry RAG — Demo</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, -apple-system, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
  header { padding: 20px 32px; border-bottom: 1px solid #1e2a3a; display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 18px; font-weight: 600; color: #f8fafc; }
  header span { font-size: 12px; background: #1a2744; color: #60a5fa; padding: 3px 10px; border-radius: 20px; }
  .main { flex: 1; display: flex; gap: 0; }
  .chat { flex: 1; display: flex; flex-direction: column; padding: 24px 32px; max-width: 760px; }
  .messages { flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 16px; margin-bottom: 20px; min-height: 300px; }
  .msg { padding: 12px 16px; border-radius: 12px; font-size: 14px; line-height: 1.6; max-width: 90%; }
  .msg.user { background: #1e3a5f; align-self: flex-end; color: #bfdbfe; }
  .msg.assistant { background: #1a2333; align-self: flex-start; color: #e2e8f0; white-space: pre-wrap; }
  .msg.system { background: #1a1f2e; align-self: center; color: #64748b; font-size: 12px; font-style: italic; }
  .input-row { display: flex; gap: 8px; }
  .input-row input { flex: 1; padding: 12px 16px; border-radius: 8px; border: 1px solid #1e2a3a; background: #151b28; color: #e2e8f0; font-size: 14px; outline: none; }
  .input-row input:focus { border-color: #3b82f6; }
  .input-row button { padding: 12px 20px; border-radius: 8px; background: #3b82f6; color: white; border: none; font-size: 14px; font-weight: 500; cursor: pointer; }
  .input-row button:hover { background: #2563eb; }
  .input-row button:disabled { background: #1e2a3a; color: #4b5563; cursor: not-allowed; }
  .suggestions { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
  .suggestions button { padding: 6px 12px; border-radius: 20px; border: 1px solid #1e3a5f; background: transparent; color: #60a5fa; font-size: 12px; cursor: pointer; }
  .suggestions button:hover { background: #1e3a5f; }
  .sidebar { width: 280px; border-left: 1px solid #1e2a3a; padding: 20px; display: flex; flex-direction: column; gap: 16px; }
  .sidebar h2 { font-size: 13px; font-weight: 600; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }
  .stat-card { background: #151b28; border-radius: 8px; padding: 12px 14px; }
  .stat-card .label { font-size: 11px; color: #64748b; margin-bottom: 4px; }
  .stat-card .value { font-size: 22px; font-weight: 600; color: #f8fafc; }
  .sources { margin-top: 8px; }
  .source-item { font-size: 11px; color: #64748b; border-left: 2px solid #1e3a5f; padding: 4px 8px; margin-bottom: 4px; line-height: 1.4; }
  .source-item .svc { color: #60a5fa; font-weight: 500; }
  .source-item .type { display: inline-block; font-size: 9px; font-weight: 600; letter-spacing: 0.5px; color: #0f1117; background: #475569; border-radius: 3px; padding: 1px 4px; margin-right: 4px; vertical-align: middle; }
  .source-item .ok { color: #34d399; }
  .source-item .err { color: #f87171; }
  .spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #1e3a5f; border-top-color: #3b82f6; border-radius: 50%; animation: spin 0.6s linear infinite; vertical-align: middle; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<header>
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#3b82f6" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
  <h1>Telemetry Intelligence</h1>
  <span>Powered by Claude</span>
</header>
<div class="main">
  <div class="chat">
    <div class="messages" id="messages">
      <div class="msg system">Ask any question about your microservices telemetry data.</div>
    </div>
    <div class="suggestions">
      <button onclick="ask('Are there any errors in the last few minutes?')">Any errors?</button>
      <button onclick="ask('Which service has the slowest response times?')">Slowest service?</button>
      <button onclick="ask('Show me the order placement flow and how long each step takes')">Order flow timing</button>
      <button onclick="ask('What operations are failing and why?')">What is failing?</button>
      <button onclick="ask('Give me a health summary of all services')">Health summary</button>
    </div>
    <div class="input-row">
      <input id="q" type="text" placeholder="Ask about your telemetry data..." onkeydown="if(event.key==='Enter') sendQuery()">
      <button id="btn" onclick="sendQuery()">Ask Claude</button>
    </div>
  </div>
  <div class="sidebar">
    <div>
      <h2>Telemetry Stats</h2>
      <div style="display:flex;flex-direction:column;gap:8px;margin-top:10px" id="stats-area">
        <div class="stat-card"><div class="label">Total spans indexed</div><div class="value" id="s-total">—</div></div>
        <div class="stat-card"><div class="label">Services monitored</div><div class="value" id="s-svcs">—</div></div>
      </div>
    </div>
    <div>
      <h2>Last answer sources</h2>
      <div id="sources-area" class="sources"><div style="font-size:12px;color:#4b5563">Sources appear after your first query.</div></div>
    </div>
  </div>
</div>
<script>
async function loadStats() {
  try {
    const r = await fetch('/stats');
    const d = await r.json();
    document.getElementById('s-total').textContent = d.total_records.toLocaleString();
    const svcs = new Set(d.breakdown.map(b => b.service)).size;
    document.getElementById('s-svcs').textContent = svcs;
  } catch(e) {}
}
loadStats();
setInterval(loadStats, 15000);

function ask(q) {
  document.getElementById('q').value = q;
  sendQuery();
}

async function sendQuery() {
  const input = document.getElementById('q');
  const btn   = document.getElementById('btn');
  const q     = input.value.trim();
  if (!q) return;

  const msgs = document.getElementById('messages');

  // User bubble
  const userEl = document.createElement('div');
  userEl.className = 'msg user';
  userEl.textContent = q;
  msgs.appendChild(userEl);

  // Loading bubble
  const loadEl = document.createElement('div');
  loadEl.className = 'msg assistant';
  loadEl.innerHTML = '<span class="spinner"></span> Searching telemetry and asking Claude…';
  msgs.appendChild(loadEl);
  msgs.scrollTop = msgs.scrollHeight;

  input.value = '';
  btn.disabled = true;

  try {
    const res = await fetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: q }),
    });
    const data = await res.json();

    loadEl.textContent = data.answer;

    // Render sources in sidebar
    const sa = document.getElementById('sources-area');
    sa.innerHTML = '';
    data.sources.slice(0, 8).forEach(s => {
      const d = document.createElement('div');
      d.className = 'source-item';
      const statusClass = (s.status || '').includes('ERROR') ? 'err' : 'ok';
      const dur = (s.duration_ms ?? null) !== null ? ` · ${s.duration_ms}ms` : '';
      const type = (s.source_type || 'trace').toUpperCase();
      d.innerHTML = `<span class="type">${type}</span> <span class="svc">${s.service || '?'}</span> · ${s.operation || '—'}<br>
        <span class="${statusClass}">${s.status || 'OK'}</span>${dur} · sim ${s.similarity}`;
      sa.appendChild(d);
    });

  } catch(e) {
    loadEl.textContent = 'Error contacting the RAG service. Check that ANTHROPIC_API_KEY is set.';
  }

  btn.disabled = false;
  msgs.scrollTop = msgs.scrollHeight;
}
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
def demo_ui():
    return DEMO_HTML

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8200)
