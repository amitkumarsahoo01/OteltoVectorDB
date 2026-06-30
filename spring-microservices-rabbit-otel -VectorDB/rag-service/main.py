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
from typing import Optional

import anthropic
import psycopg2
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pgvector.psycopg2 import register_vector
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

# ── Config ────────────────────────────────────────────────────────────────────
DB_URL           = os.getenv("DB_URL",           "postgresql://postgres:postgres@postgres:5432/telemetrydb")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL     = os.getenv("CLAUDE_MODEL",     "claude-haiku-4-5-20251001")
TOP_K            = int(os.getenv("TOP_K",        "12"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("rag-service")

log.info("Loading embedding model...")
EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
log.info("Model ready.")

claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

app = FastAPI(title="Telemetry RAG — powered by Claude")

# ── Schemas ───────────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str
    top_k: Optional[int] = None

class SourceItem(BaseModel):
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
                SELECT service_name, status, COUNT(*) AS cnt,
                       ROUND(AVG(duration_ms)) AS avg_ms,
                       MAX(telemetry_timestamp) AS latest
                FROM telemetry_embeddings
                GROUP BY service_name, status
                ORDER BY service_name, cnt DESC
            """)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {
        "total_records": total,
        "breakdown": [
            {
                "service":   r[0], "status": r[1],
                "count":     r[2], "avg_duration_ms": r[3],
                "latest":    str(r[4]),
            }
            for r in rows
        ],
    }

@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    k = req.top_k or TOP_K

    # 1. Embed the question
    q_vec = EMBED_MODEL.encode(req.question).tolist()

    # 2. Vector search in pgvector
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT service_name, operation_name, status, duration_ms,
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

    if not rows:
        return QueryResponse(
            question=req.question,
            answer=(
                "No telemetry data is available yet. "
                "Make sure the microservices are running and generating traces, "
                "then wait ~30 seconds for the pipeline to index them."
            ),
            sources=[],
            model=CLAUDE_MODEL,
        )

    # 3. Build context string
    sources = []
    context_lines = []
    for svc, op, status, dur_ms, raw_text, ts, sim in rows:
        sources.append(SourceItem(
            service=svc, operation=op, status=status,
            duration_ms=dur_ms, timestamp=str(ts),
            similarity=round(float(sim), 3),
        ))
        context_lines.append(f"[{ts}] {raw_text}")

    context = "\n".join(context_lines)

    # 4. Ask Claude
    system_prompt = (
        "You are an expert Site Reliability Engineer (SRE) analysing distributed "
        "trace telemetry from a Spring Boot microservices application.\n\n"
        "The application is an online store with five services: "
        "catalog-service (products), order-service (order lifecycle), "
        "inventory-service (stock), payment-service (payments), "
        "shipment-service (delivery). They communicate asynchronously via RabbitMQ.\n\n"
        "Telemetry is collected with OpenTelemetry and stored as vector embeddings. "
        "The context below is the most semantically similar telemetry to the user's question.\n\n"
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

    msg = claude.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1024,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    return QueryResponse(
        question=req.question,
        answer=msg.content[0].text,
        sources=sources,
        model=CLAUDE_MODEL,
    )

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
      d.innerHTML = `<span class="svc">${s.service || '?'}</span> · ${s.operation || '?'}<br>
        <span class="${statusClass}">${s.status || 'OK'}</span> · ${s.duration_ms ?? '?'}ms · sim ${s.similarity}`;
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
