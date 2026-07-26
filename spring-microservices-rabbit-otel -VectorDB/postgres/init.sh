#!/bin/bash
set -e

# Create all service databases
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE catalogdb;
    CREATE DATABASE orderdb;
    CREATE DATABASE inventorydb;
    CREATE DATABASE paymentdb;
    CREATE DATABASE shipmentdb;
    CREATE DATABASE telemetrydb;
EOSQL

# Set up pgvector in telemetrydb and create the embeddings table
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "telemetrydb" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS vector;

    CREATE TABLE IF NOT EXISTS telemetry_embeddings (
        id          SERIAL PRIMARY KEY,
        source_type VARCHAR(20)  NOT NULL,           -- 'trace', 'log', or 'metric'
        source_id   VARCHAR(512) NOT NULL UNIQUE,    -- trace_id+span_id or log hash
        trace_id        VARCHAR(64),                 -- OTel trace id, when known
        service_name    VARCHAR(255),
        operation_name  VARCHAR(512),
        status          VARCHAR(50),
        duration_ms     BIGINT,
        attributes      JSONB,
        raw_text        TEXT NOT NULL,
        embedding       vector(384),                 -- all-MiniLM-L6-v2 dimension
        created_at      TIMESTAMPTZ DEFAULT NOW(),
        telemetry_timestamp TIMESTAMPTZ
    );

    -- HNSW, not IVFFlat: IVFFlat computes its cluster centroids at index-build
    -- time, so building it here (on an empty table) yields garbage centroids and
    -- similarity queries miss rows. HNSW has no such training step and works
    -- correctly regardless of table size.
    CREATE INDEX IF NOT EXISTS idx_telemetry_embedding
        ON telemetry_embeddings USING hnsw (embedding vector_cosine_ops);

    CREATE INDEX IF NOT EXISTS idx_telemetry_source_id  ON telemetry_embeddings (source_id);
    CREATE INDEX IF NOT EXISTS idx_telemetry_service    ON telemetry_embeddings (service_name);
    CREATE INDEX IF NOT EXISTS idx_telemetry_status     ON telemetry_embeddings (status);
    CREATE INDEX IF NOT EXISTS idx_telemetry_ts         ON telemetry_embeddings (telemetry_timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_telemetry_trace_id   ON telemetry_embeddings (trace_id);
EOSQL

echo ">>> Postgres init complete: catalogdb, orderdb, inventorydb, paymentdb, shipmentdb, telemetrydb created."
