-- Window-level error clusters written by Spark
CREATE TABLE IF NOT EXISTS error_clusters (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    service         TEXT NOT NULL,
    fingerprint     TEXT NOT NULL,
    template        TEXT,
    event_count     INT NOT NULL,
    host_count      INT,
    first_seen      TIMESTAMPTZ,
    last_seen       TIMESTAMPTZ,
    sample_messages JSONB,
    status          TEXT NOT NULL DEFAULT 'new',   -- new | summarized | merged | ignored
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_clusters_status ON error_clusters(status);
CREATE INDEX IF NOT EXISTS idx_clusters_window ON error_clusters(window_start);

-- LLM-generated incidents
CREATE TABLE IF NOT EXISTS incidents (
    id                BIGSERIAL PRIMARY KEY,
    fingerprint       TEXT NOT NULL,
    service           TEXT NOT NULL,
    title             TEXT NOT NULL,
    severity          TEXT NOT NULL,               -- P1..P4
    summary           TEXT,
    root_cause        TEXT,
    suggested_actions JSONB,
    event_count       INT NOT NULL DEFAULT 0,
    first_seen        TIMESTAMPTZ,
    last_seen         TIMESTAMPTZ,
    status            TEXT NOT NULL DEFAULT 'open', -- open | acknowledged | resolved
    llm_model         TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_incidents_fp ON incidents(fingerprint, status);

CREATE OR REPLACE VIEW error_rate_per_minute AS
SELECT window_start AS minute, service, SUM(event_count) AS errors
FROM error_clusters GROUP BY 1, 2;
