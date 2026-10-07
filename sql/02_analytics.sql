-- =====================================================================
-- 02_analytics.sql  -  Analytics layer (idempotent: safe to run many times)
-- Run on an existing DB:  docker compose exec -T postgres psql -U pipeline -d incidents < sql/02_analytics.sql
-- =====================================================================

-- 1) New columns needed for MTTR and root-cause analysis
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS resolved_at          TIMESTAMPTZ;
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS root_cause_category  TEXT;
CREATE INDEX IF NOT EXISTS idx_incidents_first_seen ON incidents(first_seen);

-- 2) "Gold" table filled by the Spark batch job (Delta Lake -> Postgres)
CREATE TABLE IF NOT EXISTS log_stats_hourly (
    hour        TIMESTAMPTZ NOT NULL,
    service     TEXT        NOT NULL,
    level       TEXT        NOT NULL,
    fingerprint TEXT,
    template    TEXT,
    log_count   BIGINT      NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_log_stats_hour ON log_stats_hourly(hour);

-- 3) Error-type classifier (rule based, easy to explain in an interview)
CREATE OR REPLACE FUNCTION classify_error(msg TEXT) RETURNS TEXT AS $$
    SELECT CASE
        WHEN msg ILIKE '%timed out%' OR msg ILIKE '%timeout%'                          THEN 'Timeout'
        WHEN msg ILIKE '%connection%'                                                  THEN 'Connection'
        WHEN msg ILIKE '%outofmemory%' OR msg ILIKE '%heap%' OR msg ILIKE '%memory%'   THEN 'Memory'
        WHEN msg ILIKE '%disk%' OR msg ILIKE '%no space%'                              THEN 'Disk'
        WHEN msg ILIKE '%parse%' OR msg ILIKE '%payload%'                              THEN 'Parsing'
        ELSE 'Other'
    END
$$ LANGUAGE sql IMMUTABLE;

-- 4) Incident view: adds criticality, resolution state, time-to-resolve, error type
CREATE OR REPLACE VIEW v_incidents AS
SELECT i.id,
       i.service,
       i.title,
       i.severity,
       CASE WHEN i.severity IN ('P1','P2') THEN 'Critical' ELSE 'Non-critical' END   AS criticality,
       COALESCE(i.root_cause_category, 'Unknown')                                   AS root_cause_category,
       i.root_cause,
       i.event_count,
       i.first_seen,
       i.last_seen,
       i.created_at,
       i.resolved_at,
       i.status,
       CASE WHEN i.status = 'resolved' THEN 'Resolved' ELSE 'Unresolved' END        AS resolution_state,
       CASE WHEN i.resolved_at IS NOT NULL
            THEN EXTRACT(EPOCH FROM (i.resolved_at - i.first_seen)) / 60.0 END      AS ttr_minutes,
       classify_error(t.template)                                                   AS error_type,
       t.template
FROM incidents i
LEFT JOIN LATERAL (
    SELECT ec.template FROM error_clusters ec
    WHERE ec.fingerprint = i.fingerprint
    ORDER BY ec.window_start LIMIT 1
) t ON TRUE;

-- 5) Error-event view: one row per (1-min window, service, fingerprint), labelled with severity + type
CREATE OR REPLACE VIEW v_error_events AS
SELECT ec.id,
       ec.window_start,
       ec.window_end,
       ec.service,
       ec.fingerprint,
       ec.template,
       ec.event_count,
       ec.host_count,
       classify_error(ec.template)             AS error_type,
       COALESCE(inc.severity, 'Unclassified')  AS severity,   -- 'Unclassified' = too small to become an incident
       inc.id                                  AS incident_id
FROM error_clusters ec
LEFT JOIN LATERAL (
    SELECT i.id, i.severity FROM incidents i
    WHERE i.fingerprint = ec.fingerprint AND i.first_seen < ec.window_end
    ORDER BY i.first_seen DESC LIMIT 1
) inc ON TRUE;
