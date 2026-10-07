-- =====================================================================
-- analytics_kpis.sql - KPI queries for the incident-monitoring project
-- Run:  docker compose exec -T postgres psql -U pipeline -d incidents < sql/analytics_kpis.sql
-- Change the analysis window here:
\set window '24 hours'
-- =====================================================================

\echo '--- 1. Headline KPIs ---'
SELECT COUNT(*)                                                         AS total_incidents,
       COUNT(*) FILTER (WHERE criticality = 'Critical')                 AS critical_incidents,
       COUNT(*) FILTER (WHERE resolution_state = 'Unresolved')          AS unresolved,
       ROUND(AVG(ttr_minutes)::numeric, 1)                              AS mttr_minutes
FROM v_incidents
WHERE first_seen >= now() - :'window'::interval;

\echo '--- 2. Total error events + error rate (Delta Lake gold table) ---'
SELECT SUM(log_count)                                                                   AS total_logs,
       SUM(log_count) FILTER (WHERE level IN ('ERROR','FATAL','CRITICAL'))             AS error_logs,
       ROUND(100.0 * SUM(log_count) FILTER (WHERE level IN ('ERROR','FATAL','CRITICAL'))
             / NULLIF(SUM(log_count), 0), 2)                                           AS error_rate_pct
FROM log_stats_hourly
WHERE hour >= now() - :'window'::interval;

\echo '--- 3. Errors by service and severity ---'
SELECT service,
       SUM(event_count) FILTER (WHERE severity = 'P1')           AS p1,
       SUM(event_count) FILTER (WHERE severity = 'P2')           AS p2,
       SUM(event_count) FILTER (WHERE severity = 'P3')           AS p3,
       SUM(event_count) FILTER (WHERE severity = 'P4')           AS p4,
       SUM(event_count) FILTER (WHERE severity = 'Unclassified') AS unclassified,
       SUM(event_count)                                          AS total
FROM v_error_events
WHERE window_start >= now() - :'window'::interval
GROUP BY service
ORDER BY total DESC;

\echo '--- 4. Top recurring errors ---'
SELECT template, error_type, service,
       SUM(event_count)  AS occurrences,
       COUNT(*)          AS minutes_affected,
       MAX(window_start) AS last_seen
FROM v_error_events
WHERE window_start >= now() - :'window'::interval
GROUP BY template, error_type, service
ORDER BY occurrences DESC
LIMIT 10;

\echo '--- 5. Incident trend (per hour, by severity) ---'
SELECT date_trunc('hour', first_seen) AS hour, severity, COUNT(*) AS incidents
FROM v_incidents
WHERE first_seen >= now() - :'window'::interval
GROUP BY 1, 2
ORDER BY 1, 2;

\echo '--- 6. Most failing services ---'
SELECT service,
       SUM(event_count)                       AS error_events,
       COUNT(DISTINCT incident_id)            AS incidents
FROM v_error_events
WHERE window_start >= now() - :'window'::interval
GROUP BY service
ORDER BY error_events DESC
LIMIT 10;

\echo '--- 7. Critical (P1/P2) vs non-critical ---'
SELECT criticality, COUNT(*) AS incidents, SUM(event_count) AS error_events
FROM v_incidents
WHERE first_seen >= now() - :'window'::interval
GROUP BY criticality;

\echo '--- 8. MTTR overall, median, p90 and by service (minutes) ---'
SELECT COALESCE(service, 'ALL SERVICES')                                              AS service,
       COUNT(*)                                                                       AS resolved_incidents,
       ROUND(AVG(ttr_minutes)::numeric, 1)                                            AS mttr_avg,
       ROUND((percentile_cont(0.5) WITHIN GROUP (ORDER BY ttr_minutes))::numeric, 1)  AS mttr_median,
       ROUND((percentile_cont(0.9) WITHIN GROUP (ORDER BY ttr_minutes))::numeric, 1)  AS mttr_p90
FROM v_incidents
WHERE resolution_state = 'Resolved' AND first_seen >= now() - :'window'::interval
GROUP BY ROLLUP (service)
ORDER BY service NULLS FIRST;

\echo '--- 9. Resolved vs unresolved ---'
SELECT resolution_state, COUNT(*) AS incidents,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM v_incidents
WHERE first_seen >= now() - :'window'::interval
GROUP BY resolution_state;

\echo '--- 10. Root-cause distribution ---'
SELECT root_cause_category, COUNT(*) AS incidents,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM v_incidents
WHERE first_seen >= now() - :'window'::interval
GROUP BY root_cause_category
ORDER BY incidents DESC;

\echo '--- 11. Repeat offenders: the same error causing multiple incidents (RCA candidates) ---'
SELECT service, template, root_cause_category,
       COUNT(*) AS incidents, SUM(event_count) AS error_events,
       ROUND(AVG(ttr_minutes)::numeric, 1) AS avg_ttr_min
FROM v_incidents
WHERE first_seen >= now() - :'window'::interval
GROUP BY service, template, root_cause_category
HAVING COUNT(*) > 1
ORDER BY incidents DESC;

\echo '--- 12. Error-type x service heatmap data ---'
SELECT service, error_type, SUM(event_count) AS error_events
FROM v_error_events
WHERE window_start >= now() - :'window'::interval
GROUP BY service, error_type
ORDER BY service, error_events DESC;
