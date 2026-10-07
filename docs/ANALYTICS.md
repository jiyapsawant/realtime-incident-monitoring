# Analytics Layer (Data Analyst + Data Engineering view)

The core pipeline is unchanged. The analytics layer sits **on top of** its outputs:

```
Logs → Kafka → Spark Streaming → error clusters ─┐
                      │                          ├─▶ Postgres: incidents, error_clusters
                      ▼                          │        │
                 Delta Lake (bronze: all logs)   │        ▼
                      │                          │   SQL views (silver/semantic layer)
                      ▼                          │     v_incidents, v_error_events
        Spark batch HourlyAggregates (gold) ─────┘     + log_stats_hourly
                                                              │
                                                              ▼
                                         KPI SQL (sql/analytics_kpis.sql) → Grafana "Incident Analytics"
```

| Layer | Object | Role |
|---|---|---|
| Bronze | Delta Lake `/data/delta/logs` | every parsed log, partitioned by date |
| Gold (DE) | `log_stats_hourly` (Spark batch `HourlyAggregates.java`, refreshed every 5 min) | log volume by hour/service/level/error pattern |
| Semantic (DA) | `v_incidents`, `v_error_events`, `classify_error()` | clean, documented business definitions used by every KPI |
| Presentation | Grafana `Incident Analytics` dashboard | KPIs, trends, RCA with filters |

## What changed in the existing code (small)
- `incidents` gets `resolved_at` and `root_cause_category` (LLM returns the category; keyword fallback when no API key).
- Summarizer auto-resolves incidents after `RESOLVE_AFTER_MINUTES` (default 5) without new errors → enables MTTR.
- New Spark batch class `HourlyAggregates` + `analytics` service in docker-compose.

## KPI definitions
| KPI | Definition | Source |
|---|---|---|
| Total incidents | count of rows in `incidents` | `v_incidents` |
| Total error events | sum of clustered error log lines | `v_error_events.event_count` |
| Error rate % | error-level logs ÷ all logs | `log_stats_hourly` (Delta gold) |
| Errors by service & severity | error events grouped by service × P1–P4 | `v_error_events` |
| Top recurring errors | error pattern (normalised message) ranked by occurrences | `v_error_events` |
| Incident trend | incidents per 10 min by severity | `v_incidents` |
| Most failing services | error events per service, top 10 | `v_error_events` |
| Critical vs non-critical | Critical = P1/P2 | `v_incidents.criticality` |
| MTTR | AVG(`resolved_at − first_seen`) in minutes, resolved only | `v_incidents.ttr_minutes` |
| Resolved vs unresolved | `status = 'resolved'` vs everything else | `v_incidents.resolution_state` |
| Root-cause distribution | incidents by LLM `root_cause_category` | `v_incidents` |

Error type (filter) is derived by `classify_error()`: Timeout, Connection, Memory, Disk, Parsing, Other.
Error events with no matching incident (below `MIN_EVENTS_PER_CLUSTER`) show as severity **Unclassified**.

## Dashboard: "Incident Analytics (KPIs, Trends, RCA)"
Filters (top): **time range** (Grafana picker), **Service**, **Severity**, **Error type** (all multi-select).

1. **Headline KPIs** (8 stat tiles): total incidents, error events, error rate, critical, non-critical, unresolved, resolved %, MTTR
2. **Trends**: incident trend by severity · error events over time by service
3. **Where are we failing?**: errors by service × severity (stacked) · most failing services · critical vs non-critical · resolved vs unresolved
4. **Root cause analysis**: root-cause distribution · MTTR by service · top recurring errors
5. **Incident detail**: full incident table with LLM summary/probable cause

Open: http://localhost:3000 → Dashboards → *Incident Analytics*.

## Run it
```bash
# NEW project: just
docker compose up -d --build

# EXISTING project (database already created): apply the analytics SQL once, then rebuild
docker compose exec -T postgres psql -U pipeline -d incidents < sql/02_analytics.sql
docker compose up -d --build

# KPI queries in psql (change the window at the top of the file)
docker compose exec -T postgres psql -U pipeline -d incidents < sql/analytics_kpis.sql
```
Give it ~10 minutes to get meaningful trend/MTTR data. The `analytics` service uses another ~1 GB RAM; stop it with `docker compose stop analytics` if needed.

## Caveats (be upfront about these in interviews)
- Data is **simulated**; MTTR is driven by the auto-resolve rule. In production `resolved_at` comes from PagerDuty/Opsgenie webhooks or manual updates.
- Error rate uses hourly buckets, so very short time ranges look coarse.
- `classify_error()` is rule-based; the root-cause category is LLM-assigned and should be spot-checked.
- If Grafana's *Service* filter is empty on first load, wait for the first incident cluster, then refresh.

## 2-day plan
**Day 1 (data + SQL):** apply `02_analytics.sql`, rebuild, let data accumulate, run `analytics_kpis.sql` and sanity-check every number by hand against raw tables; tweak `classify_error()` and categories.
**Day 2 (dashboard + story):** open the dashboard, test each filter, screenshot it, write 3 findings (e.g. "payment-svc DB timeouts caused 40% of P2 incidents; MTTR 6.2 min"), update README with screenshots.

## Resume / interview bullets
- *DE:* Built a Kafka → Spark Structured Streaming → Delta Lake pipeline with a batch gold layer (Spark → Postgres) and idempotent SQL views.
- *DA:* Defined 11 incident KPIs (MTTR, error rate, recurrence, root-cause mix) in SQL with window functions, `FILTER`, `ROLLUP`, `percentile_cont`; delivered a filterable Grafana dashboard.
- *Both:* LLM-assisted root-cause categorisation with a deterministic fallback; discussed data-quality caveats and metric definitions.
