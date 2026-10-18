# Real-Time Log/Event Pipeline with LLM-Powered Incident Summarization (Java)
[![CI](https://github.com/jiyapsawant/realtime-incident-monitoring/actions/workflows/ci.yml/badge.svg)](https://github.com/jiyapsawant/realtime-incident-monitoring/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

![Dashboard](docs/dashboard.png)

```
Log sources ─▶ Kafka ─▶ Spark Structured Streaming ─┬▶ Delta Lake (all logs, analytics/training)
 (simulated)   raw-logs  parse • enrich • fingerprint └▶ Postgres.error_clusters (1-min windows)
                                                              │
                                      Java Summarizer (LLM) ◀─┘
                                              │
                      Postgres.incidents ─▶ Slack / PagerDuty ─▶ Grafana dashboard
```

## Project layout
```
producer/    LogProducer.java      Kafka producer simulating logs + incident bursts
spark/       StreamJob.java        Spark Structured Streaming job (Kafka -> Delta + Postgres)
             Analytics.java        Batch queries on the Delta Lake table
             HourlyAggregates.java Gold layer: Delta -> hourly stats in Postgres (runs every 5 min)
summarizer/  SummarizerApp.java    Polls Postgres, calls OpenAI, saves incident, sends Slack/PagerDuty
sql/init.sql                       Postgres tables (error_clusters, incidents)
sql/02_analytics.sql               Analytics layer: views, classifier, gold table
sql/analytics_kpis.sql             12 KPI queries (run with psql)
docs/ANALYTICS.md                  KPI definitions, dashboard guide, 2-day plan
grafana/                           Auto-provisioned datasource + dashboard
docker-compose.yml                 Runs everything
```
Each Java module is an independent Maven project (`pom.xml` + `src/main/java/com/logpipeline`).

## Prerequisites
Docker Desktop (Compose v2) and ~6 GB free RAM. You do NOT need Java or Maven installed to just run it — Docker builds the code.
To edit/run in an IDE: JDK 17 + Maven + IntelliJ IDEA (open each folder as a Maven project).

## Run
```bash
cp .env.example .env        # optional: add OPENAI_API_KEY / SLACK_WEBHOOK_URL
docker compose up -d --build
docker compose logs -f spark summarizer producer
```
First start is slow (Maven + Spark download dependencies; cached afterwards).
An incident burst starts ~30 s after the producer starts; first alert appears ~2 minutes in.

| What | Where |
|---|---|
| Grafana | http://localhost:3000 (admin / admin) → "Log Pipeline - Incident Overview" |
| Postgres | localhost:5432, user/pass `pipeline`, db `incidents` |
| Kafka (from host) | localhost:29092 |
| Delta Lake files | `./data/delta/logs` |

## Useful commands
```bash
docker compose exec postgres psql -U pipeline -d incidents -c "select severity,service,title,event_count from incidents order by id desc limit 5;"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic raw-logs --max-messages 3

# Lakehouse analytics on the Delta table (Java class Analytics)
docker compose exec spark /opt/spark/bin/spark-submit --master local[1] \
  --packages io.delta:delta-spark_2.12:3.2.0 --conf spark.jars.ivy=/tmp/ivy \
  --class com.logpipeline.Analytics /opt/app/spark-job.jar

# Rebuild after changing Java code
docker compose up -d --build producer spark summarizer

# Reset everything
docker compose down -v && rm -rf data/delta data/checkpoints
```

## Running a module from IntelliJ / terminal (optional)
```bash
docker compose up -d kafka kafka-init postgres grafana
cd producer && mvn package && KAFKA_BOOTSTRAP=localhost:29092 java -jar target/app.jar
cd summarizer && mvn package && PG_HOST=localhost java -jar target/app.jar
```

## How it works
1. **LogProducer** – generates JSON logs (web, microservice, mobile, infra) and periodic error bursts.
2. **StreamJob** – reads Kafka, parses JSON, normalises the message (UUID/IP/number → placeholders), hashes it to a *fingerprint* (error clustering), writes all logs to Delta (partitioned by date), and aggregates errors per (1-min window, service, fingerprint) with a watermark into Postgres. Checkpoints in `./data/checkpoints` give fault tolerance.
3. **SummarizerApp** – takes clusters with ≥ `MIN_EVENTS_PER_CLUSTER` events, merges repeats into an open incident (no alert spam for `DEDUP_MINUTES`), asks the LLM for title/severity/summary/root cause/actions, stores it, alerts Slack (all) and PagerDuty (P1/P2). Without an API key it falls back to a simple built-in summary.
4. **Grafana** – shows open incidents, error trends per service, and the AI summaries.

## Troubleshooting
- **Spark exits** – `docker compose logs spark`; usually a blocked jar download. Re-run `docker compose up -d spark`.
- **No incidents** – wait ≥ 2 min; or lower `MIN_EVENTS_PER_CLUSTER` in `.env`.
- **Maven build error in Docker** – check internet/proxy, then `docker compose build --no-cache <service>`.
- **Grafana "no data"** – make sure rows exist in `error_clusters`; set time range to "Last 1 hour".

## Extend it
Schema Registry + Avro, MinIO/S3 for Delta, dead-letter topic for malformed logs, pgvector similarity clustering, Slack ack buttons, JUnit tests, GitHub Actions CI, Kubernetes deployment.

## Analytics layer (Data Analyst view)
A second Grafana dashboard, **Incident Analytics**, adds KPIs, trends and root-cause analysis with filters for time, service, severity and error type. See [docs/ANALYTICS.md](docs/ANALYTICS.md).
Existing database? Run once: `docker compose exec -T postgres psql -U pipeline -d incidents < sql/02_analytics.sql`
