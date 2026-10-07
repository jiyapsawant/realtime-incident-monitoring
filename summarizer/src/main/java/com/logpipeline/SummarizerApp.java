package com.logpipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Polls Postgres for new error clusters -> asks an LLM for an incident report ->
 * stores it in `incidents` -> alerts Slack / PagerDuty.
 */
public class SummarizerApp {

    record Cluster(long id, Timestamp windowStart, Timestamp windowEnd, String service, String fingerprint,
                   String template, int eventCount, int hostCount, Timestamp firstSeen, Timestamp lastSeen,
                   String sampleMessages) {}

    record Report(String title, String severity, String summary, String rootCause, String category,
                  List<String> actions, String model) {}

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();

    static final String PG_URL = "jdbc:postgresql://" + env("PG_HOST", "localhost") + ":5432/" + env("PG_DB", "incidents");
    static final String PG_USER = env("PG_USER", "pipeline");
    static final String PG_PASS = env("PG_PASSWORD", "pipeline");
    static final String OPENAI_KEY = env("OPENAI_API_KEY", "");
    static final String MODEL = env("OPENAI_MODEL", "gpt-4o-mini");
    static final String SLACK_URL = env("SLACK_WEBHOOK_URL", "");
    static final String PD_KEY = env("PAGERDUTY_ROUTING_KEY", "");
    static final int MIN_EVENTS = Integer.parseInt(env("MIN_EVENTS_PER_CLUSTER", "5"));
    static final int DEDUP_MINUTES = Integer.parseInt(env("DEDUP_MINUTES", "15"));
    static final int POLL_SECONDS = Integer.parseInt(env("POLL_SECONDS", "10"));
    static final int RESOLVE_AFTER_MINUTES = Integer.parseInt(env("RESOLVE_AFTER_MINUTES", "5"));
    static final Set<String> CATEGORIES = Set.of("Database", "Network", "Memory/Resource", "Dependency",
            "Deployment/Config", "Application Bug", "Infrastructure", "Unknown");

    static final String SYSTEM_PROMPT = "You are an expert SRE. Given a cluster of similar error logs, produce a concise "
            + "incident report. Reply ONLY with JSON: {\"title\": string, \"severity\": \"P1|P2|P3|P4\", "
            + "\"summary\": string, \"probable_root_cause\": string, \"root_cause_category\": \"Database|Network|Memory/Resource|Dependency|"
            + "Deployment/Config|Application Bug|Infrastructure|Unknown\", \"suggested_actions\": [string, ...]}. "
            + "P1 = customer-facing outage or data loss; P2 = major degradation; P3 = minor; P4 = noise.";

    static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Summarizer started (LLM=" + (OPENAI_KEY.isEmpty() ? "off" : MODEL)
                + ", slack=" + (SLACK_URL.isEmpty() ? "off" : "on")
                + ", pagerduty=" + (PD_KEY.isEmpty() ? "off" : "on") + ")");
        while (true) {
            try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
                processNewClusters(conn);
                resolveQuietIncidents(conn);
            } catch (Exception e) {
                System.out.println("Loop error: " + e);
            }
            Thread.sleep(POLL_SECONDS * 1000L);
        }
    }

    // ---------------------------------------------------------------- main logic

    static void processNewClusters(Connection conn) throws Exception {
        for (Cluster c : fetchNewClusters(conn)) {
            if (c.eventCount() < MIN_EVENTS) {
                setClusterStatus(conn, c.id(), "ignored");
                continue;
            }
            Long openIncident = findOpenIncident(conn, c.fingerprint());
            if (openIncident != null) {               // same problem still going on: merge, don't re-alert
                mergeIntoIncident(conn, openIncident, c);
                setClusterStatus(conn, c.id(), "merged");
                continue;
            }
            Report report = summarize(c);
            long incidentId = insertIncident(conn, c, report);
            setClusterStatus(conn, c.id(), "summarized");
            notifyTeam(incidentId, c, report);
        }
    }

    static List<Cluster> fetchNewClusters(Connection conn) throws SQLException {
        String sql = "SELECT id, window_start, window_end, service, fingerprint, template, event_count, host_count, "
                + "first_seen, last_seen, sample_messages::text AS samples "
                + "FROM error_clusters WHERE status = 'new' ORDER BY window_start LIMIT 20";
        List<Cluster> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                out.add(new Cluster(rs.getLong("id"), rs.getTimestamp("window_start"), rs.getTimestamp("window_end"),
                        rs.getString("service"), rs.getString("fingerprint"), rs.getString("template"),
                        rs.getInt("event_count"), rs.getInt("host_count"), rs.getTimestamp("first_seen"),
                        rs.getTimestamp("last_seen"), rs.getString("samples")));
            }
        }
        return out;
    }

    static void setClusterStatus(Connection conn, long id, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("UPDATE error_clusters SET status = ? WHERE id = ?")) {
            ps.setString(1, status);
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    static Long findOpenIncident(Connection conn, String fingerprint) throws SQLException {
        String sql = "SELECT id FROM incidents WHERE fingerprint = ? AND status = 'open' "
                + "AND created_at > now() - make_interval(mins => ?) LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, fingerprint);
            ps.setInt(2, DEDUP_MINUTES);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }

    static void mergeIntoIncident(Connection conn, long incidentId, Cluster c) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE incidents SET event_count = event_count + ?, last_seen = ? WHERE id = ?")) {
            ps.setInt(1, c.eventCount());
            ps.setTimestamp(2, c.lastSeen());
            ps.setLong(3, incidentId);
            ps.executeUpdate();
        }
    }

    static long insertIncident(Connection conn, Cluster c, Report r) throws Exception {
        String sql = "INSERT INTO incidents (fingerprint, service, title, severity, summary, root_cause, "
                + "suggested_actions, event_count, first_seen, last_seen, llm_model, root_cause_category) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?) RETURNING id";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, c.fingerprint());
            ps.setString(2, c.service());
            ps.setString(3, r.title());
            ps.setString(4, r.severity());
            ps.setString(5, r.summary());
            ps.setString(6, r.rootCause());
            ps.setString(7, MAPPER.writeValueAsString(r.actions()));
            ps.setInt(8, c.eventCount());
            ps.setTimestamp(9, c.firstSeen());
            ps.setTimestamp(10, c.lastSeen());
            ps.setString(11, r.model());
            ps.setString(12, r.category());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    // ---------------------------------------------------------------- LLM

    static Report summarize(Cluster c) {
        if (OPENAI_KEY.isEmpty()) {
            return heuristic(c, "heuristic");
        }
        try {
            ObjectNode input = MAPPER.createObjectNode();
            input.put("service", c.service());
            input.put("error_template", c.template());
            input.put("event_count", c.eventCount());
            input.put("hosts_affected", c.hostCount());
            input.put("window_start", String.valueOf(c.windowStart()));
            input.put("window_end", String.valueOf(c.windowEnd()));
            input.set("sample_messages", MAPPER.readTree(c.sampleMessages()));

            ObjectNode body = MAPPER.createObjectNode();
            body.put("model", MODEL);
            body.put("temperature", 0.2);
            body.putObject("response_format").put("type", "json_object");
            ArrayNode messages = body.putArray("messages");
            messages.addObject().put("role", "system").put("content", SYSTEM_PROMPT);
            messages.addObject().put("role", "user").put("content", MAPPER.writeValueAsString(input));

            HttpRequest req = HttpRequest.newBuilder(URI.create("https://api.openai.com/v1/chat/completions"))
                    .timeout(Duration.ofSeconds(60))
                    .header("Authorization", "Bearer " + OPENAI_KEY)
                    .header("Content-Type", "application/json")
                    .POST(BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
                    .build();
            HttpResponse<String> resp = HTTP.send(req, BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new RuntimeException("OpenAI HTTP " + resp.statusCode() + ": " + resp.body());
            }
            String content = MAPPER.readTree(resp.body()).at("/choices/0/message/content").asText();
            JsonNode j = MAPPER.readTree(content);

            List<String> actions = new ArrayList<>();
            j.path("suggested_actions").forEach(a -> actions.add(a.asText()));
            return new Report(
                    j.path("title").asText(c.service() + ": " + c.template()),
                    j.path("severity").asText("P3"),
                    j.path("summary").asText(""),
                    j.path("probable_root_cause").asText(""),
                    CATEGORIES.contains(j.path("root_cause_category").asText()) ? j.path("root_cause_category").asText()
                                                                               : categoryFor(c.template()),
                    actions, MODEL);
        } catch (Exception e) {
            System.out.println("LLM error, using fallback: " + e.getMessage());
            return heuristic(c, "heuristic-fallback");
        }
    }

    static Report heuristic(Cluster c, String model) {
        String shortTemplate = c.template().length() > 80 ? c.template().substring(0, 80) : c.template();
        return new Report(
                c.service() + ": " + shortTemplate,
                c.eventCount() >= 50 ? "P2" : "P3",
                c.eventCount() + " similar errors from " + c.service() + " across " + c.hostCount() + " host(s).",
                "LLM disabled (no OPENAI_API_KEY) - inspect the sample messages.",
                categoryFor(c.template()),
                List.of("Check recent deploys", "Inspect service dependencies", "Review sample logs"),
                model);
    }

    /** Keyword fallback so root-cause analytics still works without an LLM. */
    static String categoryFor(String template) {
        String t = template == null ? "" : template.toLowerCase();
        if (t.contains("postgres") || t.contains("database") || t.contains("sql") || t.contains("pool")) return "Database";
        if (t.contains("outofmemory") || t.contains("heap") || t.contains("memory")) return "Memory/Resource";
        if (t.contains("disk") || t.contains("node")) return "Infrastructure";
        if (t.contains("upstream") || t.contains("dns") || t.contains("socket")) return "Network";
        if (t.contains("redis") || t.contains("refused") || t.contains("dependency")) return "Dependency";
        if (t.contains("deploy") || t.contains("config")) return "Deployment/Config";
        if (t.contains("exception") || t.contains("null") || t.contains("parse")) return "Application Bug";
        return "Unknown";
    }

    /**
     * Auto-resolve: an open incident with no new errors for RESOLVE_AFTER_MINUTES is marked resolved.
     * This is what makes MTTR (resolved_at - first_seen) computable. In production, resolved_at would come
     * from PagerDuty / Opsgenie webhooks or an engineer's manual update.
     */
    static void resolveQuietIncidents(Connection conn) throws SQLException {
        String sql = "UPDATE incidents SET status = 'resolved', resolved_at = now() "
                + "WHERE status = 'open' AND last_seen < now() - make_interval(mins => ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, RESOLVE_AFTER_MINUTES);
            int n = ps.executeUpdate();
            if (n > 0) System.out.println("Auto-resolved " + n + " incident(s)");
        }
    }

    // ---------------------------------------------------------------- alerts

    static void notifyTeam(long incidentId, Cluster c, Report r) {
        StringBuilder text = new StringBuilder();
        text.append(":rotating_light: *[").append(r.severity()).append("] ").append(r.title()).append("*\n")
            .append("*Incident #").append(incidentId).append("* | *Service:* ").append(c.service())
            .append(" | *Events:* ").append(c.eventCount()).append("\n")
            .append("*Summary:* ").append(r.summary()).append("\n")
            .append("*Probable cause:* ").append(r.rootCause()).append(" (").append(r.category()).append(")\n*Actions:*\n");
        r.actions().forEach(a -> text.append("• ").append(a).append("\n"));

        if (SLACK_URL.isEmpty()) {
            System.out.println(text);               // no Slack configured: print the alert instead
        } else {
            ObjectNode payload = MAPPER.createObjectNode().put("text", text.toString());
            post(SLACK_URL, payload);
        }

        if (!PD_KEY.isEmpty() && (r.severity().equals("P1") || r.severity().equals("P2"))) {
            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("routing_key", PD_KEY);
            payload.put("event_action", "trigger");
            payload.put("dedup_key", c.fingerprint());
            ObjectNode p = payload.putObject("payload");
            p.put("summary", r.title());
            p.put("source", c.service());
            p.put("severity", r.severity().equals("P1") ? "critical" : "error");
            p.putObject("custom_details").put("summary", r.summary()).put("root_cause", r.rootCause());
            post("https://events.pagerduty.com/v2/enqueue", payload);
        }
    }

    static void post(String url, ObjectNode json) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(BodyPublishers.ofString(MAPPER.writeValueAsString(json)))
                    .build();
            HttpResponse<String> resp = HTTP.send(req, BodyHandlers.ofString());
            if (resp.statusCode() >= 300) {
                System.out.println("POST " + url + " -> HTTP " + resp.statusCode());
            }
        } catch (Exception e) {
            System.out.println("POST failed: " + e.getMessage());
        }
    }
}
