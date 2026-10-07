package com.logpipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Supplier;

/**
 * Simulates logs from web servers, microservices, mobile apps and infrastructure.
 * Every INCIDENT_EVERY_SECONDS it injects a burst of similar errors from one service.
 */
public class LogProducer {

    record LogTemplate(String level, String message) {}
    record IncidentTemplate(String service, String level, String message) {}

    static final String TOPIC = "raw-logs";
    static final Random RND = new Random();
    static final ObjectMapper MAPPER = new ObjectMapper();

    // service -> [source type, host1, host2, ...]
    static final Map<String, String[]> SERVICES = new LinkedHashMap<>();
    static {
        SERVICES.put("web-nginx",      new String[]{"web", "host-w1", "host-w2"});
        SERVICES.put("payment-svc",    new String[]{"microservice", "host-p1", "host-p2", "host-p3"});
        SERVICES.put("auth-svc",       new String[]{"microservice", "host-a1", "host-a2"});
        SERVICES.put("orders-svc",     new String[]{"microservice", "host-o1", "host-o2"});
        SERVICES.put("mobile-gateway", new String[]{"mobile", "host-m1"});
        SERVICES.put("k8s-node",       new String[]{"infra", "node-1", "node-2"});
    }

    static final List<LogTemplate> NORMAL = List.of(
        new LogTemplate("INFO",  "GET /api/v1/items/{n} 200 {n}ms"),
        new LogTemplate("INFO",  "User {n} logged in from {ip}"),
        new LogTemplate("INFO",  "Order {uuid} created for customer {n}"),
        new LogTemplate("DEBUG", "Cache hit for key session:{uuid}"),
        new LogTemplate("WARN",  "Slow query detected: {n}ms on table orders"),
        new LogTemplate("WARN",  "Retrying request to inventory-svc (attempt {n})"),
        new LogTemplate("ERROR", "Failed to parse payload for request {uuid}")   // background noise
    );

    static final List<IncidentTemplate> INCIDENTS = List.of(
        new IncidentTemplate("payment-svc", "ERROR", "Connection to postgres-primary timed out after {n}ms (pool exhausted, active={n})"),
        new IncidentTemplate("auth-svc",    "ERROR", "Redis connection refused at {ip}:6379 for session {uuid}"),
        new IncidentTemplate("orders-svc",  "FATAL", "OutOfMemoryError: Java heap space in OrderBatchProcessor (heap={n}MB)"),
        new IncidentTemplate("web-nginx",   "ERROR", "upstream timed out (110: Connection timed out) while reading response from {ip}"),
        new IncidentTemplate("k8s-node",    "ERROR", "Disk pressure on node: /var/lib/docker usage {n}%")
    );

    static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    /** Replaces every occurrence of token with a fresh random value. */
    static String replaceEach(String text, String token, Supplier<String> value) {
        while (text.contains(token)) {
            text = text.replaceFirst(java.util.regex.Pattern.quote(token), java.util.regex.Matcher.quoteReplacement(value.get()));
        }
        return text;
    }

    static String fill(String template) {
        String s = replaceEach(template, "{n}", () -> String.valueOf(1 + RND.nextInt(9999)));
        s = replaceEach(s, "{ip}", () -> "10." + RND.nextInt(256) + "." + RND.nextInt(256) + "." + (1 + RND.nextInt(254)));
        s = replaceEach(s, "{uuid}", () -> UUID.randomUUID().toString());
        return s;
    }

    static Map<String, Object> event(String service, String level, String message) {
        String[] src = SERVICES.get(service);
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("timestamp", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());
        e.put("level", level);
        e.put("service", service);
        e.put("source_type", src[0]);
        e.put("host", src[1 + RND.nextInt(src.length - 1)]);
        e.put("env", "prod");
        e.put("trace_id", UUID.randomUUID().toString().replace("-", "").substring(0, 16));
        e.put("message", fill(message));
        return e;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:29092");
        int rate = Integer.parseInt(env("LOGS_PER_SECOND", "20"));
        long incidentEveryMs = Long.parseLong(env("INCIDENT_EVERY_SECONDS", "90")) * 1000;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);

        List<String> serviceNames = new ArrayList<>(SERVICES.keySet());
        System.out.println("Producing to " + bootstrap + " at ~" + rate + " logs/s");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long nextIncident = System.currentTimeMillis() + 30_000;
            long burstUntil = 0;
            IncidentTemplate burst = null;

            while (true) {
                long now = System.currentTimeMillis();
                if (now >= nextIncident) {
                    burst = INCIDENTS.get(RND.nextInt(INCIDENTS.size()));
                    burstUntil = now + 25_000;
                    nextIncident = now + incidentEveryMs;
                    System.out.println("*** INCIDENT BURST: " + burst.service() + " -> " + burst.message());
                }
                for (int i = 0; i < rate; i++) {
                    String service, level, message;
                    if (burst != null && now < burstUntil && RND.nextDouble() < 0.6) {
                        service = burst.service(); level = burst.level(); message = burst.message();
                    } else {
                        service = serviceNames.get(RND.nextInt(serviceNames.size()));
                        LogTemplate t = NORMAL.get(RND.nextInt(NORMAL.size()));
                        level = t.level(); message = t.message();
                    }
                    String json = MAPPER.writeValueAsString(event(service, level, message));
                    producer.send(new ProducerRecord<>(TOPIC, service, json));
                }
                Thread.sleep(1000);
            }
        }
    }
}
