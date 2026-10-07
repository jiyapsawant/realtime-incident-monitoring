package com.logpipeline;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * Kafka -> parse / enrich / fingerprint -> (1) Delta Lake (all logs)
 *                                          (2) Postgres error_clusters (1-minute windows)
 */
public class StreamJob {

    static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    public static void main(String[] args) throws Exception {
        String kafka = env("KAFKA_BOOTSTRAP", "kafka:9092");
        String pgUrl = "jdbc:postgresql://" + env("PG_HOST", "postgres") + ":5432/" + env("PG_DB", "incidents")
                + "?stringtype=unspecified";
        Properties pgProps = new Properties();
        pgProps.put("user", env("PG_USER", "pipeline"));
        pgProps.put("password", env("PG_PASSWORD", "pipeline"));
        pgProps.put("driver", "org.postgresql.Driver");

        String deltaPath = "/data/delta/logs";
        String checkpoints = "/data/checkpoints";

        SparkSession spark = SparkSession.builder()
                .appName("log-pipeline")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        StructType schema = new StructType()
                .add("timestamp", DataTypes.StringType)
                .add("level", DataTypes.StringType)
                .add("service", DataTypes.StringType)
                .add("source_type", DataTypes.StringType)
                .add("host", DataTypes.StringType)
                .add("env", DataTypes.StringType)
                .add("trace_id", DataTypes.StringType)
                .add("message", DataTypes.StringType);

        // ---- 1. Ingest from Kafka ----
        Dataset<Row> raw = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", kafka)
                .option("subscribe", "raw-logs")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        // ---- 2. Parse JSON, drop malformed records ----
        Dataset<Row> parsed = raw
                .select(from_json(col("value").cast("string"), schema).alias("j"))
                .where("j IS NOT NULL AND j.message IS NOT NULL")
                .select("j.*");

        // ---- 3. Enrich: turn the message into a template, then hash it into a fingerprint ----
        // "Timeout after 532ms for 10.1.2.3" -> "Timeout after <NUM>ms for <IP>"
        String[][] normalisers = {
            {"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<UUID>"},
            {"\\b\\d{1,3}(\\.\\d{1,3}){3}\\b", "<IP>"},
            {"0x[0-9a-fA-F]+", "<HEX>"},
            {"\\b\\d+\\b", "<NUM>"}
        };
        Column template = col("message");
        for (String[] n : normalisers) {
            template = regexp_replace(template, n[0], n[1]);
        }

        Dataset<Row> enriched = parsed
                .withColumn("event_time", to_timestamp(col("timestamp")))
                .withColumn("level", upper(col("level")))
                .withColumn("template", template)
                .withColumn("fingerprint",
                        substring(sha2(concat_ws("|", col("service"), col("level"), col("template")), 256), 1, 16))
                .withColumn("is_error", col("level").isin("ERROR", "FATAL", "CRITICAL"))
                .withColumn("ingest_time", current_timestamp())
                .withColumn("log_date", to_date(col("event_time")))
                .where("event_time IS NOT NULL");

        // ---- 4a. Sink 1: all logs -> Delta Lake (the "lakehouse") ----
        enriched.writeStream()
                .format("delta")
                .outputMode("append")
                .partitionBy("log_date")
                .option("checkpointLocation", checkpoints + "/delta")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start(deltaPath);

        // ---- 4b. Sink 2: error clusters per 1-minute window -> Postgres ----
        Dataset<Row> clusters = enriched
                .where("is_error")
                .withWatermark("event_time", "30 seconds")
                .groupBy(window(col("event_time"), "1 minute"), col("service"), col("fingerprint"))
                .agg(
                        count(lit(1)).alias("event_count"),
                        approx_count_distinct("host").alias("host_count"),
                        min("event_time").alias("first_seen"),
                        max("event_time").alias("last_seen"),
                        first("template").alias("template"),
                        to_json(slice(collect_list(col("message")), 1, 5)).alias("sample_messages"))
                .select(
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        col("service"), col("fingerprint"), col("template"),
                        col("event_count"), col("host_count"),
                        col("first_seen"), col("last_seen"), col("sample_messages"));

        // The cast is required in Java to pick the right foreachBatch overload.
        VoidFunction2<Dataset<Row>, Long> writeToPostgres = (batch, batchId) -> {
            batch.persist();
            long n = batch.count();
            if (n > 0) {
                batch.write().mode("append").jdbc(pgUrl, "error_clusters", pgProps);
                System.out.println("[batch " + batchId + "] wrote " + n + " clusters to Postgres");
            }
            batch.unpersist();
        };

        clusters.writeStream()
                .outputMode("append")
                .foreachBatch(writeToPostgres)
                .option("checkpointLocation", checkpoints + "/clusters")
                .trigger(Trigger.ProcessingTime("15 seconds"))
                .start();

        spark.streams().awaitAnyTermination();
    }
}
