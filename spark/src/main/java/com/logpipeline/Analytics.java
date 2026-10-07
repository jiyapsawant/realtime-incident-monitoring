package com.logpipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/** Batch analytics over the Delta Lake table (see README for how to run). */
public class Analytics {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("log-analytics")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> logs = spark.read().format("delta").load("/data/delta/logs");
        System.out.println("Total logs: " + logs.count());

        System.out.println("\nLogs by service and level:");
        logs.groupBy("service", "level").count().orderBy("service", "level").show(50, false);

        System.out.println("\nTop error templates:");
        logs.where("is_error").groupBy("service", "template").count()
            .orderBy(col("count").desc()).show(15, false);
    }
}
