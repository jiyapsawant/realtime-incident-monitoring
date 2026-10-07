FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM apache/spark:3.5.1-scala2.12-java17-ubuntu
USER root
COPY --from=build /build/target/spark-job.jar /opt/app/spark-job.jar
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3", \
  "--conf", "spark.jars.ivy=/tmp/ivy", \
  "--class", "com.logpipeline.StreamJob", "/opt/app/spark-job.jar"]
