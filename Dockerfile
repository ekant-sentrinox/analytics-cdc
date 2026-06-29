# Runtime image. The jar is built on the host (`mvn -DskipTests package`) because
# io.dazzleduck.sql:dazzleduck-sql-common is only in the local ~/.m2, not on any
# public repo — a clean build container cannot resolve it.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/analytics-cdc.jar /app/app.jar
# Default entrypoint runs the ollylake table init; the analytics-cdc service
# overrides the main class in docker-compose.yml.
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
