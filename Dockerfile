# Single image containing both runnable jars; docker-compose picks the entry
# point per service. Keeps the build to one Maven pass instead of three.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src

# Copy poms first so dependency resolution is cached independently of sources.
COPY pom.xml .
COPY outbox-core/pom.xml     outbox-core/
COPY inbox-core/pom.xml      inbox-core/
COPY outbox-runner/pom.xml   outbox-runner/
COPY outbox-example/pom.xml  outbox-example/
RUN mvn -B -q dependency:go-offline -DskipTests || true

COPY outbox-core     outbox-core
COPY inbox-core      inbox-core
COPY outbox-runner   outbox-runner
COPY outbox-example  outbox-example
# Tests need a Docker socket (Testcontainers), so they are skipped in the image
# build and run on the host via `make test`.
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build /src/outbox-runner/target/outbox-runner.jar   /app/relay.jar
COPY --from=build /src/outbox-example/target/outbox-example.jar /app/example.jar
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70"
