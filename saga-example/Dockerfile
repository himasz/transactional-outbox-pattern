# Build context is the REPOSITORY ROOT, not this directory:
#   docker build -f saga-example/Dockerfile .
# docker-compose.saga.yml sets that up. A separate Dockerfile keeps the saga
# demo from touching the root one, so the two demos stay independently runnable.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src

COPY pom.xml .
COPY outbox-core/pom.xml     outbox-core/
COPY inbox-core/pom.xml      inbox-core/
COPY outbox-runner/pom.xml   outbox-runner/
COPY outbox-example/pom.xml  outbox-example/
COPY saga-example/pom.xml    saga-example/
RUN mvn -B -q dependency:go-offline -DskipTests || true

COPY outbox-core     outbox-core
COPY inbox-core      inbox-core
COPY outbox-runner   outbox-runner
COPY outbox-example  outbox-example
COPY saga-example    saga-example
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=build /src/outbox-runner/target/outbox-runner.jar /app/relay.jar
COPY --from=build /src/saga-example/target/saga-example.jar   /app/saga.jar
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=70"
