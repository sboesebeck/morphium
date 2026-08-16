# Stage 1: Build
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

# Copy pom.xml first for layer caching
COPY pom.xml .
RUN mvn -B dependency:go-offline -DskipTests || true

# Copy source and build
COPY src src
RUN mvn -B package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:21-jre

RUN groupadd -r quarkus && useradd -r -g quarkus -d /app quarkus

WORKDIR /app

COPY --from=build --chown=quarkus:quarkus /app/target/quarkus-app/ ./quarkus-app/

USER quarkus

EXPOSE 8080

ENV QUARKUS_HTTP_HOST=0.0.0.0

ENTRYPOINT ["java", "-jar", "quarkus-app/quarkus-run.jar"]
