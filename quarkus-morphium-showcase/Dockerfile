# Stage 1: Build. The build context is the ROOT of the morphium repository (the showcase is a module of it):
#   docker build -f quarkus-morphium-showcase/Dockerfile -t quarkus-morphium-showcase:local .
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

COPY . .
RUN mvn -B -Pshowcase -pl quarkus-morphium/deployment,quarkus-morphium-showcase -am package -DskipTests

# Stage 2: Runtime
FROM eclipse-temurin:21-jre

RUN groupadd -r quarkus && useradd -r -g quarkus -d /app quarkus

WORKDIR /app

COPY --from=build --chown=quarkus:quarkus /app/quarkus-morphium-showcase/target/quarkus-app/ ./quarkus-app/

USER quarkus

EXPOSE 8080

ENV QUARKUS_HTTP_HOST=0.0.0.0

ENTRYPOINT ["java", "-jar", "quarkus-app/quarkus-run.jar"]
