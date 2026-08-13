# Morphium v6 Documentation

<p align="center">
  <img src="assets/brand/morphium-logo.svg" alt="Morphium" width="640">
</p>

Morphium is a Java 21+ Object Document Mapper (ODM) and MongoDB‑backed messaging system. It includes a custom MongoDB wire‑protocol driver, distributed caching, and a topic‑based message queue.

---

<p align="center">
  <img src="assets/brand/poppydb-logo.svg" alt="PoppyDB" width="480">
</p>

## PoppyDB — MongoDB‑compatible Server

PoppyDB is the project's second product: a standalone, self‑contained server that speaks the
MongoDB wire protocol — any MongoDB client (Java, Python, Node.js, Go, `mongosh`, ...) can
connect to it. Perfect for CI/CD pipelines, integration testing, and lightweight deployments.

- **Replica Sets** with Raft‑based failover
- Opt‑in **Authentication (SCRAM)** and **TLS**
- **Persistence** via snapshots

```bash
java -jar poppydb-<version>-cli.jar --port 27017
```

→ [Overview](./poppydb.md) · [Production Deployment Playbook](./howtos/poppydb-deployment.md) · [Migrating from MongoDB](./howtos/migration-mongodb-to-poppydb.md)

---

## 🚀 New Here? Start Here!

**Learning path for beginners:**

1. **[Why Morphium?](./why-morphium.md)** — Honest comparison with the Official Driver
2. **[Quick Start Tutorial](./quickstart-tutorial.md)** — From zero to your first query in 10 minutes
3. **[Your First Test](./first-test.md)** — Unit tests with the InMemory Driver

---

## Getting Started (Reference)
- [Overview](./overview.md) — Features und kurzer Quick Start
- [Developer Guide](./developer-guide.md) — Mapping, Queries, Aggregation, Caching, Konfiguration
- [Messaging](./messaging.md) — Built-in Message Queue Guide
- [Messaging Implementations](./howtos/messaging-implementations.md) — Standard vs. MultiCollection vs. beta DualChannelMessaging, incl. measured throughput/latency numbers
- [How‑Tos](./howtos/basic-setup.md) — Rezepte für häufige Aufgaben

## Testing & Development
Morphium includes a complete in-memory MongoDB-compatible implementation for testing and development:
- **[Developer Testing Guide](./developer-testing-guide.md)** - How to run and write tests, MultiDriverTestBase, runtests.sh
- **[Test Runner](./test-runner.md)** - Quick reference for the `runtests.sh` script
- **[InMemory Driver](./howtos/inmemory-driver.md)** - Embedded in-memory driver for unit tests (no MongoDB installation required!)
- **[PoppyDB](./poppydb.md)** - Standalone MongoDB-compatible server that speaks the wire protocol — see the [PoppyDB section](#poppydb-mongodbcompatible-server) above

## Production Deployment
- **[Production Deployment Guide](./production-deployment-guide.md)** - Complete guide for deploying Morphium in production environments
- **[PoppyDB Production Deployment Playbook](./howtos/poppydb-deployment.md)** - Running PoppyDB itself in production: systemd unit, secrets handling, capacity planning, monitoring, backup/restore, upgrades
- **[Migrating from MongoDB to PoppyDB](./howtos/migration-mongodb-to-poppydb.md)** - moving an existing workload over: data migration, validation, cutover, rollback
- **[Configuration Reference](./configuration-reference.md)** - Complete reference for all configuration options
- **[Performance & Scalability Guide](./performance-scalability-guide.md)** - Optimization strategies from small to large scale
- **[Security Guide](./security-guide.md)** - Security considerations for MongoDB Community Edition deployments

## Operations & Monitoring  
- **[Monitoring & Metrics Guide](./monitoring-metrics-guide.md)** - Comprehensive monitoring with DriverStats and performance metrics
- **[Troubleshooting Guide](./troubleshooting-guide.md)** - Common issues, diagnosis, and solutions
- **[Architecture Overview](./architecture-overview.md)** - Internal architecture and component relationships

## Reference
- **[API Reference](./api-reference.md)** - Complete API documentation with examples

## Extensions (Optional Modules)
Morphium's core module (`de.caluga:morphium`) is fully self-contained and does not need
any of the following. These are additional, opt-in modules built on top of the core:
- **[Jakarta Data](./jakarta-data.md)** - Optional module implementing the Jakarta Data
  1.0 specification on top of Morphium's query engine (repository pattern, `@Repository`)
  - Query derivation from method names, JDQL (`@Query`), `@Find`/`@Delete` with `@By`
  - Offset and cursor pagination (`Page<T>`, `CursoredPage<T>`), dynamic and static sorting
  - Zero dependency from the core: build with `-DskipExtensions` for a core-only artifact;
    framework integrations for Quarkus and Spring Boot build on top of this module
- **[Quarkus Extension](./quarkus-extension.md)** - Optional module integrating Morphium into
  Quarkus applications via a CDI producer, `@ConfigMapping`, `@MorphiumTransactional`, health
  checks, Dev Services, Dev UI, and build-time Jakarta Data repository generation via Gizmo
  - GraalVM native-image support and `MorphiumId` JSON (de)serialization out of the box
  - Zero dependency from the core: build with `-DskipExtensions` for a core-only artifact

Minimum requirements
- Java 21+
- MongoDB 5.0+

Links
- Repository: https://github.com/sboesebeck/morphium

## Motivation & History

Morphium started when there was no official MongoDB object mapper and the then‑popular Morphia lacked extensibility around caching. We set out to build a flexible, extensible ODM with declarative features—especially for caching. At that time, Jackson also missed a few capabilities we needed (like better generics handling), so Morphium ships its own object mapper tuned precisely for Morphium’s use cases and performance.

The initial Message Queuing feature was created to synchronize caches across a cluster; it has since evolved into a production‑ready messaging system.

Learn more
- Object mapping and configuration: see the [Developer Guide](./developer-guide.md)
- Caching: see [Caching Examples](./howtos/caching-examples.md) and [Cache Patterns](./howtos/cache-patterns.md)
- Messaging: see [Messaging](./messaging.md) and [Messaging Implementations](./howtos/messaging-implementations.md) (implementation comparison, measured throughput/latency)
- Testing without MongoDB: see [InMemory Driver](./howtos/inmemory-driver.md), [PoppyDB](./poppydb.md)
- Upgrading: [v6.2 → v6.3](./howtos/migration-v6_2-to-v6_3.md) | [v6.1 → v6.2](./howtos/migration-v6_1-to-v6_2.md) | [v5 → v6](./howtos/migration-v5-to-v6.md)

### Our own driver (since 5.0)

With Morphium 5.0 we implemented our own MongoDB driver. The official driver includes object mapping, which interfered with Morphium’s mapping system, and we also experienced some failover issues. Building our own wire‑protocol driver gave us full control over mapping, retry/failover behavior, and performance characteristics.

Benefits
- Tailored to Morphium’s mapping and lifecycle needs; minimal impedance with Morphium’s object mapper.
- Full control over retry/failover semantics and performance trade‑offs.
- SSL/TLS support for secure connections (since v6.0).

Limitations
- No MongoDB Atlas support.
- Some advanced features of the official driver are not available.
