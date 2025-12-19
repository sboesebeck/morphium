# Morphium v6 Documentation

Morphium is a Java 21+ Object Document Mapper (ODM) and MongoDB‑backed messaging system. It includes a custom MongoDB wire‑protocol driver, distributed caching, and a topic‑based message queue.

## Getting Started
- Start here: [Overview](./overview.md) of features and a short quick start.
- Setup: [Developer Guide](./developer-guide.md) for mapping, queries, aggregation, caching, configuration, and extension points.
- Messaging: dedicated [Messaging](./messaging.md) guide (exclusive vs broadcast, listeners, concurrency, change streams).
- How‑Tos: focused recipes for common tasks and migrations — browse [How‑Tos](./howtos/basic-setup.md) to get started.

## Testing & Development
Morphium includes a complete in-memory MongoDB-compatible implementation for testing and development:
- **[Developer Testing Guide](./developer-testing-guide.md)** - How to run and write tests, MultiDriverTestBase, runtests.sh
- **[Test Runner](./test-runner.md)** - Quick reference for the `runtests.sh` script
- **[InMemory Driver](./howtos/inmemory-driver.md)** - Embedded in-memory driver for unit tests (no MongoDB installation required!)
- **[MorphiumServer](./morphium-server.md)** - Standalone MongoDB-compatible server that speaks the wire protocol
  - Perfect for CI/CD pipelines, integration testing, and microservices development
  - Any MongoDB client (Java, Python, Node.js, Go, etc.) can connect to it
  - Supports **Replica Sets** (experimental) and **Persistence (Snapshots)**
  - Run with: `java -jar target/morphium-6.1.1-server-cli.jar --port 27017`

## Production Deployment
- **[Production Deployment Guide](./production-deployment-guide.md)** - Complete guide for deploying Morphium in production environments
- **[Configuration Reference](./configuration-reference.md)** - Complete reference for all configuration options
- **[Performance & Scalability Guide](./performance-scalability-guide.md)** - Optimization strategies from small to large scale
- **[Security Guide](./security-guide.md)** - Security considerations for MongoDB Community Edition deployments

## Operations & Monitoring  
- **[Monitoring & Metrics Guide](./monitoring-metrics-guide.md)** - Comprehensive monitoring with DriverStats and performance metrics
- **[Troubleshooting Guide](./troubleshooting-guide.md)** - Common issues, diagnosis, and solutions
- **[Architecture Overview](./architecture-overview.md)** - Internal architecture and component relationships

## Reference
- **[API Reference](./api-reference.md)** - Complete API documentation with examples

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
- Messaging: see [Messaging](./messaging.md)
- Testing without MongoDB: see [InMemory Driver](./howtos/inmemory-driver.md), [MorphiumServer](./morphium-server.md), and [Migration v5 → v6](./howtos/migration-v5-to-v6.md)

### Our own driver (since 5.0)

With Morphium 5.0 we implemented our own MongoDB driver. The official driver includes object mapping, which interfered with Morphium’s mapping system, and we also experienced some failover issues. Building our own wire‑protocol driver gave us full control over mapping, retry/failover behavior, and performance characteristics.

Benefits
- Tailored to Morphium’s mapping and lifecycle needs; minimal impedance with Morphium’s object mapper.
- Full control over retry/failover semantics and performance trade‑offs.
- SSL/TLS support for secure connections (since v6.0).

Limitations
- No MongoDB Atlas support.
- Some advanced features of the official driver are not available.
