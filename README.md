# Quarkus Morphium Showcase

[![Build](https://github.com/Bardioc1977/quarkus-morphium-showcase/actions/workflows/build.yml/badge.svg)](https://github.com/Bardioc1977/quarkus-morphium-showcase/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Quarkus](https://img.shields.io/badge/Quarkus-3.32.1-blue)](https://quarkus.io)
[![Java](https://img.shields.io/badge/Java-21%2B-orange)](https://adoptium.net)

> **Note:** This showcase is built on the [Bardioc1977/morphium](https://github.com/Bardioc1977/morphium) fork.
> Some features may not yet be available in the upstream [sboesebeck/morphium](https://github.com/sboesebeck/morphium).
> Alignment with upstream is planned.

A comprehensive demo application showcasing all [Morphium](https://github.com/sboesebeck/morphium) ORM
features with [Quarkus](https://quarkus.io) and [Qute](https://quarkus.io/guides/qute) templates.

Built on the [quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium) extension.

## Features

The showcase demonstrates the following Morphium ORM capabilities through
interactive web pages:

- **Product Catalog** -- CRUD operations, `@Entity`, `@Embedded`, `@Version` (optimistic locking)
- **Blog** -- References between entities, `@CreationTime`, pagination
- **Banking** -- `@MorphiumTransactional` declarative transactions, CDI transaction events
- **Analytics** -- Aggregation pipeline, `$group`, `$sort`, `$match`
- **Audit Log** -- `@PreStore` / `@PostStore` lifecycle callbacks
- **Geospatial** -- `2dsphere` index, `$near` queries
- **Messaging** -- Morphium's built-in messaging system
- **Polymorphism** -- Inheritance mapping, custom type mappers
- **Query Builder** -- Fluent query API, complex filters, sorting, projection
- **Bulk Import** -- `SequenceGenerator`, batch inserts
- **Docs Hub** -- Integrated Morphium documentation rendered from Markdown

## Prerequisites

| Dependency | Minimum version |
|---|---|
| Java | 21 |
| Maven | 3.9 |
| Docker | (for Dev Services) |

## Quick Start

```bash
# Clone and run (Dev Services starts MongoDB automatically)
git clone https://github.com/Bardioc1977/quarkus-morphium-showcase.git
cd quarkus-morphium-showcase
mvn quarkus:dev
```

Open [http://localhost:8080](http://localhost:8080) in your browser.

> **Note:** The quarkus-morphium extension is resolved from
> [GitHub Packages](https://github.com/Bardioc1977/quarkus-morphium/packages).
> For local development, you can also build the extension from source:
> ```bash
> git clone https://github.com/Bardioc1977/quarkus-morphium.git
> cd quarkus-morphium && mvn install -DskipTests && cd ..
> ```

## Docker Quickstart

Run the showcase with a single command -- no JDK, Maven, or local MongoDB required:

```bash
docker compose up
```

Open [http://localhost:8080](http://localhost:8080) in your browser.

To stop and clean up:

```bash
# Stop containers (keep data)
docker compose down

# Stop containers and remove data
docker compose down -v
```

### Local build

To build the Docker image locally instead of pulling from GHCR:

```bash
# Set GitHub credentials for Maven (quarkus-morphium SNAPSHOT resolution)
export GITHUB_ACTOR=your-github-username
export GITHUB_TOKEN=your-github-pat

# Build and start
docker compose -f docker-compose.yml -f docker-compose.build.yml up --build
```

## Configuration

The showcase uses Quarkus Dev Services -- a MongoDB container is started
automatically in dev and test mode. No manual MongoDB setup required.

```properties
# src/main/resources/application.properties
quarkus.morphium.database=morphium_showcase
quarkus.morphium.devservices.enabled=true
quarkus.morphium.devservices.image-name=mongo:8
```

## Testing

Tests use Morphium's `InMemDriver` -- no Docker or MongoDB needed:

```bash
mvn test
```

## Building

```bash
# Full build with tests
mvn verify

# Skip tests
mvn package -DskipTests
```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).

## License

This project is licensed under the [Apache License 2.0](LICENSE).

## Related Projects

- [Bardioc1977/morphium](https://github.com/Bardioc1977/morphium) -- fork of Morphium used by this showcase
- [sboesebeck/morphium](https://github.com/sboesebeck/morphium) -- upstream Morphium ORM
- [quarkus-morphium](https://github.com/Bardioc1977/quarkus-morphium) -- the Quarkus extension for Morphium
- [Quarkus](https://quarkus.io) -- the supersonic, subatomic Java framework
