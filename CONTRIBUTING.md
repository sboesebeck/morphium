# Contributing to Quarkus Morphium Showcase

Thank you for your interest in contributing! This document provides guidelines and
information for contributors.

## How to Contribute

### Reporting Bugs

- Use [GitHub Issues](https://github.com/Bardioc1977/quarkus-morphium-showcase/issues) to report bugs
- Include the Quarkus version, Morphium version, and Java version
- Provide a minimal reproducer if possible
- Describe the expected and actual behavior

### Suggesting Features

- Open a [GitHub Issue](https://github.com/Bardioc1977/quarkus-morphium-showcase/issues) describing the feature
- Explain the motivation and use case
- Discuss the proposed approach before starting implementation

### Submitting Pull Requests

1. Fork the repository and create a feature branch from `main`
2. Make your changes following the code conventions below
3. Add or update tests as appropriate
4. Ensure `mvn verify` passes locally
5. Open a pull request against `main`

## Development Setup

### Prerequisites

- JDK 21+
- Maven 3.9+
- Docker (for Dev Services – MongoDB container is started automatically)

### Building from Source

```bash
# Clone the quarkus-morphium extension (SNAPSHOT required)
git clone https://github.com/Bardioc1977/quarkus-morphium.git
cd quarkus-morphium
mvn install -DskipTests
cd ..

# Clone and build the showcase
git clone https://github.com/Bardioc1977/quarkus-morphium-showcase.git
cd quarkus-morphium-showcase
mvn verify
```

### Running the Showcase

```bash
# Dev mode with live reload (MongoDB container starts automatically)
mvn quarkus:dev

# Open http://localhost:8080 in your browser
```

### Running Tests

```bash
# All tests (uses InMemDriver, no Docker needed)
mvn test

# Full verification including compilation checks
mvn verify
```

## Code Conventions

- Java 21+ features are welcome (records, sealed classes, pattern matching, etc.)
- Follow existing code style (4-space indentation, no tabs)
- No `sun.*` or `jdk.internal.*` imports
- Use Lombok where it reduces boilerplate

## License

By contributing to this project, you agree that your contributions will be licensed
under the [Apache License 2.0](LICENSE).
