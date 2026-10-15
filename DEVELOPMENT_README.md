# Morphium Developer Guide

This guide is intended for developers who want to contribute to the Morphium project. It provides a comprehensive overview of the project's architecture, development process, and testing procedures.

## What is Morphium?

Morphium is a feature-rich Object-Document Mapper (ODM) for MongoDB and a messaging framework for Java. It provides a high-level API for interacting with MongoDB, allowing developers to work with Java objects instead of raw BSON documents.

### Core Features

*   **Object-Document Mapping:** Morphium maps Plain Old Java Objects (POJOs) to MongoDB documents. It uses annotations to configure the mapping, making it easy to define entities and their relationships.
*   **Messaging System:** Morphium includes a built-in messaging system that uses MongoDB as a message broker. This allows for asynchronous communication between different parts of an application or between different applications.
*   **Caching:** Morphium provides a multi-level caching mechanism to improve performance. It can cache query results and objects, reducing the number of queries to the database.
*   **In-Memory Driver:** For testing purposes, Morphium provides an in-memory driver that simulates a MongoDB instance. This allows for fast and isolated unit tests without the need for a running MongoDB server.
*   **Custom Wire-Protocol Driver:** Morphium has its own implementation of the MongoDB wire protocol, which is optimized for performance and provides advanced features.

## Getting Started

### Prerequisites

*   Java 21 or newer
*   Maven 3.6 or newer

### Building the Project

To build the project, run the following command from the root directory:

```bash
mvn clean install
```

This will compile the source code, run all tests, and install the Morphium artifact into your local Maven repository.

## Project Structure

*   `src/main/java`: The main source code for the Morphium library.
    *   `de/caluga/morphium/`: The root package for all Morphium classes.
    *   `de/caluga/morphium/annotations/`: Annotations used for object mapping and configuration.
    *   `de/caluga/morphium/driver/`: The custom MongoDB wire protocol driver.
    *   `de/caluga/morphium/messaging/`: The messaging system implementation.
    *   `de/caluga/morphium/cache/`: The caching implementation.
*   `src/test/java`: Test classes for the Morphium library.
*   `docs`: Project documentation in Markdown format.
*   `scripts`: Helper scripts for developers.
*   `pom.xml`: The Maven project configuration.

## Testing

Morphium has a comprehensive test suite that covers all of its features. Tests are written using JUnit 5.

### Running Tests

The easiest way to run tests is by using the `runtests.sh` script in the root directory. This script provides a variety of options for running tests in different modes.

```bash
# Run all tests with the in-memory driver (fastest)
./runtests.sh

# Run only the core tests
./runtests.sh --tags core

# Run tests in parallel
./runtests.sh --parallel 4

# Run a single test class
./runtests.sh --test-class de.caluga.test.morphium.BasicFunctionalityTests

# For more options, see the "Developer Scripts" section below or run:
./runtests.sh --help
```

### Test Categories

Tests are organized into different categories using JUnit tags. This allows for running specific subsets of tests. The main categories are:

*   `core`: Core functionality of the ODM.
*   `messaging`: Messaging system tests.
*   `cache`: Caching functionality tests.
*   `driver`: Wire protocol driver tests.

### Developer Scripts

The `./scripts` directory contains several helpful scripts for developers. These scripts are designed to be run from the root of the project.

*   `./scripts/chooseTest.sh`: Interactively select a test class and method to run using `fzf`.
*   `./scripts/cleanup.sh`: Removes temporary files created during test runs.
*   `./scripts/getFailedTests.sh`: Lists the names of all failed tests from the logs.
*   `./scripts/getStats.sh`: (Obsolete) Use `runtests.sh --stats` instead.
*   `./scripts/getTestClassesRun.sh`: Lists all test classes that were run.
*   `./scripts/rerunSingleFailed.sh`: Interactively select and rerun a single failed test from the logs using `fzf`.
*   `./scripts/showlog.sh`: Interactively view test logs using `fzf` and `bat`.
*   `./scripts/viewTestlogs.sh`: View the logs of currently running tests in parallel using `multitail`.

## Task ledger

Planned work (defects, API gaps, analyses, documentation) is tracked in a small file-based ledger:

*   `docs/tasks/ledger.yaml` is the **single source of truth**. Add, change and close tasks there.
*   `docs/tasks/ledger.md` is the readable view. It is **generated** from the YAML file; **never edit it by hand**.
*   Task ids have the form `MOR-NNN` and are never reused. Get the next free one with
    `python3 scripts/ledger/ledger_md.py --next-id`.
*   A task has `id`, `title`, `type`, `priority` (1 first, 3 later), `status` (`open`, `in_progress`, `blocked`,
    `done`) and usually `effort` (S/M/L), `description`, `acceptance`, `depends_on` and `source`. A `done` task needs
    `evidence`, a `blocked` task needs `blocked_by`; the generator refuses the ledger otherwise.

```bash
# Regenerate docs/tasks/ledger.md after editing ledger.yaml
python3 scripts/ledger/ledger_md.py

# Verify that ledger.md is up to date (exit code 1 if not), writes nothing
python3 scripts/ledger/ledger_md.py --check

# Unit tests of the generator
python3 -m unittest scripts/ledger/test_ledger_md.py
```

The scripts need Python 3 and **PyYAML** (`python3 -m pip install pyyaml`). The `--check` command and the unit tests
also run in the GitHub `Build` workflow. With Claude Code, use the `ledger` skill or the `/ledger` command; they keep
YAML and Markdown in sync. The tooling (file formats, status and type vocabularies, validation rules) is shared with
other projects, so keep it compatible. `docs/tasks/` and `docs/research/` are excluded from the published MkDocs site.
