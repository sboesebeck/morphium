# Developer Testing Guide

This guide explains how to run and write tests for Morphium. It covers the test infrastructure, the `MultiDriverTestBase` class, and the `runtests.sh` script.

## Quick Start

```bash
# Fast local testing with InMemoryDriver (no MongoDB needed)
./runtests.sh --driver inmem --restart

# Test against MorphiumServer (single node - recommended for most testing)
./runtests.sh --morphium-server --driver pooled --restart

# Test against MorphiumServer replica set (for replication testing)
./runtests.sh --morphium-server-replicaset --driver pooled --restart

# Run specific test class
./runtests.sh --driver inmem --test BasicFunctionalityTest

# Run with Maven directly
mvn test -Dmorphium.test.driver=inmem -Dtest=BasicFunctionalityTest
```

## Test Infrastructure Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        runtests.sh                               │
│  (orchestrates test runs, manages MorphiumServer, collects stats)│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Maven Surefire Plugin                         │
│         (executes JUnit 5 tests with system properties)          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     MultiDriverTestBase                          │
│    (provides Morphium instances configured per driver type)      │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        ┌──────────┐   ┌──────────┐   ┌──────────────┐
        │InMemory  │   │ Pooled   │   │SingleConnect │
        │ Driver   │   │ Driver   │   │   Driver     │
        └──────────┘   └──────────┘   └──────────────┘
              │               │               │
              ▼               ▼               ▼
        ┌──────────┐   ┌─────────────────────────────┐
        │In-Memory │   │  MongoDB / MorphiumServer   │
        │ Storage  │   │                             │
        └──────────┘   └─────────────────────────────┘
```

## MultiDriverTestBase

`MultiDriverTestBase` is the foundation for parameterized tests that run against multiple driver types.

### How It Works

1. **Test classes extend `MultiDriverTestBase`** and use JUnit 5 `@ParameterizedTest` with `@MethodSource`
2. **Method sources** like `getMorphiumInstances()` return `Stream<Arguments>` with pre-configured `Morphium` instances
3. **Each test method** receives a `Morphium` instance as parameter and runs independently
4. **Database isolation**: Each driver instance gets a unique database name (`morphium_test_1`, `morphium_test_2`, etc.)

### Available Method Sources

| Method Source | Drivers Included |
|---------------|------------------|
| `getMorphiumInstances()` | InMemory, Pooled |
| `getMorphiumInstancesNoSingle()` | InMemory, Pooled |
| `getMorphiumInstancesPooledOnly()` | Pooled only |
| `getMorphiumInstancesSingleOnly()` | SingleConnect only |
| `getMorphiumInstancesInMemOnly()` | InMemory only |
| `getMorphiumInstancesNoInMem()` | Pooled, SingleConnect |
| `getInMemInstanceOnly()` | InMemory only |

### Writing a Test

```java
import de.caluga.test.mongo.suite.base.MultiDriverTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class MyFeatureTest extends MultiDriverTestBase {

    @ParameterizedTest
    @MethodSource("getMorphiumInstances")  // Runs with InMemory and Pooled
    public void testMyFeature(Morphium morphium) throws Exception {
        try (morphium) {  // Auto-close when done
            // Your test code here
            UncachedObject obj = new UncachedObject("test", 1);
            morphium.store(obj);

            var result = morphium.createQueryFor(UncachedObject.class)
                .f("counter").eq(1)
                .get();

            assertNotNull(result);
            assertEquals("test", result.getStrValue());
        }
    }
}
```

### Driver Selection via System Properties

The `MultiDriverTestBase` respects these system properties:

| Property | Values | Description |
|----------|--------|-------------|
| `morphium.driver` | `inmem`, `pooled`, `single`, `all` | Which drivers to include |
| `morphium.database` | e.g., `morphium_test_slot1` | Base database name prefix |
| `morphium.tests.external` | `true`/`false` | Enable external MongoDB tests |

When `morphium.tests.external` is **not** set (default), only `InMemoryDriver` is used regardless of `morphium.driver` setting. This allows fast local testing without MongoDB.

### TestConfig

`TestConfig.java` centralizes test configuration with this precedence:

1. **System properties** (`-Dmorphium.xxx`)
2. **Environment variables** (`MORPHIUM_XXX`)
3. **`morphium-test.properties`** resource file
4. **Built-in defaults**

Key configuration options:

```properties
# Connection
morphium.uri=mongodb://localhost:27017/test
morphium.hostSeed=localhost:27017,localhost:27018
morphium.database=morphium_test

# Driver
morphium.driver=pooled  # pooled|single|inmem

# Authentication
morphium.user=testuser
morphium.pass=testpass
morphium.authDb=admin

# Timeouts (milliseconds)
morphium.connectionTimeout=2000
morphium.readTimeout=10000
morphium.serverSelectionTimeout=15000
```

## runtests.sh Script

The `runtests.sh` script provides a convenient wrapper around Maven with additional features.

### Command Line Options

#### Driver Selection
```bash
--driver NAME       # pooled|single|inmem (default: inmem if no external)
--external          # Enable external MongoDB tests
```

#### Test Selection
```bash
--test PATTERN      # Run only matching test classes
--tags LIST         # Include JUnit 5 tags (comma-separated)
--exclude-tags LIST # Exclude JUnit 5 tags
--rerunfailed       # Rerun only previously failed tests
```

Available tags: `core`, `messaging`, `driver`, `inmemory`, `aggregation`, `cache`, `admin`, `performance`, `encryption`, `jms`, `geo`, `util`, `external`

#### MorphiumServer Options
```bash
--morphium-server              # Start single-node MorphiumServer (recommended)
--morphium-server-replicaset   # Start 3-node replica set (ports 27017-27019)
```

#### Execution Control
```bash
--parallel N        # Run tests in N parallel slots (1-16)
--retry N           # Retry failed tests N times
--restart           # Clear logs and start fresh
--skip              # Skip already-run tests (continue mode)
```

#### Output Control
```bash
--logs NUM          # Number of log lines to show
--refresh NUM       # Refresh display every NUM seconds
--stats             # Show test statistics
--verbose           # Enable verbose test output
```

### Examples

```bash
# Fast development cycle - InMemory only
./runtests.sh --driver inmem --test MyNewTest --restart

# Full test against MorphiumServer single node
./runtests.sh --morphium-server --driver pooled --restart

# Parallel testing for speed
./runtests.sh --driver inmem --parallel 4 --restart

# Test specific tags
./runtests.sh --driver inmem --tags core,messaging --restart

# Rerun failed tests with retries
./runtests.sh --rerunfailed --retry 3

# External MongoDB with authentication
./runtests.sh --external --driver pooled \
    --uri mongodb://user:pass@mongo1:27017/test?authSource=admin
```

### Log Files

- **Sequential runs**: `test.log/<TestClass>.log`
- **Parallel runs**: `test.log/slot_<N>/<TestClass>.log`
- **MorphiumServer logs**: `.morphiumserver-local/logs/morphiumserver_<port>.log`
- **Failed tests summary**: `failed.txt`

## Best Practices

### 1. Use InMemoryDriver for Development

```bash
./runtests.sh --driver inmem --test YourTest
```

- No external dependencies
- Fast execution
- Full feature parity for most operations

### 2. Use try-with-resources

```java
@ParameterizedTest
@MethodSource("getMorphiumInstances")
public void myTest(Morphium morphium) throws Exception {
    try (morphium) {  // Ensures cleanup
        // test code
    }
}
```

### 3. Wait for Async Operations

```java
// Use TestUtils for waiting
TestUtils.waitForConditionToBecomeTrue(5000, "Data not stored",
    () -> morphium.createQueryFor(MyClass.class).countAll() == expectedCount);
```

### 4. Handle Replica Set Timing

When testing against replica sets (MorphiumServer or MongoDB), data replication takes time:

```java
// Allow time for replication (increase timeout for replica sets)
TestUtils.waitForConditionToBecomeTrue(5000, "Replication timeout",
    () -> query.countAll() == expected);
```

### 5. Clean Test Data

`MultiDriverTestBase` automatically drops test databases before each test run. For additional cleanup within tests:

```java
morphium.dropCollection(MyClass.class);
```

### 6. Tag Your Tests Appropriately

```java
@Tag("core")        // Core functionality
@Tag("messaging")   // Messaging tests
@Tag("performance") // Slow performance tests
@Tag("external")    // Requires external MongoDB
@Tag("inmemory")    // InMemory-specific tests
```

### 7. Use Appropriate Method Sources

```java
// For tests that work with any driver
@MethodSource("getMorphiumInstances")

// For tests requiring real MongoDB features
@MethodSource("getMorphiumInstancesNoInMem")

// For InMemory-specific behavior tests
@MethodSource("getMorphiumInstancesInMemOnly")
```

## Troubleshooting

### Test Hangs

1. Check for infinite loops in wait conditions
2. Verify MorphiumServer is running (if using `--morphium-server`)
3. Check connection timeouts in logs

### Flaky Tests with Replica Sets

Replica set tests can be timing-sensitive:

1. Increase wait timeouts for replication
2. Use `--morphium-server` (single node) instead of `--morphium-server-replicaset` for most tests
3. Run flaky tests with `--retry 2`

### Database Conflicts in Parallel Mode

Each parallel slot uses a unique database prefix. If you see conflicts:

1. Ensure tests use `morphium` parameter, not shared static instances
2. Check that `--restart` is used to clear old data
3. Verify tests close their Morphium instances properly

### View Test Statistics

```bash
./runtests.sh --stats           # Full statistics
./runtests.sh --stats --noreason # Just test names
cat failed.txt                   # List of failed tests
```

## Running with Maven Directly

For IDE integration or CI pipelines:

```bash
# InMemory (default, no external MongoDB needed)
mvn test -Dmorphium.test.driver=inmem

# External MongoDB
mvn test -Pexternal -Dmorphium.driver=pooled \
    -Dmorphium.uri=mongodb://localhost:27017/test

# Specific test
mvn test -Dtest=BasicFunctionalityTest -Dmorphium.test.driver=inmem

# With tags
mvn test -Dgroups=core,messaging -DexcludedGroups=performance
```

## See Also

- [Test Runner Reference](./test-runner.md) - Additional `runtests.sh` details
- [InMemory Driver](./inmemory-driver.md) - InMemoryDriver specifics
- [MorphiumServer](./morphium-server.md) - MorphiumServer documentation
