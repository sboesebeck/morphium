# Morphium 6.0

**Feature-rich MongoDB ODM and messaging framework for Java 21+**

Available languages: English | [Deutsch](README.de.md)

- 🗄️ **High-performance object mapping** with annotation-driven configuration
- 📨 **Integrated message queue** backed by MongoDB (no extra infrastructure)
- ⚡ **Multi-level caching** with cluster-wide invalidation
- 🔌 **Custom MongoDB wire-protocol driver** tuned for Morphium
- 🧪 **In-memory driver** for fast tests (no MongoDB required)
- 🎯 **JMS API (experimental)** for standards-based messaging
- 🚀 **Java 21** with virtual threads for optimal concurrency

[![Maven Central](https://img.shields.io/maven-central/v/de.caluga/morphium.svg)](https://search.maven.org/artifact/de.caluga/morphium)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🎯 Why Morphium?

Morphium is the only Java ODM that ships a message queue living inside MongoDB. If you already run MongoDB, you can power persistence, messaging, caching, and change streams with a single component.

| Feature | Morphium | Spring Data + RabbitMQ | Kafka |
|---------|----------|------------------------|-------|
| Infrastructure | MongoDB only | MongoDB + RabbitMQ | MongoDB + Kafka |
| Setup complexity | ⭐ Very low | ⭐⭐⭐ Medium | ⭐⭐⭐⭐⭐ High |
| Message persistence | Built in | Optional | Built in |
| Message priority | ✅ Yes | ✅ Yes | ❌ No |
| Distributed locks | ✅ Yes | ❌ No | ❌ No |
| Throughput (internal tests) | ~8K msg/s | 10K–50K msg/s | 100K+ msg/s |
| Operations | ⭐ Very easy | ⭐⭐ Medium | ⭐⭐⭐⭐ Complex |

_* Numbers are indicative and depend heavily on hardware and workload._

## 📚 Documentation

### Quick access
- **[Documentation hub](docs/index.md)** – entry point for all guides
- **[Overview](docs/overview.md)** – core concepts, quick start, compatibility
- **[Migration v5→v6](docs/howtos/migration-v5-to-v6.md)** – step-by-step upgrade guide
- **[InMemory Driver Guide](docs/howtos/inmemory-driver.md)** – capabilities, caveats, testing tips

### More resources
- Aggregation examples: `docs/howtos/aggregation-examples.md`
- Messaging implementations: `docs/howtos/messaging-implementations.md`
- Performance guide: `docs/performance-scalability-guide.md`
- Production deployment: `docs/production-deployment-guide.md`
- Monitoring & troubleshooting: `docs/monitoring-metrics-guide.md`

## 🚀 What’s New in v6.0

### Java 21 & Modern Language Features
- **Virtual threads** for high-throughput messaging and change streams
- **Pattern matching & records** across driver and mapping layers
- **Sealed class support** for cleaner domain models

### Messaging Improvements
- **Fewer duplicates** thanks to refined message processing
- **Virtual-thread integration** for smoother concurrency
- **Higher throughput** confirmed in internal benchmarking
- **Distributed locking** for coordinated multi-instance deployments

### In-Memory Driver Enhancements
- **No MongoDB required** for unit tests or CI pipelines
- **Significantly faster test cycles** in pure in-memory mode
- **Broad operator coverage** with clearly documented exceptions
- **Change streams & transactions** available for integration testing

### Documentation Overhaul
- Complete rewrite of the guide set
- Practical examples and end-to-end use cases
- Dedicated migration playbook from 5.x to 6.x
- Architecture insights and best practices

## ✅ Requirements
- Java 21 or newer
- MongoDB 5.0+ for production deployments
- Maven

Maven dependencies:
```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>[6.0.0,)</version>
</dependency>
<dependency>
  <groupId>org.mongodb</groupId>
  <artifactId>bson</artifactId>
  <version>4.7.1</version>
</dependency>
```

Migrating from v5? → `docs/howtos/migration-v5-to-v6.md`

## ⚡ Quick Start

### Maven dependency

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>6.0.0</version>
</dependency>
```

### Object mapping example

```java
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import java.time.LocalDateTime;

// Entity definition
@Entity
public class User {
    @Id
    private MorphiumId id;
    private String name;
    private String email;
    private LocalDateTime createdAt;
    // getters/setters
}

// Configuration
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");

Morphium morphium = new Morphium(cfg);

// Store entity
User user = new User();
user.setName("John Doe");
user.setEmail("john@example.com");
user.setCreatedAt(LocalDateTime.now());
morphium.store(user);

// Query
List<User> users = morphium.createQueryFor(User.class)
    .f("email").matches(".*@example.com")
    .sort("createdAt")
    .asList();
```

### Messaging example

```java
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;

// Messaging setup
MorphiumMessaging messaging = morphium.createMessaging();
messaging.setSenderId("my-app");
messaging.start();

// Send a message
Msg message = new Msg("orderQueue", "Process Order", "Order #12345");
message.setPriority(5);
message.setTtl(300000); // 5 minutes
messaging.sendMessage(message);

// Receive messages
messaging.addListenerForTopic("orderQueue", (m, msg) -> {
    log.info("Processing {}", msg.getValue());
    // process order ...
    return null; // no reply
});
```

### Properties & environment configuration

```bash
# Environment variables
export MONGODB_URI='mongodb://user:pass@localhost:27017/app?replicaSet=rs0'
export MORPHIUM_DRIVER=inmem

# System properties
mvn -Dmorphium.uri='mongodb://localhost/mydb' test

# Properties file (morphium.properties)
morphium.hosts=mongo1.example.com:27017,mongo2.example.com:27017
morphium.database=myapp
morphium.replicaSet=myReplicaSet
```

## 🧪 Tests & Test Runner

### Maven
```bash
# All tests
mvn test

# Full build with checks
mvn clean verify

# Tagged test selection
mvn test -Dgroups="core,messaging"

# Run against a real MongoDB instance
mvn test -Dmorphium.driver=pooled -Dmorphium.uri=mongodb://localhost/testdb
```

### `./runtests.sh` helper
```bash
# Default: in-memory driver
./runtests.sh

# Run tagged suites
./runtests.sh --tags core,messaging

# Parallel runs
./runtests.sh --parallel 8 --tags core

# Retry only failed methods
./runtests.sh --rerunfailed
./runtests.sh --rerunfailed --retry 3

# Target a MongoDB cluster
./runtests.sh --driver pooled --uri mongodb://mongo1,mongo2/testdb

# Single test class
./runtests.sh CacheTests

# Statistics
./runtests.sh --stats
./getFailedTests.sh  # list failed methods
```

**New in v6.0**
- ✅ **Method-level reruns**: `--rerunfailed` only re-executes failing methods
- ✅ **No more hangs**: known deadlocks resolved
- ✅ **Faster iteration**: noticeably quicker partial retries
- ✅ **Better filtering**: class-name filters now reliable

Run `./runtests.sh --help` to see every option.

### Test configuration precedence

`TestConfig` consolidates all test settings. Priority order:
1. System properties (`-Dmorphium.*`)
2. Environment variables (`MORPHIUM_*`, `MONGODB_URI`)
3. `src/test/resources/morphium-test.properties`
4. Defaults (localhost:27017)

## 🔧 MorphiumServer & InMemoryDriver

### InMemoryDriver – MongoDB-free testing

The in-memory driver provides a largely MongoDB-compatible data store fully in memory:

**Features**
- ✅ Full CRUD operations
- ✅ Rich query operator coverage
- ✅ Aggregation stages such as `$match`, `$group`, `$project`
- ✅ Single-instance transactions
- ✅ Basic change streams
- ✅ JavaScript `$where` support

**Performance**
- Significantly faster than external MongoDB for tests
- No network latency
- No disk I/O
- Ideal for CI/CD pipelines

**Usage**
```bash
# All tests with the in-memory driver
./runtests.sh --driver inmem

# Specific tests
mvn test -Dmorphium.driver=inmem -Dtest="CacheTests"
```

See `docs/howtos/inmemory-driver.md` for feature coverage and limitations.

### MorphiumServer – Standalone MongoDB replacement

MorphiumServer runs the Morphium wire-protocol driver in a separate process:

```bash
# Start the server
java -jar morphium-6.0.0.jar de.caluga.morphium.server.MorphiumServer

# Connect with mongosh or MongoDB Compass
mongosh mongodb://localhost:27017
```

**Use cases**
- Local development without installing MongoDB
- CI environments
- Embedded database for desktop applications
- Smoke-testing MongoDB tooling (mongosh, Compass, mongodump, …)

**Current limitations**
- No replica set emulation (planned for 6.x)
- No sharding support
- Some advanced aggregation operators and joins still missing

## 🚀 Production Use Cases

Organizations run Morphium in production for:
- **E-commerce**: order processing with guaranteed delivery
- **Financial services**: coordinating transactions across microservices
- **Healthcare**: patient-data workflows with strict compliance
- **IoT platforms**: device state synchronization and command distribution
- **Content management**: document workflows and event notifications

## 🤝 Community & Contribution

### Stay in touch
- **Slack**: [Team Morphium](https://join.slack.com/t/team-morphium/shared_invite/enQtMjgwODMzMzEzMTU5LTA1MjdmZmM5YTM3NjRmZTE2ZGE4NDllYTA0NTUzYjU2MzkxZTJhODlmZGQ2MThjMGY0NmRkMWE1NDE2YmQxYjI)
- **Blog**: https://caluga.de
- **GitHub**: [sboesebeck/morphium](https://github.com/sboesebeck/morphium)
- **Issues**: Report bugs or request features on GitHub

### Contributing

We appreciate pull requests! Areas where help is especially welcome:
- **InMemoryDriver**: expanding MongoDB feature coverage
- **Documentation**: tutorials, examples, translations
- **Performance**: profiling and benchmarks
- **Tests**: broader scenarios and regression coverage

**How to contribute**
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push the branch (`git push origin feature/AmazingFeature`)
5. Open a pull request

**Tips**
- Respect test tags (`@Tag("inmemory")`, `@Tag("external")`)
- Run `./runtests.sh --tags core` before submitting
- Update documentation when you change APIs

## 📜 License

Apache License 2.0 – see [LICENSE](LICENSE) for details.

## 🙏 Thanks

Thanks to every contributor who helped ship Morphium 6.0 and to the MongoDB community for continuous feedback.

---

**Questions?** Open an issue on [GitHub](https://github.com/sboesebeck/morphium/issues) or browse the [documentation](docs/index.md).

**Planning an upgrade?** Follow the [migration guide](docs/howtos/migration-v5-to-v6.md).

Enjoy Morphium 6.0! 🚀

*Stephan Bösebeck & the Morphium team*
