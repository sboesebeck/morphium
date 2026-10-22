# Morphium

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="branding/morphium-logo-dark.svg">
    <img src="branding/morphium-logo.svg" alt="Morphium" width="640">
  </picture>
</p>

<p align="center">
  <b>The MongoDB ODM for Java 21+ — with a message queue, multi-level caching, an in-memory driver,
  and a complete Jakarta Data implementation.</b>
</p>

<p align="center">
  <a href="https://search.maven.org/artifact/de.caluga/morphium"><img src="https://img.shields.io/maven-central/v/de.caluga/morphium.svg" alt="Maven Central"></a>
  <a href="https://github.com/sboesebeck/morphium/releases"><img src="https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsboesebeck%2Fmorphium%2Ftest-results%2Fbadges%2Ftests.json" alt="Tests"></a>
  <a href="#-jakarta-data"><img src="https://img.shields.io/badge/Jakarta%20Data%20TCK-73%20of%2073-brightgreen" alt="Jakarta Data TCK: 73 of 73"></a>
  <img src="https://img.shields.io/badge/Java-21%2B-orange" alt="Java 21+">
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
</p>

<p align="center">
  <a href="#-quick-start">Quick start</a> ·
  <a href="#-jakarta-data">Jakarta Data</a> ·
  <a href="#-frameworks">Quarkus &amp; Spring Boot</a> ·
  <a href="#-messaging">Messaging</a> ·
  <a href="#-testing-without-mongodb">Testing</a> ·
  <a href="#-poppydb">PoppyDB</a> ·
  <a href="docs/index.md">Documentation</a> ·
  <a href="https://morphium.kopp-cloud.de/">Live showcase</a>
</p>

Available languages: English and [Deutsch](README.de.md)

---

## What is Morphium?

Morphium maps Java objects to MongoDB documents — with its own wire-protocol driver, so there is no second
mapping layer between you and the server. If you already run MongoDB, the same component also gives you a
message queue, caches that are invalidated across the cluster, change streams, transactions and a
test backend that needs no MongoDB at all.

|  |  |
|---|---|
| 🗄️ **Object mapping** | Annotation-driven, lifecycle callbacks, `@Reference`, `@Version` optimistic locking, field-level encryption, enums by name or ordinal |
| 📚 **Jakarta Data 1.0** | `@Repository` interfaces for Quarkus and Spring Boot. **Passes all 73 tests of the standalone TCK suite** |
| 📨 **Message queue** | Topics, priorities, exclusive delivery, request/response, distributed locks — in MongoDB, no broker to run |
| ⚡ **Caching** | Per-entity `@Cache`, query-result cache, cluster-wide invalidation, JCache (JSR-107) |
| 🧪 **In-memory driver** | Fast unit tests with the same API, no container, no installation |
| 🌱 **[PoppyDB](#-poppydb)** | A MongoDB-compatible server in one jar: replica sets, SCRAM, TLS — also a dedicated message broker |
| 🔌 **Frameworks** | [Quarkus extension](docs/quarkus-extension.md) · [Spring Boot starter](docs/spring-boot.md) |

## ⚡ Quick start

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>6.3.14</version>
</dependency>
```

```java
@Entity
public class Order {
    @Id private MorphiumId id;
    private String customer;
    private String status;
    private double total;
    private LocalDateTime createdAt;
    // getters and setters
}

MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("shop");
cfg.clusterSettings().addHostToSeed("localhost", 27017);

try (Morphium morphium = new Morphium(cfg)) {
    Order order = new Order();
    order.setCustomer("Ada");
    order.setStatus("OPEN");
    order.setTotal(129.90);
    morphium.store(order);

    List<Order> open = morphium.createQueryFor(Order.class)
        .f("status").eq("OPEN")
        .f("total").gt(100)
        .sort("-createdAt")
        .asList();
}
```

No MongoDB at hand? Set `cfg.driverSettings().setDriverName("InMemDriver")` and the same code runs in memory.
Configuration can also come from `MONGODB_URI`, system properties or a properties file — see the
[configuration reference](docs/configuration-reference.md).

## 📚 Jakarta Data

Morphium implements [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/): declare an interface, get the
implementation. On Quarkus it is generated at build time, on Spring Boot it is a runtime proxy; both use the same
runtime, `morphium-jakarta-data`.

```java
@Repository
public interface OrderRepository extends CrudRepository<Order, MorphiumId> {

    // derived from the name; IgnoreCase works in front of any operator
    List<Order> findByStatusAndTotalGreaterThan(String status, double min);
    List<Order> findByCustomerIgnoreCaseContains(String part);
    Optional<Order> findFirstByCustomerOrderByCreatedAtDesc(String customer);
    long countAll();

    // keyset pagination: the cursor continues where the last page ended
    CursoredPage<Order> findByStatusOrderByCreatedAtDesc(String status, PageRequest page);

    // JDQL, with named or ordinal parameters, groups and aggregates mapped to a record
    @Query("SELECT status, COUNT(this), SUM(total) GROUP BY status HAVING COUNT(this) > :min")
    List<StatusStats> statistics(@Param("min") long min);

    // set-based writes in ONE server command
    @Query("UPDATE Order SET status = :to, total = total * 1.19 WHERE status = :from")
    long move(@Param("from") String from, @Param("to") String to);

    @Query("DELETE FROM Order WHERE status = 'CANCELLED'")
    long purge();

    record StatusStats(String status, long count, double total) {}
}
```

**What is covered:** query derivation from method names · `@Find`, `@Delete`, `@Insert`, `@Save`, `@Update` in every
shape the specification allows (one entity, collection, array, varargs; `void`, entity, list, `boolean`, `int`, `long`) ·
JDQL `SELECT`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`, `UPDATE` and `DELETE` statements · `Page`, `CursoredPage`, `Sort`,
`Order`, `Limit` · `Stream`, arrays and `CompletionStage` results · repositories that extend nothing and work on several
entities.

**Beyond the specification** (clearly marked as Morphium-specific): `@AtomicUpdate` — one atomic conditional update or
find-and-modify, for leases, counters and "create if absent" — and the `MorphiumRepository` escape hatch to the
`Morphium` instance for aggregation pipelines, change streams and messaging.

### Conformance

The standalone entity tests of the **Jakarta Data TCK 1.0.0 pass 73 of 73**, on the in-memory driver and on a real
MongoDB 8. The suite runs as a normal test (`JakartaDataTckTest` in `spring-boot-morphium`) and fails when a test that is
not on its exclusion list fails — the list is empty.

```bash
cd spring-boot-morphium && mvn -nsu test -Dtest=JakartaDataTckTest
```

The TCK sorts enums by the order of their constants, so the test stores enums by ordinal
(`morphium.enum-type=ORDINAL`, see [Enums](docs/configuration-reference.md#enums)). The CDI, signature and web parts of the TCK need a
Jakarta EE container and are not run. How it works and what each exclusion meant on the way:
[Jakarta Data guide](docs/jakarta-data.md#conformance-jakarta-data-tck).

## 🔌 Frameworks

<table>
<tr><th>Quarkus</th><th>Spring Boot</th></tr>
<tr><td valign="top">

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>quarkus-morphium</artifactId>
  <version>6.3.14</version>
</dependency>
```

```properties
quarkus.morphium.database=shop
quarkus.morphium.hosts=localhost:27017
```

```java
@Inject OrderRepository orders;

@MorphiumTransactional
void checkout(Order o) { orders.save(o); }
```

</td><td valign="top">

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium-spring-boot-starter</artifactId>
  <version>6.3.14</version>
</dependency>
```

```properties
morphium.database=shop
morphium.hosts=localhost:27017
```

```java
@EnableMorphiumRepositories
@SpringBootApplication
class Shop { }

@Autowired OrderRepository orders;
```

</td></tr>
</table>

Both give you the repositories above, health checks, `@MorphiumTransactional`, and the same configuration surface.
The Quarkus extension adds build-time code generation, Dev Services, a test module and a migration runner
(`@MorphiumChangeUnit`). Guides: [Quarkus extension](docs/quarkus-extension.md) · [Spring Boot](docs/spring-boot.md).

> 🎬 **See it run:** the [Quarkus Morphium Showcase](https://morphium.kopp-cloud.de/) is a live, interactive demo of
> the repositories, JDQL, pagination, aggregation, caching, change streams, encryption and migrations.

## 🧩 Enums by name or by ordinal

An enum is stored by the name of its constant. Where the order of the constants should count — sorting, range
queries — store the ordinal, per field like `@Enumerated` of JPA or for all fields at once:

```java
public enum Priority { LOW, NORMAL, HIGH }

@Entity
public class Ticket {
    @Id private MorphiumId id;
    @Enumerated(EnumType.ORDINAL) private Priority priority;   // 0, 1, 2 — sorts LOW < NORMAL < HIGH
    private Priority legacy;                                   // by name, unless the setting says otherwise
}

morphium.createQueryFor(Ticket.class).f("priority").gte(Priority.NORMAL).asList();   // NORMAL and HIGH
```

Reading accepts both forms. The setting (`objectMappingSettings().setEnumType(ORDINAL)`, `morphium.enum-type`,
`quarkus.morphium.enum-type`) defaults to `STRING`, so nothing changes unless you ask for it. Existing documents are
not rewritten — see [Enums](docs/configuration-reference.md#enums) before you switch a collection.

## 📨 Messaging

A message queue inside MongoDB — no broker to run, persistent, with priorities, TTL, exclusive delivery and
request/response.

```java
MorphiumMessaging messaging = morphium.createMessaging();
messaging.setSenderId("order-service");
messaging.start();

messaging.addListenerForTopic("orders", (mq, msg) -> {
    process(msg.getValue());
    return null;                                   // or return an answer message
});

Msg msg = new Msg("orders", "new order", "#12345");
msg.setPriority(5);
msg.setTtl(300_000);
messaging.sendMessage(msg);
```

|  | Morphium | Morphium + PoppyDB | Spring Data + RabbitMQ | Kafka |
|---|---|---|---|---|
| Extra infrastructure | none (MongoDB only) | **none** — embedded Java server | RabbitMQ | Kafka |
| Priorities / distributed locks | ✅ / ✅ | ✅ / ✅ | ✅ / ❌ | ❌ / ❌ |
| Request→response (ping-pong)\* | 89 msg/s | **223 msg/s** | — | — |
| One-way throughput\* | ~870–1,250 msg/s | ~770–4,900 msg/s | 10K–50K msg/s | 100K+ msg/s |

<sub>\* Indicative; depends on hardware and workload. Morphium's numbers are
[measured](docs/v5-vs-v6-performance.md), the others quote typical community figures. Morphium acknowledges every
message individually; Kafka's headline figure comes from batching — forced into the same semantics it drops to about
8–10K msg/s. Methodology: [docs/v5-vs-v6-performance.md](docs/v5-vs-v6-performance.md).</sub>

Implementations (`SingleCollectionMessaging`, `MultiCollectionMessaging`, `DualChannelMessaging`) and the JMS bridge:
[messaging guide](docs/messaging.md). All participants of one queue must run the same implementation.

## 🧪 Testing without MongoDB

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.driverSettings().setDriverName("InMemDriver");
Morphium morphium = new Morphium(cfg);          // same API, in process, in milliseconds
```

The in-memory driver covers CRUD, queries, the aggregation pipeline (incl. `$lookup`, `$graphLookup`, `$setWindowFields`),
transactions and change streams. The Morphium test suite itself runs on it, and the Jakarta Data TCK too.
Limits and tips: [in-memory driver guide](docs/howtos/inmemory-driver.md). For tests in other languages or with a real
wire protocol, use [PoppyDB](#-poppydb).

## 🌱 PoppyDB

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="branding/poppydb-logo-dark.svg">
    <img src="branding/poppydb-logo.svg" alt="PoppyDB" width="420">
  </picture>
</p>

PoppyDB is Morphium's sibling: an in-memory server that speaks the MongoDB wire protocol. `mongosh`, Compass, PyMongo
and the official drivers connect to it. It starts in milliseconds, with no Docker and no installation.

- Wire protocol, change streams, aggregation, indexes, transactions
- **Replica sets** with leader election and failover · **SCRAM authentication + TLS** · snapshot persistence
- Optimised for Morphium Messaging — a standing message broker in a single jar

```bash
curl -O https://repo1.maven.org/maven2/de/caluga/poppydb/6.3.14/poppydb-6.3.14-cli.jar
java -jar poppydb-6.3.14-cli.jar --port 27017 --no-config
```

```java
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);   // embedded, for Java tests
server.start();
```

→ [PoppyDB guide](docs/poppydb.md) · [deployment playbook](docs/howtos/poppydb-deployment.md) ·
[migrating from MongoDB](docs/howtos/migration-mongodb-to-poppydb.md)

## 📖 Documentation

| Start here | Go deeper |
|---|---|
| [Documentation hub](docs/index.md) | [Jakarta Data](docs/jakarta-data.md) · [Quarkus](docs/quarkus-extension.md) · [Spring Boot](docs/spring-boot.md) |
| [Overview](docs/overview.md) · [Why Morphium?](docs/why-morphium.md) | [Configuration reference](docs/configuration-reference.md) · [Caching](docs/howtos/cache-patterns.md) · [Aggregation](docs/howtos/aggregation-examples.md) |
| [Quick-start tutorial](docs/quickstart-tutorial.md) | [Optimistic locking](docs/howtos/optimistic-locking.md) · [Update pitfalls](docs/howtos/update-pitfalls.md) · [References](docs/howtos/references-and-relationships.md) |
| [Upgrade 6.2 → 6.3](docs/howtos/migration-v6_2-to-v6_3.md) · [5 → 6](docs/howtos/migration-v5-to-v6.md) | [Security](docs/security-guide.md) · [TLS / X.509](docs/ssl-tls.md) · [Production deployment](docs/production-deployment-guide.md) |

Live monitoring and load testing: [Morpheus](https://github.com/sboesebeck/morpheus), a terminal UI for messages,
topics and node health.

> ⚠️ **On 6.3.x?** Upgrade straight to the latest patch release and roll out **clients first, then servers**; skip
> 6.3.4 (a resume-token loop, [#329](https://github.com/sboesebeck/morphium/issues/329)). Details and the full release
> highlights of 6.x: [what is new in 6.x](docs/releases/whats-new-v6.md) · [CHANGELOG](CHANGELOG.md).

## 🛠️ Build and test

```bash
mvn clean install            # builds all modules and runs the tests (in-memory driver, no MongoDB needed)
./runtests.sh                # the test runner: tags, parallel slots, reruns, statistics
./runtests.sh --tags core,messaging --parallel 8
./runtests.sh --uri mongodb://localhost/testdb --driver pooled     # against a real MongoDB
./runtests.sh --poppydb --driver pooled                            # against PoppyDB
```

Tests run against several backends through `MultiDriverTestBase`; settings come from system properties,
`MORPHIUM_*` environment variables, `morphium-test.properties`, then defaults. More: [test runner](docs/test-runner.md) ·
[developer testing guide](docs/developer-testing-guide.md).

Requirements: Java 21+, MongoDB 5.0+ for production (or PoppyDB), Maven.

## 🤝 Community and contributing

- **Blog:** <https://caluga.de> · **Issues and ideas:** [GitHub](https://github.com/sboesebeck/morphium/issues)
- **Showcase:** [Quarkus Morphium Showcase](https://morphium.kopp-cloud.de/) by Heiko Kopp
  ([Bardioc1977](https://github.com/Bardioc1977))

Pull requests are welcome — especially for the in-memory driver's MongoDB coverage, documentation, benchmarks and tests.
Branch **from `develop`** and open the pull request **against `develop`** (`master` is only updated on release).
Respect the test tags (`@Tag("inmemory")`, `@Tag("poppydb")`), run `./runtests.sh --tags core` before you submit, and
update the documentation when you change an API. Details: [contributing](docs/contributing.md).

## 📜 License

Apache License 2.0 — <https://www.apache.org/licenses/LICENSE-2.0>.

## 🙏 Thanks

To every contributor, and to the MongoDB community for steady feedback. A special thank-you to **Heiko Kopp**
([Bardioc1977](https://github.com/Bardioc1977)) for countless contributions, real-world feedback from large-scale
production use, and the showcase.

---

*Stephan Bösebeck & the Morphium team*
