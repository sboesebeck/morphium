<!--@nrg.languages=en,de-->
<!--@nrg.defaultLanguage=en-->
<!--@nrg.fileNamePattern.de=README.de.md-->

# Morphium 6.2.0

**Feature-rich MongoDB ODM and messaging framework for Java 21+**<!--en-->
**Feature-reiches MongoDB ODM und Messaging-Framework für Java 21+**<!--de-->

Available languages: English and [Deutsch](README.de.md)<!--en-->
Verfügbare Sprachen: [English](README.md) | Deutsch<!--de-->

- 🗄️ **High-performance object mapping** with annotation-driven configuration<!--en-->
Morphium ist eine umfassende Datenschicht-Lösung für MongoDB mit:<!--de-->
- 📨 **Integrated message queue** backed by MongoDB (no extra infrastructure)<!--en-->
- 🗄️ **Leistungsstarkes Object Mapping** mit Annotation-basierter Konfiguration<!--de-->
- ⚡ **Multi-level caching** with cluster-wide invalidation<!--en-->
- 📨 **Integrierte Message Queue** – nutzt MongoDB als Backend (keine zusätzliche Infrastruktur!)<!--de-->
- 🔌 **Custom MongoDB wire-protocol driver** tuned for Morphium<!--en-->
- ⚡ **Multi-Level Caching** mit automatischer Cluster-Synchronisation<!--de-->
- 🧪 **In-memory driver** for fast tests (no MongoDB required)<!--en-->
- 🔌 **Eigener MongoDB Wire-Protocol-Treiber** für direkte Kommunikation<!--de-->
- 🎯 **JMS API (experimental)** for standards-based messaging<!--en-->
- 🧪 **In-Memory-Treiber** für schnelle Tests (deutlich weniger Latenz, kein MongoDB nötig)<!--de-->
- 🚀 **Java 21** with virtual threads for optimal concurrency<!--en-->
- 🎯 **JMS API (experimentell)** für standardbasiertes Messaging<!--de-->
<!--en-->
- 🚀 **JDK 21** mit Virtual Threads für optimale Concurrency<!--de-->
[![Maven Central](https://img.shields.io/maven-central/v/de.caluga/morphium.svg)](https://search.maven.org/artifact/de.caluga/morphium)<!--en-->
<!--de-->
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)<!--en-->
[![Maven Central](https://img.shields.io/maven-central/v/de.caluga/morphium.svg)](https://search.maven.org/artifact/de.caluga/morphium)<!--de-->
<!--en-->
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)<!--de-->
## 🎯 Why Morphium?<!--en-->
<!--de-->
<!--en-->
## 🎯 Warum Morphium?<!--de-->
Morphium is the only Java ODM that ships a message queue living inside MongoDB. If you already run MongoDB, you can power persistence, messaging, caching, and change streams with a single component.<!--en-->
 <!--de-->
<!--en-->
**Einzigartig:** Morphium bietet **verteiltes Messaging ohne zusätzliche Infrastruktur**. Wenn Sie bereits MongoDB nutzen, haben Sie alles was Sie brauchen – kein RabbitMQ, Kafka oder ActiveMQ erforderlich.<!--de-->
| Feature | Morphium | Spring Data + RabbitMQ | Kafka |<!--en-->
<!--de-->
|---------|----------|------------------------|-------|<!--en-->
### Schnellvergleich<!--de-->
| Infrastructure | MongoDB only | MongoDB + RabbitMQ | MongoDB + Kafka |<!--en-->
<!--de-->
| Setup complexity | ⭐ Very low | ⭐⭐⭐ Medium | ⭐⭐⭐⭐⭐ High |<!--en-->
| Feature | Morphium | Spring Data + RabbitMQ | Kafka |<!--de-->
| Message persistence | Built in | Optional | Built in |<!--en-->
|---------|----------|------------------------|-------|<!--de-->
| Message priority | ✅ Yes | ✅ Yes | ❌ No |<!--en-->
| Infrastruktur | Nur MongoDB | MongoDB + RabbitMQ | MongoDB + Kafka |<!--de-->
| Distributed locks | ✅ Yes | ❌ No | ❌ No |<!--en-->
| Setup-Komplexität | ⭐ Sehr niedrig | ⭐⭐⭐ Mittel | ⭐⭐⭐⭐⭐ Hoch |<!--de-->
| Throughput (internal tests) | ~8K msg/s | 10K–50K msg/s | 100K+ msg/s |<!--en-->
| Nachrichten persistent | Standard | Optional | Standard |<!--de-->
| Operations | ⭐ Very easy | ⭐⭐ Medium | ⭐⭐⭐⭐ Complex |<!--en-->
| Nachrichtenpriorität | ✅ Ja | ✅ Ja | ❌ Nein |<!--de-->
<!--en-->
| Distributed Locks | ✅ Ja | ❌ Nein | ❌ Nein |<!--de-->
_* Numbers are indicative and depend heavily on hardware and workload._<!--en-->
| Durchsatz (interne Tests) | ~8K msg/s | 10K–50K msg/s | 100K+ msg/s |<!--de-->
<!--en-->
| Betrieb | ⭐ Sehr einfach | ⭐⭐ Mittel | ⭐⭐⭐⭐ Komplex |<!--de-->
## 📚 Documentation<!--en-->
<!--de-->
<!--en-->
_* Richtwerte aus internen Messungen; tatsächliche Werte hängen von Hardware und Workload ab._<!--de-->
### Quick access<!--en-->
<!--de-->
- **[Documentation hub](docs/index.md)** – entry point for all guides<!--en-->
## 📚 Dokumentation<!--de-->
- **[Overview](docs/overview.md)** – core concepts, quick start, compatibility<!--en-->
<!--de-->
- **[Upgrade v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md)** – migration checklist for 6.2.0<!--en-->
### Schnellzugriff<!--de-->
- **[Migration v5→v6](docs/howtos/migration-v5-to-v6.md)** – step-by-step upgrade guide<!--en-->
- **[Dokumentenportal](docs/index.md)** – Einstieg in sämtliche Guides<!--de-->
- **[InMemory Driver Guide](docs/howtos/inmemory-driver.md)** – capabilities, caveats, testing tips<!--en-->
- **[Überblick](docs/overview.md)** – Kernkonzepte, Quickstart, Kompatibilität<!--de-->
- **[Optimistic Locking (`@Version`)](docs/howtos/optimistic-locking.md)** – prevent lost updates with `@Version`<!--en-->
- **[Upgrade v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md)** – Migrationsleitfaden für 6.2.0<!--de-->
- **[SSL/TLS & MONGODB-X509](docs/ssl-tls.md)** – encrypted connections and certificate-based authentication<!--en-->
- **[Migration v5→v6](docs/howtos/migration-v5-to-v6.md)** – Schritt-für-Schritt-Anleitung<!--de-->
<!--en-->
- **[InMemory Driver Guide](docs/howtos/inmemory-driver.md)** – Features, Einschränkungen, Tests<!--de-->
### More resources<!--en-->
<!--de-->
- Aggregation examples: `docs/howtos/aggregation-examples.md`<!--en-->
### Weitere Ressourcen<!--de-->
- Messaging implementations: `docs/howtos/messaging-implementations.md`<!--en-->
- Aggregationsbeispiele: `docs/howtos/aggregation-examples.md`<!--de-->
- Performance guide: `docs/performance-scalability-guide.md`<!--en-->
- Messaging-Implementierungen: `docs/howtos/messaging-implementations.md`<!--de-->
- Production deployment: `docs/production-deployment-guide.md`<!--en-->
- Performance-Guide: `docs/performance-scalability-guide.md`<!--de-->
- Monitoring & troubleshooting: `docs/monitoring-metrics-guide.md`<!--en-->
- Production-Deployment: `docs/production-deployment-guide.md`<!--de-->
<!--en-->
- Monitoring & Troubleshooting: `docs/monitoring-metrics-guide.md`<!--de-->
## 🚀 What’s New in v6.2<!--en-->
<!--de-->
<!--en-->
## 🚀 Neu in Version 6.2<!--de-->
### Multi-Module Maven Build<!--en-->
<!--de-->
Morphium is now a multi-module project: `morphium-parent` (BOM), `morphium` (core library), and `poppydb` (server). The core library `de.caluga:morphium` no longer drags in server dependencies (Netty, etc.) — 90% leaner for users who just need the ODM.<!--en-->
### Multi-Module Maven Build<!--de-->
<!--en-->
Morphium ist jetzt ein Multi-Module-Projekt: `morphium-parent` (BOM), `morphium` (Core-Bibliothek) und `poppydb` (Server). Die Core-Bibliothek `de.caluga:morphium` enthält keine Server-Abhängigkeiten (Netty etc.) mehr — ca. 90% schlanker für Nutzer, die nur das ODM benötigen.<!--de-->
### PoppyDB – Standalone MongoDB-Compatible Server<!--en-->
<!--de-->
The former MorphiumServer is now an independent module `de.caluga:poppydb`. It implements the MongoDB Wire Protocol as an in-memory server with Replica Set emulation, Change Streams, Aggregation Pipeline, and snapshot-based persistence.<!--en-->
### PoppyDB – Standalone MongoDB-kompatibler Server<!--de-->
<!--en-->
Der ehemalige MorphiumServer ist jetzt ein eigenständiges Modul `de.caluga:poppydb`. Er implementiert das MongoDB Wire Protocol als In-Memory-Server mit Replica-Set-Emulation, Change Streams, Aggregation Pipeline und Snapshot-basierter Persistenz.<!--de-->
PoppyDB and Morphium Messaging are **optimized for each other** — both sides recognize the counterpart and adapt their behavior, resulting in lower latency and less overhead than with a real MongoDB as messaging backend.<!--en-->
<!--de-->
<!--en-->
PoppyDB und Morphium Messaging sind **aufeinander optimiert** — beide Seiten erkennen das Gegenüber und passen ihr Verhalten an. Das Ergebnis: niedrigere Latenz und weniger Overhead als mit einer echten MongoDB als Messaging-Backend.<!--de-->
```xml<!--en-->
<!--de-->
<dependency><!--en-->
```xml<!--de-->
    <groupId>de.caluga</groupId><!--en-->
<dependency><!--de-->
    <artifactId>poppydb</artifactId><!--en-->
    <groupId>de.caluga</groupId><!--de-->
    <version>6.2.0</version><!--en-->
    <artifactId>poppydb</artifactId><!--de-->
    <scope>test</scope> <!-- or remove scope for production use --><!--en-->
    <version>6.2.0</version><!--de-->
</dependency><!--en-->
    <scope>test</scope> <!-- oder scope entfernen für produktiven Einsatz --><!--de-->
```<!--en-->
</dependency><!--de-->
<!--en-->
```<!--de-->
- ✅ **Full Wire Protocol**: Any MongoDB client can connect (mongosh, Compass, PyMongo, ...)<!--en-->
<!--de-->
- ✅ **Messaging Backend**: Run Morphium messaging without MongoDB — optimized for low-latency<!--en-->
- ✅ **Volle Wire-Protocol-Unterstützung**: Jeder MongoDB-Client kann sich verbinden (mongosh, Compass, PyMongo, ...)<!--de-->
- ✅ **CLI Tooling**: `poppydb-6.2.0-cli.jar` for standalone deployment<!--en-->
- ✅ **Messaging-Backend**: Morphium-Messaging ohne MongoDB betreiben — optimiert für niedrige Latenz<!--de-->
- ✅ **Replica Set Emulation**: Test cluster behavior without real MongoDB<!--en-->
- ✅ **CLI-Tooling**: `poppydb-6.2.0-cli.jar` für Standalone-Deployment<!--de-->
- ✅ **Snapshot Persistence**: `--dump-dir` / `--dump-interval` to preserve data across restarts<!--en-->
- ✅ **Replica-Set-Emulation**: Cluster-Verhalten testen ohne echtes MongoDB<!--de-->
<!--en-->
- ✅ **Snapshot-Persistenz**: `--dump-dir` / `--dump-interval` zum Sichern der Daten über Neustarts<!--de-->
### MorphiumDriverException is now unchecked<!--en-->
<!--de-->
`MorphiumDriverException` extends `RuntimeException` — consistent with the MongoDB Java driver. Eliminates 40+ boilerplate `catch-wrap-rethrow` blocks.<!--en-->
### MorphiumDriverException ist jetzt unchecked<!--de-->
<!--en-->
`MorphiumDriverException` erbt von `RuntimeException` — konsistent mit dem MongoDB Java Driver. Eliminiert 40+ Boilerplate `catch-wrap-rethrow`-Blöcke.<!--de-->
### @Reference Cascade Delete/Store<!--en-->
<!--de-->
`@Reference` now supports `cascadeDelete` and `cascadeStore` for automatic lifecycle management of referenced entities.<!--en-->
### @Reference Cascade Delete/Store<!--de-->
<!--en-->
`@Reference` unterstützt jetzt `cascadeDelete` und `cascadeStore` für automatisches Lifecycle-Management referenzierter Entities.<!--de-->
### @AutoSequence<!--en-->
<!--de-->
Annotation-driven auto-increment sequences — no manual counter management needed.<!--en-->
### @AutoSequence<!--de-->
<!--en-->
Annotations-basierte Auto-Increment-Sequenzen — kein manuelles Counter-Management mehr nötig.<!--de-->
### @CreationTime Improvements<!--en-->
<!--de-->
Works correctly with `store()` and `storeList()`, supports `@CreationTime` on `Date`, `long`, and `String` fields.<!--en-->
### @CreationTime Verbesserungen<!--de-->
<!--en-->
Funktioniert korrekt mit `store()` und `storeList()`, unterstützt `@CreationTime` auf `Date`-, `long`- und `String`-Feldern. Field-Level-Annotation genügt, Class-Level ist nicht mehr erforderlich.<!--de-->
### CosmosDB Auto-Detection<!--en-->
<!--de-->
Morphium detects Azure CosmosDB connections and automatically adjusts behavior for compatibility.<!--en-->
### CosmosDB Auto-Erkennung<!--de-->
<!--en-->
Morphium erkennt Azure CosmosDB-Verbindungen und passt sein Verhalten automatisch an.<!--de-->
See [CHANGELOG](CHANGELOG.md) for full details.<!--en-->
<!--de-->
<!--en-->
Siehe [CHANGELOG](CHANGELOG.md) für alle Details.<!--de-->
## Upgrading from 6.1.x to 6.2.0<!--en-->
<!--de-->
<!--en-->
## Upgrade von 6.1.x auf 6.2.0<!--de-->
### Breaking: MorphiumDriverException is now unchecked<!--en-->
<!--de-->
<!--en-->
Die wichtigsten Änderungen beim Upgrade:<!--de-->
`MorphiumDriverException` extends `RuntimeException` instead of `Exception`. This eliminates boilerplate `catch-wrap-rethrow` blocks but requires attention in existing code:<!--en-->
<!--de-->
<!--en-->
### Breaking: MorphiumDriverException ist jetzt unchecked<!--de-->
```java<!--en-->
<!--de-->
// Multi-catch — simplify (MorphiumDriverException IS a RuntimeException now)<!--en-->
```java<!--de-->
// Before:<!--en-->
// Multi-catch vereinfachen (MorphiumDriverException IST jetzt ein RuntimeException)<!--de-->
catch (RuntimeException | MorphiumDriverException e) { ... }<!--en-->
// Vorher:<!--de-->
// After:<!--en-->
catch (RuntimeException | MorphiumDriverException e) { ... }<!--de-->
catch (RuntimeException e) { ... }<!--en-->
// Nachher:<!--de-->
<!--en-->
catch (RuntimeException e) { ... }<!--de-->
// throws declarations — can be removed (but still compile if left in)<!--en-->
<!--de-->
// Before:<!--en-->
// throws-Deklaration kann entfernt werden (kompiliert aber weiterhin)<!--de-->
public void doStuff() throws MorphiumDriverException { ... }<!--en-->
// Vorher:<!--de-->
// After:<!--en-->
public void doStuff() throws MorphiumDriverException { ... }<!--de-->
public void doStuff() { ... }<!--en-->
// Nachher:<!--de-->
<!--en-->
public void doStuff() { ... }<!--de-->
// Standalone catch — works unchanged<!--en-->
```<!--de-->
catch (MorphiumDriverException e) { ... }  // still compiles<!--en-->
<!--de-->
```<!--en-->
### Breaking: MorphiumServer → PoppyDB<!--de-->

### Breaking: MorphiumServer → PoppyDB<!--en-->
| | 6.1.x | 6.2.0 |<!--de-->
<!--en-->
|---|---|---|<!--de-->
The embedded MongoDB-compatible server was extracted to its own module and renamed:<!--en-->
| Maven-Artifact | in `morphium` enthalten | separat: `de.caluga:poppydb:6.2.0` |<!--de-->
<!--en-->
| Package | `de.caluga.morphium.server` | `de.caluga.poppydb` |<!--de-->
| | 6.1.x | 6.2.0 |<!--en-->
| Hauptklasse | `MorphiumServer` | `PoppyDB` |<!--de-->
|---|---|---|<!--en-->
| CLI-JAR | `morphium-*-server-cli.jar` | `poppydb-*-cli.jar` |<!--de-->
| Maven artifact | included in `morphium` | separate: `de.caluga:poppydb:6.2.0` |<!--en-->
| Test-Tag | `@Tag("morphiumserver")` | `@Tag("poppydb")` |<!--de-->
| Package | `de.caluga.morphium.server` | `de.caluga.poppydb` |<!--en-->
<!--de-->
| Main class | `MorphiumServer` | `PoppyDB` |<!--en-->
Wire-Protokoll-Kompatibilität ist gewahrt — PoppyDB antwortet im Hello-Handshake sowohl auf `poppyDB` als auch `morphiumServer`.<!--de-->
| CLI JAR | `morphium-*-server-cli.jar` | `poppydb-*-cli.jar` |<!--en-->
<!--de-->
| Test tag | `@Tag("morphiumserver")` | `@Tag("poppydb")` |<!--en-->
### Migrations-Checkliste<!--de-->

If you use PoppyDB in tests, add the dependency:<!--en-->
1. **`catch (RuntimeException | MorphiumDriverException`** suchen → zu `catch (RuntimeException` vereinfachen<!--de-->
```xml<!--en-->
2. **`import de.caluga.morphium.server`** suchen → durch `import de.caluga.poppydb` ersetzen<!--de-->
<dependency><!--en-->
3. **`MorphiumServer`** suchen → in `PoppyDB` umbenennen<!--de-->
    <groupId>de.caluga</groupId><!--en-->
4. **`@Tag("morphiumserver")`** suchen → in `@Tag("poppydb")` umbenennen<!--de-->
    <artifactId>poppydb</artifactId><!--en-->
5. **`poppydb`-Dependency** hinzufügen falls der Embedded Server genutzt wird<!--de-->
    <version>6.2.0</version><!--en-->
6. **CLI-Skripte** aktualisieren — JAR-Name ist jetzt `poppydb-*-cli.jar`<!--de-->
    <scope>test</scope><!--en-->
<!--de-->
</dependency><!--en-->
Detaillierte Anleitung: **[Migration v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md)**<!--de-->
```<!--en-->
<!--de-->
<!--en-->
## 🚀 Neu in Version 6.0<!--de-->
Wire-protocol compatibility is preserved — PoppyDB responds to both `poppyDB` and `morphiumServer` in the hello handshake.<!--en-->
<!--de-->
<!--en-->
### JDK 21 & Moderne Java-Features<!--de-->
### Deprecated: Direct config setters → sub-objects<!--en-->
- **Virtual Threads**: Messaging-System optimiert für Project Loom<!--de-->
<!--en-->
- **Pattern Matching**: Verbesserte Code-Klarheit und Typ-Sicherheit<!--de-->
`MorphiumConfig` now organizes settings into typed sub-objects. The old setters still work but are `@Deprecated`:<!--en-->
- **Records**: Noch nicht als `@Entity` oder `@Embedded` unterstützt (siehe [#116](https://github.com/sboesebeck/morphium/issues/116))<!--de-->
<!--en-->
- **Sealed Classes**: Bessere Typ-Hierarchien in Domain-Models<!--de-->
```java<!--en-->
<!--de-->
// 6.1.x style (deprecated but functional)<!--en-->
### Treiber & Konnektivität<!--de-->
cfg.setDatabase("mydb");<!--en-->
- **SSL/TLS-Unterstützung**: Sichere Verbindungen zu MongoDB-Instanzen (seit v6.0)<!--de-->
cfg.addHostToSeed("localhost", 27017);<!--en-->
- **Virtual Threads** im Treiber für optimale Performance<!--de-->

// 6.2.0 style (preferred)<!--en-->
### Verbessertes Messaging-System<!--de-->
cfg.connectionSettings().setDatabase("mydb");<!--en-->
- **Weniger Duplikate**: Optimierte Message-Processing-Logik<!--de-->
cfg.clusterSettings().addHostToSeed("localhost", 27017);<!--en-->
- **Virtual Thread Integration**: Bessere Concurrency-Performance<!--de-->
cfg.driverSettings().setDriverName("PooledDriver");<!--en-->
- **Höherer Durchsatz**: Interne Benchmarks zeigen deutliche Steigerungen<!--de-->
```<!--en-->
- **Distributed Locking**: Verbesserte Multi-Instance-Koordination<!--de-->

Available sub-objects: `connectionSettings()`, `clusterSettings()`, `driverSettings()`, `messagingSettings()`, `cacheSettings()`, `authSettings()`, `threadPoolSettings()`, `objectMappingSettings()`, `writerSettings()`.<!--en-->
### In-Memory-Treiber für Testing<!--de-->
<!--en-->
- **Keine MongoDB benötigt**: Komplette Test-Suite ohne externe Abhängigkeiten<!--de-->
### New: Multi-Module Maven Structure<!--en-->
- **Deutlich schnellere Tests**: Profitieren von reinem In-Memory-Zugriff<!--de-->
<!--en-->
- **CI/CD-freundlich**: Perfekt für Continuous Integration Pipelines<!--de-->
The `morphium` core artifact no longer bundles server dependencies (Netty, etc.). If you only use Morphium as ODM, your dependency tree is ~90% leaner — no changes to your pom needed.<!--en-->
- **Breite Feature-Unterstützung**: Viele MongoDB-Operationen, mit dokumentierten Ausnahmen<!--de-->

### Migration checklist<!--en-->
### Umfassende Dokumentation<!--de-->
<!--en-->
- Komplette Neuschreibung aller Guides<!--de-->
1. **Search for `catch (RuntimeException | MorphiumDriverException`** — simplify to `catch (RuntimeException`<!--en-->
- Praxis-Beispiele und Use Cases<!--de-->
2. **Search for `import de.caluga.morphium.server`** — replace with `import de.caluga.poppydb`<!--en-->
- Migration-Guide von 5.x<!--de-->
3. **Search for `MorphiumServer`** — rename to `PoppyDB`<!--en-->
- Architektur-Details und Best Practices<!--de-->
4. **Search for `@Tag("morphiumserver")`** — rename to `@Tag("poppydb")`<!--en-->
<!--de-->
5. **Add `poppydb` dependency** if you use the embedded server in tests<!--en-->
## Anforderungen & Abhängigkeiten<!--de-->
6. **Optional:** migrate direct config setters to sub-object style<!--en-->
- Java 21 oder neuer<!--de-->
7. **Optional:** adopt new features (`@Reference(cascadeDelete)`, `@AutoSequence`, `@Version`)<!--en-->
- MongoDB 5.0+ für produktive Deployments<!--de-->
<!--en-->
- Maven<!--de-->
## 🚀 What’s New in v6.1.x<!--en-->
<!--de-->
<!--en-->
Maven-Abhängigkeiten:<!--de-->
### MONGODB-X509 Client-Certificate Authentication<!--en-->
```xml<!--de-->
- Connect to MongoDB instances that require mutual TLS / x.509 client certificates<!--en-->
<dependency><!--de-->
- Configure via `AuthSettings.setAuthMechanism("MONGODB-X509")` together with the existing `SslHelper` mTLS setup<!--en-->
  <groupId>de.caluga</groupId><!--de-->
<!--en-->
  <artifactId>morphium</artifactId><!--de-->
### `@Version` – Optimistic Locking<!--en-->
  <version>[6.2.0,)</version><!--de-->
Prevents lost updates in concurrent environments without requiring pessimistic database locks. See `docs/howtos/optimistic-locking.md` for the full guide.<!--en-->
</dependency><!--de-->
<!--en-->
<dependency><!--de-->
## 🚀 What’s New in v6.0<!--en-->
  <groupId>org.mongodb</groupId><!--de-->
<!--en-->
  <artifactId>bson</artifactId><!--de-->
### Java 21 & Modern Language Features<!--en-->
  <version>4.7.1</version><!--de-->
- **Virtual threads** for high-throughput messaging and change streams<!--en-->
</dependency><!--de-->
- **Pattern matching** across driver and mapping layers<!--en-->
```<!--de-->
- **Records**: Not yet supported as `@Entity` or `@Embedded` types (see [#116](https://github.com/sboesebeck/morphium/issues/116))<!--en-->
<!--de-->
- **Sealed class support** for cleaner domain models<!--en-->
Migration von v5? → `docs/howtos/migration-v5-to-v6.md`<!--de-->
<!--en-->
Upgrade von v6.1? → `docs/howtos/migration-v6_1-to-v6_2.md`<!--de-->
### Driver & Connectivity<!--en-->
<!--de-->
- **SSL/TLS Support**: Secure connections to MongoDB instances (added in v6.0)<!--en-->
## ⚡ Quick Start<!--de-->
- **Virtual threads** in the driver for optimal concurrency<!--en-->
<!--de-->
<!--en-->
### Maven Dependency<!--de-->
### Messaging Improvements<!--en-->
<!--de-->
- **Fewer duplicates** thanks to refined message processing<!--en-->
```xml<!--de-->
- **Virtual-thread integration** for smoother concurrency<!--en-->
<dependency><!--de-->
- **Higher throughput** confirmed in internal benchmarking<!--en-->
  <groupId>de.caluga</groupId><!--de-->
- **Distributed locking** for coordinated multi-instance deployments<!--en-->
  <artifactId>morphium</artifactId><!--de-->
<!--en-->
  <version>6.2.0</version><!--de-->
### In-Memory Driver Enhancements<!--en-->
</dependency><!--de-->
- **No MongoDB required** for unit tests or CI pipelines<!--en-->
```<!--de-->
- **Significantly faster test cycles** in pure in-memory mode<!--en-->
<!--de-->
- **~93% MongoDB feature coverage** including advanced operations<!--en-->
### Einfaches Beispiel - Object Mapping<!--de-->
- **Full aggregation pipeline** with `$lookup`, `$graphLookup`, `$bucket`, `$mergeObjects`<!--en-->
<!--de-->
- **MapReduce support** with JavaScript engine integration<!--en-->
```java<!--de-->
- **Array operators** including `$pop`, `$push`, `$pull`, `$addToSet`<!--en-->
import de.caluga.morphium.Morphium;<!--de-->
- **Change streams & transactions** available for integration testing<!--en-->
import de.caluga.morphium.MorphiumConfig;<!--de-->
- **Drop-in replacement** for most development and testing scenarios<!--en-->
import de.caluga.morphium.annotations.*;<!--de-->
<!--en-->
import de.caluga.morphium.driver.MorphiumId;<!--de-->
### Documentation Overhaul<!--en-->
import java.time.LocalDateTime;<!--de-->
- Complete rewrite of the guide set<!--en-->
import java.util.List;<!--de-->
- Practical examples and end-to-end use cases<!--en-->
<!--de-->
- Dedicated migration playbook from 5.x to 6.x<!--en-->
// Entity definieren<!--de-->
- Architecture insights and best practices<!--en-->
@Entity<!--de-->
<!--en-->
public class User {<!--de-->
## ✅ Requirements<!--en-->
    @Id<!--de-->
- Java 21 or newer<!--en-->
    private MorphiumId id;<!--de-->
- MongoDB 5.0+ for production deployments<!--en-->
    private String name;<!--de-->
- Maven<!--en-->
    private String email;<!--de-->
<!--en-->
    private LocalDateTime createdAt;<!--de-->
Maven dependencies:<!--en-->
    // getters/setters<!--de-->
```xml<!--en-->
}<!--de-->
<dependency><!--en-->
<!--de-->
  <groupId>de.caluga</groupId><!--en-->
// Konfiguration<!--de-->
  <artifactId>morphium</artifactId><!--en-->
MorphiumConfig cfg = new MorphiumConfig();<!--de-->
  <version>[6.2.0,)</version><!--en-->
cfg.connectionSettings().setDatabase("myapp");<!--de-->
</dependency><!--en-->
cfg.clusterSettings().addHostToSeed("localhost", 27017);<!--de-->
<dependency><!--en-->
cfg.driverSettings().setDriverName("PooledDriver");<!--de-->
  <groupId>org.mongodb</groupId><!--en-->
<!--de-->
  <artifactId>bson</artifactId><!--en-->
Morphium morphium = new Morphium(cfg);<!--de-->
  <version>4.7.1</version><!--en-->
<!--de-->
</dependency><!--en-->
// Entity speichern<!--de-->
```<!--en-->
User user = new User();<!--de-->
<!--en-->
user.setName("John Doe");<!--de-->
Migrating from v5? → `docs/howtos/migration-v5-to-v6.md`<!--en-->
user.setEmail("john@example.com");<!--de-->
<!--en-->
user.setCreatedAt(LocalDateTime.now());<!--de-->
## ⚡ Quick Start<!--en-->
morphium.store(user);<!--de-->

### Maven dependency<!--en-->
// Abfragen<!--de-->
<!--en-->
List<User> users = morphium.createQueryFor(User.class)<!--de-->
```xml<!--en-->
    .f("email").matches(".*@example.com")<!--de-->
<dependency><!--en-->
    .sort("createdAt")<!--de-->
  <groupId>de.caluga</groupId><!--en-->
    .asList();<!--de-->
  <artifactId>morphium</artifactId><!--en-->
```<!--de-->
  <version>6.2.0</version><!--en-->
<!--de-->
</dependency><!--en-->
### Messaging-Beispiel<!--de-->
```<!--en-->
<!--de-->
<!--en-->
```java<!--de-->
### Object mapping example<!--en-->
import de.caluga.morphium.messaging.MorphiumMessaging;<!--de-->
<!--en-->
import de.caluga.morphium.messaging.Msg;<!--de-->
```java<!--en-->
<!--de-->
import de.caluga.morphium.Morphium;<!--en-->
// Messaging Setup<!--de-->
import de.caluga.morphium.MorphiumConfig;<!--en-->
MorphiumMessaging messaging = morphium.createMessaging();<!--de-->
import de.caluga.morphium.annotations.*;<!--en-->
messaging.setSenderId("my-app");<!--de-->
import de.caluga.morphium.driver.MorphiumId;<!--en-->
messaging.start();<!--de-->
import java.time.LocalDateTime;<!--en-->
<!--de-->
import java.util.List;<!--en-->
// Nachricht senden<!--de-->
<!--en-->
Msg message = new Msg("orderQueue", "Process Order", "Order #12345");<!--de-->
// Entity definition<!--en-->
message.setPriority(5);<!--de-->
@Entity<!--en-->
message.setTtl(300000); // 5 Minuten<!--de-->
public class User {<!--en-->
messaging.sendMessage(message);<!--de-->
    @Id<!--en-->
<!--de-->
    private MorphiumId id;<!--en-->
// Nachrichten empfangen<!--de-->
    private String name;<!--en-->
messaging.addListenerForTopic("orderQueue", (m, msg) -> {<!--de-->
    private String email;<!--en-->
    // Order verarbeiten...<!--de-->
    private LocalDateTime createdAt;<!--en-->
    return null; // keine Antwort senden<!--de-->
    // getters/setters<!--en-->
});<!--de-->
}<!--en-->
```<!--de-->

// Configuration<!--en-->
### Konfiguration über Properties/Environment<!--de-->
MorphiumConfig cfg = new MorphiumConfig();<!--en-->
<!--de-->
cfg.connectionSettings().setDatabase("myapp");<!--en-->
```bash<!--de-->
cfg.clusterSettings().addHostToSeed("localhost", 27017);<!--en-->
# Environment Variables<!--de-->
cfg.driverSettings().setDriverName("PooledDriver");<!--en-->
export MONGODB_URI='mongodb://user:pass@localhost:27017/app?replicaSet=rs0'<!--de-->
<!--en-->
export MORPHIUM_DRIVER=inmem<!--de-->
Morphium morphium = new Morphium(cfg);<!--en-->
<!--de-->
<!--en-->
# System Properties<!--de-->
// Store entity<!--en-->
mvn -Dmorphium.uri='mongodb://localhost/mydb' test<!--de-->
User user = new User();<!--en-->
<!--de-->
user.setName("John Doe");<!--en-->
# Properties-Datei (morphium.properties)<!--de-->
user.setEmail("john@example.com");<!--en-->
morphium.hosts=mongo1.example.com:27017,mongo2.example.com:27017<!--de-->
user.setCreatedAt(LocalDateTime.now());<!--en-->
morphium.database=myapp<!--de-->
morphium.store(user);<!--en-->
morphium.replicaSet=myReplicaSet<!--de-->
<!--en-->
```<!--de-->
// Query<!--en-->
<!--de-->
List<User> users = morphium.createQueryFor(User.class)<!--en-->
## 🧪 Tests & Test-Runner<!--de-->
    .f("email").matches(".*@example.com")<!--en-->
<!--de-->
    .sort("createdAt")<!--en-->
### Maven-Tests<!--de-->
    .asList();<!--en-->
```bash<!--de-->
```<!--en-->
# Alle Tests<!--de-->
<!--en-->
mvn test<!--de-->
### Messaging example<!--en-->
<!--de-->
<!--en-->
# Vollständiger Build mit Checks<!--de-->
```java<!--en-->
mvn clean verify<!--de-->
import de.caluga.morphium.messaging.MorphiumMessaging;<!--en-->
<!--de-->
import de.caluga.morphium.messaging.Msg;<!--en-->
# Nur Core-Tests (schnell)<!--de-->
<!--en-->
mvn test -Dgroups="core,messaging"<!--de-->
// Messaging setup<!--en-->
<!--de-->
MorphiumMessaging messaging = morphium.createMessaging();<!--en-->
# Tests mit echtem MongoDB<!--de-->
messaging.setSenderId("my-app");<!--en-->
mvn test -Dmorphium.driver=pooled -Dmorphium.uri=mongodb://localhost/testdb<!--de-->
messaging.start();<!--en-->
```<!--de-->

// Send a message<!--en-->
### Test-Runner Script (`./runtests.sh`)<!--de-->
Msg message = new Msg("orderQueue", "Process Order", "Order #12345");<!--en-->
Umfassender Test-Runner mit farbiger Ausgabe, paralleler Ausführung und automatischen Wiederholungen.<!--de-->
message.setPriority(5);<!--en-->
<!--de-->
message.setTtl(300000); // 5 minutes<!--en-->
```bash<!--de-->
messaging.sendMessage(message);<!--en-->
# Alle Tests mit InMemory-Treiber (Standard)<!--de-->
<!--en-->
./runtests.sh<!--de-->
// Receive messages<!--en-->
<!--de-->
messaging.addListenerForTopic("orderQueue", (m, msg) -> {<!--en-->
# Nur Core-Tests<!--de-->
    // process order ...<!--en-->
./runtests.sh --tags core,messaging<!--de-->
    return null; // no reply<!--en-->
<!--de-->
});<!--en-->
# Parallele Ausführung (8 Slots = 8x schneller!)<!--de-->
```<!--en-->
./runtests.sh --parallel 8 --tags core<!--de-->

### Properties & environment configuration<!--en-->
# Nur fehlgeschlagene Tests wiederholen (NEU in 6.0!)<!--de-->
<!--en-->
./runtests.sh --rerunfailed<!--de-->
```bash<!--en-->
./runtests.sh --rerunfailed --retry 3<!--de-->
# Environment variables<!--en-->
<!--de-->
export MONGODB_URI='mongodb://user:pass@localhost:27017/app?replicaSet=rs0'<!--en-->
# Tests gegen echten MongoDB-Cluster<!--de-->
export MORPHIUM_DRIVER=inmem<!--en-->
./runtests.sh --driver pooled --uri mongodb://mongo1,mongo2/testdb<!--de-->

# System properties<!--en-->
# Spezifische Test-Klasse<!--de-->
mvn -Dmorphium.uri='mongodb://localhost/mydb' test<!--en-->
./runtests.sh CacheTests<!--de-->

# Properties file (morphium.properties)<!--en-->
# Statistiken anzeigen<!--de-->
morphium.hosts=mongo1.example.com:27017,mongo2.example.com:27017<!--en-->
./runtests.sh --stats<!--de-->
morphium.database=myapp<!--en-->
./getFailedTests.sh  # Liste der fehlgeschlagenen Methoden<!--de-->
morphium.replicaSet=myReplicaSet<!--en-->
```<!--de-->
```<!--en-->
<!--de-->
<!--en-->
**Neue Features in v6.0:**<!--de-->
## 🧪 Tests & Test Runner<!--en-->
- ✅ **Method-Level Rerun**: `--rerunfailed` führt nur fehlgeschlagene Methoden aus (nicht ganze Klassen)<!--de-->
<!--en-->
- ✅ **Kein Hängen mehr**: Alle bekannten Hänge-Probleme behoben<!--de-->
### Maven<!--en-->
- ✅ **Schnellere Iteration**: Spürbar schneller bei partiellen Wiederholungen<!--de-->
```bash<!--en-->
- ✅ **Bessere Filterung**: Klassenname-Filter funktionieren zuverlässig<!--de-->
# All tests<!--en-->
<!--de-->
mvn test<!--en-->
Weitere Optionen zeigt `./runtests.sh --help`.<!--de-->

# Full build with checks<!--en-->
### Test-Konfiguration<!--de-->
mvn clean verify<!--en-->
<!--de-->
<!--en-->
`TestConfig` konsolidiert alle Test-Einstellungen. Priorität der Quellen:<!--de-->
# Tagged test selection<!--en-->
1. System Properties (`-Dmorphium.*`)<!--de-->
mvn test -Dgroups="core,messaging"<!--en-->
2. Environment Variables (`MORPHIUM_*`, `MONGODB_URI`)<!--de-->
<!--en-->
3. `src/test/resources/morphium-test.properties`<!--de-->
# Run against a real MongoDB instance<!--en-->
4. Defaults (localhost:27017)<!--de-->
mvn test -Dmorphium.driver=pooled -Dmorphium.uri=mongodb://localhost/testdb<!--en-->
<!--de-->
```<!--en-->
## 🔧 PoppyDB & InMemoryDriver<!--de-->

### `./runtests.sh` helper<!--en-->
### InMemoryDriver - Testing ohne MongoDB<!--de-->
```bash<!--en-->
<!--de-->
# Default: in-memory driver (fast, no MongoDB required)<!--en-->
Der InMemoryDriver bietet eine weitgehend kompatible MongoDB-Simulation im Speicher:<!--de-->
./runtests.sh<!--en-->
<!--de-->
<!--en-->
**Features:**<!--de-->
# Run tagged suites<!--en-->
- ✅ Alle CRUD-Operationen<!--de-->
./runtests.sh --tags core,messaging<!--en-->
- ✅ Komplexe Queries mit breiter Operator-Unterstützung<!--de-->
<!--en-->
- ✅ Aggregation-Pipelines (z. B. `$match`, `$group`, `$project`)<!--de-->
# Parallel runs<!--en-->
- ✅ Transaktionen (single-instance)<!--de-->
./runtests.sh --parallel 8 --tags core<!--en-->
- ✅ Change Streams (Basis-Implementation)<!--de-->
<!--en-->
- ✅ JavaScript `$where`-Operator<!--de-->
# Retry only failed methods<!--en-->
<!--de-->
./runtests.sh --rerunfailed<!--en-->
**Performance:**<!--de-->
./runtests.sh --rerunfailed --retry 3<!--en-->
- Spürbar schneller als Tests gegen einen externen MongoDB-Server<!--de-->
<!--en-->
- Keine Netzwerk-Latenz<!--de-->
# Single test class<!--en-->
- Keine Disk I/O<!--de-->
./runtests.sh CacheTests<!--en-->
- Perfekt für CI/CD-Pipelines<!--de-->

# Statistics<!--en-->
**Verwendung:**<!--de-->
./runtests.sh --stats<!--en-->
```bash<!--de-->
./getFailedTests.sh  # list failed methods<!--en-->
# Alle Tests mit InMemory<!--de-->
```<!--en-->
./runtests.sh --driver inmem<!--de-->

Run `./runtests.sh --help` to see every option.<!--en-->
# Spezifische Tests<!--de-->
<!--en-->
mvn test -Dmorphium.driver=inmem -Dtest="CacheTests"<!--de-->
### Multi-Backend Testing<!--en-->
```<!--de-->

Tests are parameterized to run against multiple drivers. Use `--driver` to select:<!--en-->
### PoppyDB - Standalone MongoDB-Ersatz<!--de-->

```bash<!--en-->
PoppyDB (ehemals MorphiumServer) ist ein eigenständiger Prozess, der das MongoDB Wire Protocol implementiert:<!--de-->
# InMemory only (fastest, default)<!--en-->
<!--de-->
./runtests.sh --driver inmem<!--en-->
```bash<!--de-->
<!--en-->
# Server starten<!--de-->
# Against external MongoDB with all drivers (pooled + single + inmem)<!--en-->
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar<!--de-->
./runtests.sh --uri mongodb://mongo1,mongo2/testdb --driver all<!--en-->
<!--de-->
<!--en-->
# Clients verbinden (z.B. MongoDB Compass, mongosh)<!--de-->
# Against external MongoDB with pooled driver only<!--en-->
mongosh mongodb://localhost:27017<!--de-->
./runtests.sh --uri mongodb://mongo1,mongo2/testdb --driver pooled<!--en-->
<!--de-->
<!--en-->
# Start mit Persistenz (Snapshots)<!--de-->
# Against PoppyDB (auto-starts local server)<!--en-->
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar --dump-dir ./data --dump-interval 300<!--de-->
./runtests.sh --poppydb --driver pooled  # --morphium-server is a deprecated alias<!--en-->
```<!--de-->
```<!--en-->
<!--de-->
<!--en-->
**Replica Set Unterstützung (experimentell)**<!--de-->
**Complete test coverage** requires running against all backends:<!--en-->
<!--de-->
```bash<!--en-->
PoppyDB unterstützt eine grundlegende Replica-Set-Emulation. Starten Sie mehrere Instanzen mit demselben Replica-Set-Namen und derselben Seed-Liste:<!--de-->
# 1. Fast in-memory tests<!--en-->
<!--de-->
./runtests.sh --driver inmem<!--en-->
```bash<!--de-->
<!--en-->
java -jar poppydb/target/poppydb-6.2.0-SNAPSHOT-cli.jar --rs-name my-rs --rs-seed host1:17017,host2:17018<!--de-->
# 2. Real MongoDB tests<!--en-->
```<!--de-->
./runtests.sh --uri mongodb://your-mongodb/testdb --driver all<!--en-->
<!--de-->
<!--en-->
**Use Cases:**<!--de-->
# 3. PoppyDB tests<!--en-->
- Lokale Entwicklung ohne MongoDB-Installation<!--de-->
./runtests.sh --poppydb --driver pooled  # --morphium-server is a deprecated alias<!--en-->
- CI/CD-Umgebungen<!--de-->
```<!--en-->
- Embedded Database für Desktop-Anwendungen<!--de-->
<!--en-->
- Testing von MongoDB-Tools (Compass, mongodump, etc.)<!--de-->
**New in v6.1**<!--en-->
<!--de-->
- ✅ **Unified test base**: All tests now use `MultiDriverTestBase` with parameterized drivers<!--en-->
**Einschränkungen:**<!--de-->
- ✅ **Driver selection**: Each test declares which drivers it supports via `@MethodSource`<!--en-->
- Keine Sharding-Unterstützung<!--de-->
- ✅ **Parallel safe**: Tests isolated per parallel slot with unique databases<!--en-->
- Einige erweiterte Aggregation-Operatoren und Joins fehlen noch (siehe `docs/howtos/inmemory-driver.md`)<!--de-->

**New in v6.0**<!--en-->
Weitere Details zu Persistenz und Replica Sets finden Sie in `docs/poppydb.md`.<!--de-->
- ✅ **Method-level reruns**: `--rerunfailed` only re-executes failing methods<!--en-->
<!--de-->
- ✅ **No more hangs**: known deadlocks resolved<!--en-->
## 🚀 Production Use Cases<!--de-->
- ✅ **Faster iteration**: noticeably quicker partial retries<!--en-->
<!--de-->
- ✅ **Better filtering**: class-name filters now reliable<!--en-->
Morphium wird produktiv eingesetzt in:<!--de-->

Run `./runtests.sh --help` to see every option.<!--en-->
- **E-Commerce**: Order-Processing mit garantierter Zustellung<!--de-->
<!--en-->
- **Finanzdienstleistungen**: Transaktions-Koordination über Microservices<!--de-->
### Test configuration precedence<!--en-->
- **Gesundheitswesen**: Patientendaten-Management mit HIPAA-Compliance<!--de-->
<!--en-->
- **IoT-Plattformen**: Device-State-Synchronisation und Command-Distribution<!--de-->
`TestConfig` consolidates all test settings. Priority order:<!--en-->
- **Content Management**: Dokument-Workflows und Benachrichtigungen<!--de-->
1. System properties (`-Dmorphium.*`)<!--en-->
<!--de-->
2. Environment variables (`MORPHIUM_*`, `MONGODB_URI`)<!--en-->
## 🤝 Community & Mitmachen<!--de-->
3. `src/test/resources/morphium-test.properties`<!--en-->
<!--de-->
4. Defaults (localhost:27017)<!--en-->
### Ressourcen<!--de-->
<!--en-->
- **Blog**: https://caluga.de<!--de-->
## 🔧 PoppyDB & InMemoryDriver<!--en-->
- **GitHub**: [sboesebeck/morphium](https://github.com/sboesebeck/morphium)<!--de-->
<!--en-->
- **Issues**: Bug-Reports und Feature-Requests auf GitHub<!--de-->
### InMemoryDriver – MongoDB-free testing<!--en-->
<!--de-->
<!--en-->
### Beitragen<!--de-->
The in-memory driver provides a largely MongoDB-compatible data store fully in memory:<!--en-->
<!--de-->
<!--en-->
Beiträge sind herzlich willkommen! Bereiche wo wir Hilfe brauchen:<!--de-->
**Features**<!--en-->
<!--de-->
- ✅ Full CRUD operations<!--en-->
- **InMemoryDriver**: Vollständigkeit von MongoDB-Features<!--de-->
- ✅ Rich query operator coverage<!--en-->
- **Dokumentation**: Beispiele, Tutorials, Übersetzungen<!--de-->
- ✅ Aggregation stages such as `$match`, `$group`, `$project`<!--en-->
- **Performance**: Optimierungen und Benchmarks<!--de-->
- ✅ Single-instance transactions<!--en-->
- **Tests**: Erweiterte Test-Szenarien<!--de-->
- ✅ Basic change streams<!--en-->
<!--de-->
- ✅ JavaScript `$where` support<!--en-->
**So tragen Sie bei:**<!--de-->
<!--en-->
1. Fork das Repository<!--de-->
**Performance**<!--en-->
2. Erstellen Sie einen Feature-Branch **von `develop`** (`git checkout -b feature/AmazingFeature develop`)<!--de-->
- Significantly faster than external MongoDB for tests<!--en-->
3. Commit Ihre Änderungen (`git commit -m 'Add AmazingFeature'`)<!--de-->
- No network latency<!--en-->
4. Push zum Branch (`git push origin feature/AmazingFeature`)<!--de-->
- No disk I/O<!--en-->
5. Öffnen Sie einen Pull Request **gegen `develop`** (nicht `master`)<!--de-->
- Ideal for CI/CD pipelines<!--en-->
<!--de-->
<!--en-->
**Wichtig:** `master` wird nur bei Releases aktualisiert. Alle PRs müssen `develop` als Ziel haben.<!--de-->
**Usage**<!--en-->
<!--de-->
```bash<!--en-->
**Hinweise:**<!--de-->
# All tests with the in-memory driver<!--en-->
- Beachten Sie die Test-Tags (`@Tag("inmemory")`, `@Tag("poppydb")`)<!--de-->
./runtests.sh --driver inmem<!--en-->
- Führen Sie `./runtests.sh --tags core` vor dem Commit aus<!--de-->
<!--en-->
- Aktualisieren Sie die Dokumentation bei API-Änderungen<!--de-->
# Specific tests<!--en-->
<!--de-->
mvn test -Dmorphium.driver=inmem -Dtest="CacheTests"<!--en-->
## 📜 Lizenz<!--de-->
```<!--en-->
<!--de-->
<!--en-->
Apache License 2.0 - Siehe [LICENSE](LICENSE) für Details<!--de-->
See `docs/howtos/inmemory-driver.md` for feature coverage and limitations.<!--en-->
<!--de-->
<!--en-->
## 🙏 Danksagungen<!--de-->
### PoppyDB – Standalone MongoDB replacement<!--en-->
<!--de-->
<!--en-->
Vielen Dank an alle Contributors die diese Release möglich gemacht haben, und an die MongoDB-Community für Support und Feedback.<!--de-->
PoppyDB (formerly MorphiumServer) runs the Morphium wire-protocol driver in a separate process, allowing it to act as a lightweight, in-memory MongoDB replacement.<!--en-->
<!--de-->
<!--en-->
---<!--de-->
**Maven dependency** (server module):<!--en-->
<!--de-->
```xml<!--en-->
**Fragen?** Öffnen Sie ein Issue auf [GitHub](https://github.com/sboesebeck/morphium/issues) oder schauen Sie in unsere [Dokumentation](docs/index.md).<!--de-->
<dependency><!--en-->
<!--de-->
  <groupId>de.caluga</groupId><!--en-->
**Upgrade geplant?** Siehe [Upgrade v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md) oder [Migration v5→v6](docs/howtos/migration-v5-to-v6.md).<!--de-->
  <artifactId>poppydb</artifactId><!--en-->
<!--de-->
  <version>6.2.0</version><!--en-->
Viel Erfolg mit Morphium 6.2.0! 🚀<!--de-->
</dependency><!--en-->
<!--de-->
```<!--en-->
*Stephan Bösebeck & das Morphium-Team*<!--de-->
<!--en-->
**Building the Server**<!--en-->
<!--en-->
```bash<!--en-->
mvn clean package -pl poppydb -am -Dmaven.test.skip=true<!--en-->
```<!--en-->
<!--en-->
This creates `poppydb/target/poppydb-6.2.0-cli.jar`.<!--en-->
<!--en-->
**Running the Server**<!--en-->
<!--en-->
```bash<!--en-->
# Start the server on the default port (17017)<!--en-->
java -jar poppydb/target/poppydb-6.2.0-cli.jar<!--en-->
<!--en-->
# Start on a different port<!--en-->
java -jar poppydb/target/poppydb-6.2.0-cli.jar --port 8080<!--en-->
<!--en-->
# Start with persistence (snapshots)<!--en-->
java -jar poppydb/target/poppydb-6.2.0-cli.jar --dump-dir ./data --dump-interval 300<!--en-->
```<!--en-->
<!--en-->
**Replica Set Support (Experimental)**<!--en-->
<!--en-->
PoppyDB supports basic replica set emulation. Start multiple instances with the same replica set name and seed list:<!--en-->
<!--en-->
```bash<!--en-->
java -jar poppydb/target/poppydb-6.2.0-cli.jar --rs-name my-rs --rs-seed host1:17017,host2:17018<!--en-->
```<!--en-->
<!--en-->
**Use cases**<!--en-->
- Local development without installing MongoDB<!--en-->
- CI environments<!--en-->
- Embedded database for desktop applications<!--en-->
- Smoke-testing MongoDB tooling (mongosh, Compass, mongodump, ...)<!--en-->
<!--en-->
**Current limitations**<!--en-->
- No sharding support<!--en-->
- Some advanced aggregation operators and joins still missing<!--en-->
<!--en-->
See `docs/poppydb.md` for more details on persistence and replica sets.<!--en-->
<!--en-->
## 🚀 Production Use Cases<!--en-->
<!--en-->
Organizations run Morphium in production for:<!--en-->
- **E-commerce**: order processing with guaranteed delivery<!--en-->
- **Financial services**: coordinating transactions across microservices<!--en-->
- **Healthcare**: patient-data workflows with strict compliance<!--en-->
- **IoT platforms**: device state synchronization and command distribution<!--en-->
- **Content management**: document workflows and event notifications<!--en-->
<!--en-->
## 🤝 Community & Contribution<!--en-->
<!--en-->
### Stay in touch<!--en-->
- **Blog**: https://caluga.de<!--en-->
- **GitHub**: [sboesebeck/morphium](https://github.com/sboesebeck/morphium)<!--en-->
- **Issues**: Report bugs or request features on GitHub<!--en-->
<!--en-->
### Contributing<!--en-->
<!--en-->
We appreciate pull requests! Areas where help is especially welcome:<!--en-->
- **InMemoryDriver**: expanding MongoDB feature coverage<!--en-->
- **Documentation**: tutorials, examples, translations<!--en-->
- **Performance**: profiling and benchmarks<!--en-->
- **Tests**: broader scenarios and regression coverage<!--en-->
<!--en-->
**How to contribute**<!--en-->
1. Fork the repository<!--en-->
2. Create a feature branch **from `develop`** (`git checkout -b feature/AmazingFeature develop`)<!--en-->
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)<!--en-->
4. Push the branch (`git push origin feature/AmazingFeature`)<!--en-->
5. Open a pull request **against `develop`** (not `master`)<!--en-->
<!--en-->
**Important:** `master` is only updated during releases. All PRs must target `develop`.<!--en-->
<!--en-->
**Tips**<!--en-->
- Respect test tags (`@Tag("inmemory")`, `@Tag("poppydb")`)<!--en-->
- Run `./runtests.sh --tags core` before submitting<!--en-->
- Update documentation when you change APIs<!--en-->
<!--en-->
## 📜 License<!--en-->
<!--en-->
Apache License 2.0 – see [LICENSE](LICENSE) for details.<!--en-->
<!--en-->
## 🙏 Thanks<!--en-->
<!--en-->
Thanks to every contributor who helped ship Morphium 6.2.0 and to the MongoDB community for continuous feedback.<!--en-->
<!--en-->
---<!--en-->
<!--en-->
**Questions?** Open an issue on [GitHub](https://github.com/sboesebeck/morphium/issues) or browse the [documentation](docs/index.md).<!--en-->
<!--en-->
**Planning an upgrade?** Follow the [migration guide](docs/howtos/migration-v5-to-v6.md).<!--en-->
<!--en-->
Enjoy Morphium 6.2.0! 🚀<!--en-->
<!--en-->
*Stephan Bösebeck & the Morphium team*<!--en-->
