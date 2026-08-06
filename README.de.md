# Morphium

**Feature-reiches MongoDB ODM und Messaging-Framework für Java 21+**

Verfügbare Sprachen: [English](README.md) | Deutsch

Morphium ist eine umfassende Datenschicht-Lösung für MongoDB mit:
- 🗄️ **Leistungsstarkes Object Mapping** mit Annotation-basierter Konfiguration
- 📨 **Integrierte Message Queue** – nutzt MongoDB als Backend (keine zusätzliche Infrastruktur!)
- ⚡ **Multi-Level Caching** mit automatischer Cluster-Synchronisation
- 🔌 **Eigener MongoDB Wire-Protocol-Treiber** für direkte Kommunikation
- 🧪 **In-Memory-Treiber** für schnelle Tests (deutlich weniger Latenz, kein MongoDB nötig)
- 🌱 **PoppyDB** — MongoDB-kompatibler In-Memory-Server: Replica Sets, Auth/TLS, Messaging-Backend
- 🎯 **JMS API (experimentell)** für standardbasiertes Messaging
- 🚀 **Java 21+** — moderne Sprachbasis (Pattern Matching, Sealed Types)

[![Maven Central](https://img.shields.io/maven-central/v/de.caluga/morphium.svg)](https://search.maven.org/artifact/de.caluga/morphium)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🎯 Warum Morphium?
 
**Einzigartig:** Morphium bietet **verteiltes Messaging ohne zusätzliche Infrastruktur**. Wenn Sie bereits MongoDB nutzen, haben Sie alles was Sie brauchen – kein RabbitMQ, Kafka oder ActiveMQ erforderlich.

### Schnellvergleich

| Feature | Morphium | Morphium + PoppyDB | Spring Data + RabbitMQ | Kafka |
|---------|----------|--------------------|------------------------|-------|
| Infrastruktur | Nur MongoDB | **Keine** — eingebetteter Java-Server | MongoDB + RabbitMQ | MongoDB + Kafka |
| Setup-Komplexität | ⭐ Sehr niedrig | ⭐ Minimal (eine Dependency) | ⭐⭐⭐ Mittel | ⭐⭐⭐⭐⭐ Hoch |
| Nachrichten persistent | Standard | Snapshots (optional) | Optional | Standard |
| Nachrichtenpriorität | ✅ Ja | ✅ Ja | ✅ Ja | ❌ Nein |
| Distributed Locks | ✅ Ja | ✅ Ja | ❌ Nein | ❌ Nein |
| Durchsatz one-way Send→Empfang* | ~870 msg/s | ~770–2100 msg/s | 10K–50K msg/s | 100K+ msg/s |
| Round-Trip Request→Response (Ping-Pong)* | 89 msg/s | **223 msg/s (2,5×)** | — | — |
| Betrieb | ⭐ Sehr einfach | ⭐ Trivial (ein Prozess) | ⭐⭐ Mittel | ⭐⭐⭐⭐ Komplex |

_* Alle Zahlen sind Richtwerte und hängen stark von Hardware und Workload ab; die
Morphium-Werte sind [gemessen](docs/v5-vs-v6-performance.md), die RabbitMQ-/Kafka-Spalten
nennen übliche Hersteller-/Community-Angaben. Die beiden Zeilen messen Unterschiedliches.
**One-way** zählt nur Send→Empfang (keine Verarbeitung, keine Antwort): ~870 msg/s gegen ein
3-Node-MongoDB-Replica-Set; PoppyDB läuft in-process und skaliert daher mit dem Host —
~770 msg/s auf einem kleinen 4-Core-CI-Host, ~2100 msg/s auf einer Laptop-CPU. **Round-Trip**
misst komplette Ping-Pongs (Request raus, Response zurück): 223 msg/s bei 4,5 ms Latenz gegen
PoppyDB vs. 89 msg/s bei 11,3 ms gegen das MongoDB-Replica-Set — 2,5-facher Durchsatz bei
weniger als halber Latenz, weil PoppyDB und Morphium Messaging aufeinander optimiert sind
(beide Seiten erkennen das Gegenüber). PoppyDBs Stärke ist die Latenz, nicht der rohe
One-way-Durchsatz auf knapper Hardware. Die Persistenz dort ist Snapshot-basiert, siehe die
[PoppyDB-Sektion](#-poppydb--mongodb-kompatibler-in-memory-server) unten._

## 🌱 PoppyDB — MongoDB-kompatibler In-Memory-Server

PoppyDB ist Morphiums Schwesterprodukt: ein In-Memory-Server, der das MongoDB Wire Protocol
spricht. Jeder Client kann sich verbinden — `mongosh`, Compass, PyMongo, die offiziellen
Treiber und natürlich Morphium. Startet in Millisekunden, braucht null Infrastruktur: kein
Docker, kein Testcontainers, keine MongoDB-Installation.

- Wire Protocol, Change Streams, Aggregation Pipeline, Indizes, Transaktionen
- **Replica-Set-Emulation** mit echter Leader Election und automatischem Failover
- **SCRAM-Authentifizierung + TLS** (6.3.0) — `mongosh` loggt sich exakt wie gegen echtes MongoDB ein
- **Deklaratives User-Provisioning** (6.3.0) via `--users-file` — idempotent, repliziert, versions-geschützt
- **Snapshot-Persistenz** — periodische Dumps, automatisches Restore beim Start
- **Messaging-Backend** — serverseitige Optimierungen speziell für Morphium Messaging

### How-to: Eingebettetes Test-Backend

```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.2.10</version>
    <scope>test</scope>
</dependency>
```

```java
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);
server.start();
// ... jeder MongoDB-Client kann sich jetzt mit localhost:27017 verbinden ...
server.shutdown();
```

### How-to: Die CLI — eine Wegwerf-MongoDB für JEDE Test-Suite

Der Embedded-Weg oben ist Java-only; das CLI-Jar funktioniert für jeden Stack. Ein einzelnes,
self-contained Jar von Maven Central (Classifier `cli`) — deine Python-/Node-/Go-/Rust-
Integrationstests bekommen in Millisekunden einen MongoDB-kompatiblen Server, kein
Docker-Image, kein Testcontainers, nichts zu installieren:

```bash
curl -O https://repo1.maven.org/maven2/de/caluga/poppydb/6.2.10/poppydb-6.2.10-cli.jar

# Start für einen Testlauf: --no-config hält den Lauf isoliert von einer
# versehentlichen ~/.config/poppydb/config auf Entwickler-Maschinen - gleiche
# Flags, gleiches Verhalten in der CI
java -jar poppydb-6.2.10-cli.jar --port 27017 --no-config
```

Test-Suite auf `mongodb://localhost:27017` zeigen lassen, Prozess danach beenden — der
Zustand ist weg (außer man will Persistenz, siehe unten). `--help` listet alle Optionen.

### How-to: Standalone-Server mit Persistenz

```bash
java -jar poppydb-6.2.10-cli.jar --port 27017 --dump-dir ./data --dump-interval 300
```

Snapshots alle 5 Minuten, finaler Dump beim Shutdown, automatisches Restore beim nächsten
Start. Die Konfiguration kann auch in einer Properties-Datei liegen: `--cfg /etc/poppydb/config`
(vorab validieren mit `--check-config`, effektives Ergebnis inspizieren mit `--print-config`).

### How-to: 3-Node-Replica-Set

Ein Prozess pro Knoten, alle mit derselben Seed-Liste — die Wahl bestimmt den Primary,
Failover passiert automatisch:

```bash
java -jar poppydb-6.2.10-cli.jar -p 17017 --rs-name myrs \
  --rs-seed host1:17017,host2:17017,host3:17017 --rs-priorities 100,50,50
```

User (`admin.system.users`) replizieren über das Set — Logins überleben den Failover.

### How-to: Authentifizierung + TLS (6.3.0)

```bash
java -jar poppydb-cli.jar -p 27018 --auth --rootUser admin --rootPassword s3cr3t \
  --ssl --sslKeystore server.jks --sslKeystorePassword changeit

mongosh "mongodb://admin:s3cr3t@localhost:27018/test?authSource=admin"
```

Für die deklarative Provisionierung eines ganzen User-Sets zeigt `--users-file` auf eine
JSON-Datei — bei jedem Leadership-Wechsel idempotent angewendet, per Version-Gate gegen
Rollback geschützt.

### How-to: Message Queue ohne MongoDB

Morphium Messaging läuft mit PoppyDB als Backend — eine vollwertige Message Queue (Topics,
exklusive Zustellung, Request/Response) mit einer einzigen Java-Dependency:

```java
PoppyDB server = new PoppyDB(27017, "localhost", 100, 10);
server.start();

try (Morphium morphium = new Morphium(cfg)) {          // cfg zeigt auf localhost:27017
    MorphiumMessaging messaging = morphium.createMessaging();
    messaging.addListenerForTopic("orders", (mq, msg) -> {
        System.out.println("Neue Bestellung: " + msg.getValue());
        return null;
    });
    messaging.start();
}
```

📖 **Vertiefung:** [PoppyDB-Guide](docs/poppydb.md) ·
[Production-Deployment-Playbook](docs/howtos/poppydb-deployment.md) ·
[Migration von MongoDB](docs/howtos/migration-mongodb-to-poppydb.md)

## 📚 Dokumentation

### Schnellzugriff
- **[Dokumentenportal](docs/index.md)** – Einstieg in sämtliche Guides
- **[Überblick](docs/overview.md)** – Kernkonzepte, Quickstart, Kompatibilität
- **[Upgrade v6.2→v6.3](docs/howtos/migration-v6_2-to-v6_3.md)** – was sich in 6.3.x ändert
- **[Upgrade v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md)** – Migrationsleitfaden für 6.2.x
- **[Migration v5→v6](docs/howtos/migration-v5-to-v6.md)** – Schritt-für-Schritt-Anleitung
- **[InMemory Driver Guide](docs/howtos/inmemory-driver.md)** – Features, Einschränkungen, Tests
- **[PoppyDB-Guide](docs/poppydb.md)** – der MongoDB-kompatible In-Memory-Server im Detail
- **[PoppyDB Deployment-Playbook](docs/howtos/poppydb-deployment.md)** – Config-File, Replica Sets, Auth/TLS in Produktion

### Weitere Ressourcen
- Aggregationsbeispiele: `docs/howtos/aggregation-examples.md`
- Messaging-Implementierungen: `docs/howtos/messaging-implementations.md`
- Performance-Guide: `docs/performance-scalability-guide.md`
- Production-Deployment: `docs/production-deployment-guide.md`
- Monitoring & Troubleshooting: `docs/monitoring-metrics-guide.md`

## 🚀 Neu in Version 6.2

### Multi-Module Maven Build
Morphium ist jetzt ein Multi-Module-Projekt: `morphium-parent` (BOM), `morphium` (Core-Bibliothek) und `poppydb` (Server). Die Core-Bibliothek `de.caluga:morphium` enthält keine Server-Abhängigkeiten (Netty etc.) mehr — ca. 90% schlanker für Nutzer, die nur das ODM benötigen.

### PoppyDB – Standalone MongoDB-kompatibler Server
Der ehemalige MorphiumServer wurde in 6.2 zum eigenständigen Modul `de.caluga:poppydb` — was
er kann und wie man ihn einsetzt, steht in der
[PoppyDB-Sektion oben](#-poppydb--mongodb-kompatibler-in-memory-server).

### MorphiumDriverException ist jetzt unchecked
`MorphiumDriverException` erbt von `RuntimeException` — konsistent mit dem MongoDB Java Driver. Eliminiert 40+ Boilerplate `catch-wrap-rethrow`-Blöcke.

### @Reference Cascade Delete/Store
`@Reference` unterstützt jetzt `cascadeDelete` und `cascadeStore` für automatisches Lifecycle-Management referenzierter Entities.

### @AutoSequence
Annotations-basierte Auto-Increment-Sequenzen — kein manuelles Counter-Management mehr nötig.

### @CreationTime Verbesserungen
Funktioniert korrekt mit `store()` und `storeList()`, unterstützt `@CreationTime` auf `Date`-, `long`- und `String`-Feldern. Field-Level-Annotation genügt, Class-Level ist nicht mehr erforderlich.

### CosmosDB Auto-Erkennung
Morphium erkennt Azure CosmosDB-Verbindungen und passt sein Verhalten automatisch an.

### Patch-Releases 6.2.1 – 6.2.10
Die 6.2.x-Patch-Releases brachten laufend Verbesserungen, unter anderem: serverseitiges Empfänger-Filtering und einen Liveness-Watchdog fürs Messaging, die neue Einstellung `defaultQueryTimeoutMS`, Feldnamen-Übersetzung in `Aggregator` und `Query.distinct()`, eine eigene `MorphiumDocumentTooLargeException` sowie zahlreiche Robustheits-Fixes für PoppyDB und den InMemoryDriver. Die späteren Patches (6.2.5–6.2.10) konzentrierten sich auf Produktions-Härtung von Wire-Pfad und Messaging: Mid-Message-Read-Timeouts desynchronisieren den Wire-Stream nicht mehr, Antworten werden gegen ihre Request-ID (`responseTo`) verifiziert, Change Streams setzen nach Neustarts am letzten Token wieder auf statt Events zu überspringen, und exklusive Messages können bei mitten in der Verarbeitung verlorenem Lock nicht mehr doppelt verarbeitet werden.

Siehe [CHANGELOG](CHANGELOG.md) für alle Details.

## Upgrade von 6.1.x auf 6.2.x

Die wichtigsten Änderungen beim Upgrade:

### Breaking: MorphiumDriverException ist jetzt unchecked

```java
// Multi-catch vereinfachen (MorphiumDriverException IST jetzt ein RuntimeException)
// Vorher:
catch (RuntimeException | MorphiumDriverException e) { ... }
// Nachher:
catch (RuntimeException e) { ... }

// throws-Deklaration kann entfernt werden (kompiliert aber weiterhin)
// Vorher:
public void doStuff() throws MorphiumDriverException { ... }
// Nachher:
public void doStuff() { ... }
```

### Breaking: MorphiumServer → PoppyDB

| | 6.1.x | 6.2.x |
|---|---|---|
| Maven-Artifact | in `morphium` enthalten | separat: `de.caluga:poppydb:6.2.10` |
| Package | `de.caluga.morphium.server` | `de.caluga.poppydb` |
| Hauptklasse | `MorphiumServer` | `PoppyDB` |
| CLI-JAR | `morphium-*-server-cli.jar` | `poppydb-*-cli.jar` |
| Test-Tag | `@Tag("morphiumserver")` | `@Tag("poppydb")` |

Wire-Protokoll-Kompatibilität ist gewahrt — PoppyDB antwortet im Hello-Handshake sowohl auf `poppyDB` als auch `morphiumServer`.

### Migrations-Checkliste

1. **`catch (RuntimeException | MorphiumDriverException`** suchen → zu `catch (RuntimeException` vereinfachen
2. **`import de.caluga.morphium.server`** suchen → durch `import de.caluga.poppydb` ersetzen
3. **`MorphiumServer`** suchen → in `PoppyDB` umbenennen
4. **`@Tag("morphiumserver")`** suchen → in `@Tag("poppydb")` umbenennen
5. **`poppydb`-Dependency** hinzufügen falls der Embedded Server genutzt wird
6. **CLI-Skripte** aktualisieren — JAR-Name ist jetzt `poppydb-*-cli.jar`

Detaillierte Anleitung: **[Migration v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md)**

## 🚀 Neu in Version 6.0

### JDK 21 & Moderne Java-Features
- **Pattern Matching**: Verbesserte Code-Klarheit und Typ-Sicherheit
- **Records**: Noch nicht als `@Entity` oder `@Embedded` unterstützt (siehe [#116](https://github.com/sboesebeck/morphium/issues/116))
- **Sealed Classes**: Bessere Typ-Hierarchien in Domain-Models
- **Virtual Threads** wurden in dieser Ära eingeführt, aber in 6.2.x wieder ausgebaut: JDK 21s `synchronized`-Pinning führte unter Last zu Deadlocks. Morphium läuft durchgehend auf Plattform-Threads; eine Neubewertung ist geplant, sobald JEP 491 (JDK 24+) die Baseline ist.

### Treiber & Konnektivität
- **SSL/TLS-Unterstützung**: Sichere Verbindungen zu MongoDB-Instanzen (seit v6.0)

### Verbessertes Messaging-System
- **Weniger Duplikate**: Optimierte Message-Processing-Logik
- **Höherer Durchsatz**: Interne Benchmarks zeigen deutliche Steigerungen
- **Distributed Locking**: Verbesserte Multi-Instance-Koordination

### In-Memory-Treiber für Testing
- **Keine MongoDB benötigt**: Komplette Test-Suite ohne externe Abhängigkeiten
- **Deutlich schnellere Tests**: Profitieren von reinem In-Memory-Zugriff
- **CI/CD-freundlich**: Perfekt für Continuous Integration Pipelines
- **Breite Feature-Unterstützung**: Viele MongoDB-Operationen, mit dokumentierten Ausnahmen

### Umfassende Dokumentation
- Komplette Neuschreibung aller Guides
- Praxis-Beispiele und Use Cases
- Migration-Guide von 5.x
- Architektur-Details und Best Practices

## Anforderungen & Abhängigkeiten
- Java 21 oder neuer
- MongoDB 5.0+ für produktive Deployments
- Maven

Maven-Abhängigkeiten:
```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>[6.2.0,)</version>
</dependency>
<dependency>
  <groupId>org.mongodb</groupId>
  <artifactId>bson</artifactId>
  <version>4.7.1</version>
</dependency>
```

Migration von v5? → `docs/howtos/migration-v5-to-v6.md`
Upgrade von v6.1? → `docs/howtos/migration-v6_1-to-v6_2.md`

## ⚡ Quick Start

### Maven Dependency

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>6.2.10</version>
</dependency>
```

### Einfaches Beispiel - Object Mapping

```java
import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.*;
import de.caluga.morphium.driver.MorphiumId;
import java.time.LocalDateTime;
import java.util.List;

// Entity definieren
@Entity
public class User {
    @Id
    private MorphiumId id;
    private String name;
    private String email;
    private LocalDateTime createdAt;
    // getters/setters
}

// Konfiguration
MorphiumConfig cfg = new MorphiumConfig();
cfg.connectionSettings().setDatabase("myapp");
cfg.clusterSettings().addHostToSeed("localhost", 27017);
cfg.driverSettings().setDriverName("PooledDriver");

Morphium morphium = new Morphium(cfg);

// Entity speichern
User user = new User();
user.setName("John Doe");
user.setEmail("john@example.com");
user.setCreatedAt(LocalDateTime.now());
morphium.store(user);

// Abfragen
List<User> users = morphium.createQueryFor(User.class)
    .f("email").matches(".*@example.com")
    .sort("createdAt")
    .asList();
```

### Messaging-Beispiel

```java
import de.caluga.morphium.messaging.MorphiumMessaging;
import de.caluga.morphium.messaging.Msg;

// Messaging Setup
MorphiumMessaging messaging = morphium.createMessaging();
messaging.setSenderId("my-app");
messaging.start();

// Nachricht senden
Msg message = new Msg("orderQueue", "Process Order", "Order #12345");
message.setPriority(5);
message.setTtl(300000); // 5 Minuten
messaging.sendMessage(message);

// Nachrichten empfangen
messaging.addListenerForTopic("orderQueue", (m, msg) -> {
    // Order verarbeiten...
    return null; // keine Antwort senden
});
```

### Konfiguration über Properties/Environment

```bash
# Environment Variables
export MONGODB_URI='mongodb://user:pass@localhost:27017/app?replicaSet=rs0'
export MORPHIUM_DRIVER=inmem

# System Properties
mvn -Dmorphium.uri='mongodb://localhost/mydb' test

# Properties-Datei (morphium.properties)
morphium.hosts=mongo1.example.com:27017,mongo2.example.com:27017
morphium.database=myapp
morphium.replicaSet=myReplicaSet
```

## 🧪 Tests & Test-Runner

### Maven-Tests
```bash
# Alle Tests
mvn test

# Vollständiger Build mit Checks
mvn clean verify

# Nur Core-Tests (schnell)
mvn test -Dgroups="core,messaging"

# Tests mit echtem MongoDB
mvn test -Dmorphium.driver=pooled -Dmorphium.uri=mongodb://localhost/testdb
```

### Test-Runner Script (`./runtests.sh`)
Umfassender Test-Runner mit farbiger Ausgabe, paralleler Ausführung und automatischen Wiederholungen.

```bash
# Alle Tests mit InMemory-Treiber (Standard)
./runtests.sh

# Nur Core-Tests
./runtests.sh --tags core,messaging

# Parallele Ausführung (8 Slots = 8x schneller!)
./runtests.sh --parallel 8 --tags core

# Nur fehlgeschlagene Tests wiederholen (NEU in 6.0!)
./runtests.sh --rerunfailed
./runtests.sh --rerunfailed --retry 3

# Tests gegen echten MongoDB-Cluster
./runtests.sh --driver pooled --uri mongodb://mongo1,mongo2/testdb

# Spezifische Test-Klasse
./runtests.sh CacheTests

# Statistiken anzeigen
./runtests.sh --stats
./getFailedTests.sh  # Liste der fehlgeschlagenen Methoden
```

**Neue Features in v6.0:**
- ✅ **Method-Level Rerun**: `--rerunfailed` führt nur fehlgeschlagene Methoden aus (nicht ganze Klassen)
- ✅ **Kein Hängen mehr**: Alle bekannten Hänge-Probleme behoben
- ✅ **Schnellere Iteration**: Spürbar schneller bei partiellen Wiederholungen
- ✅ **Bessere Filterung**: Klassenname-Filter funktionieren zuverlässig

Weitere Optionen zeigt `./runtests.sh --help`.

### Test-Konfiguration

`TestConfig` konsolidiert alle Test-Einstellungen. Priorität der Quellen:
1. System Properties (`-Dmorphium.*`)
2. Environment Variables (`MORPHIUM_*`, `MONGODB_URI`)
3. `src/test/resources/morphium-test.properties`
4. Defaults (localhost:27017)

## 🔧 PoppyDB & InMemoryDriver

### InMemoryDriver - Testing ohne MongoDB

Der InMemoryDriver bietet eine weitgehend kompatible MongoDB-Simulation im Speicher:

**Features:**
- ✅ Alle CRUD-Operationen
- ✅ Komplexe Queries mit breiter Operator-Unterstützung
- ✅ Aggregation-Pipelines (z. B. `$match`, `$group`, `$project`)
- ✅ Transaktionen (single-instance)
- ✅ Change Streams (Basis-Implementation)
- ✅ JavaScript `$where`-Operator

**Performance:**
- Spürbar schneller als Tests gegen einen externen MongoDB-Server
- Keine Netzwerk-Latenz
- Keine Disk I/O
- Perfekt für CI/CD-Pipelines

**Verwendung:**
```bash
# Alle Tests mit InMemory
./runtests.sh --driver inmem

# Spezifische Tests
mvn test -Dmorphium.driver=inmem -Dtest="CacheTests"
```

### PoppyDB - Standalone MongoDB-Ersatz

PoppyDB (ehemals MorphiumServer) ist ein eigenständiger Prozess, der das MongoDB Wire Protocol implementiert:

```bash
# Server starten
java -jar poppydb/target/poppydb-6.2.10-cli.jar

# Clients verbinden (z.B. MongoDB Compass, mongosh)
mongosh mongodb://localhost:27017

# Start mit Persistenz (Snapshots)
java -jar poppydb/target/poppydb-6.2.10-cli.jar --dump-dir ./data --dump-interval 300
```

**Replica Set Unterstützung (experimentell)**

PoppyDB unterstützt eine grundlegende Replica-Set-Emulation. Starten Sie mehrere Instanzen mit demselben Replica-Set-Namen und derselben Seed-Liste:

```bash
java -jar poppydb/target/poppydb-6.2.10-cli.jar --rs-name my-rs --rs-seed host1:17017,host2:17018
```

**Use Cases:**
- Lokale Entwicklung ohne MongoDB-Installation
- CI/CD-Umgebungen
- Embedded Database für Desktop-Anwendungen
- Testing von MongoDB-Tools (Compass, mongodump, etc.)

**Einschränkungen:**
- Keine Sharding-Unterstützung
- Einige erweiterte Aggregation-Operatoren und Joins fehlen noch (siehe `docs/howtos/inmemory-driver.md`)

Weitere Details zu Persistenz und Replica Sets finden Sie in `docs/poppydb.md`.

## 🚀 Production Use Cases

Morphium wird produktiv eingesetzt in:

- **E-Commerce**: Order-Processing mit garantierter Zustellung
- **Finanzdienstleistungen**: Transaktions-Koordination über Microservices
- **Gesundheitswesen**: Patientendaten-Management mit HIPAA-Compliance
- **IoT-Plattformen**: Device-State-Synchronisation und Command-Distribution
- **Content Management**: Dokument-Workflows und Benachrichtigungen

## 🤝 Community & Mitmachen

### Ressourcen
- **Blog**: https://caluga.de
- **GitHub**: [sboesebeck/morphium](https://github.com/sboesebeck/morphium)
- **Issues**: Bug-Reports und Feature-Requests auf GitHub

### Showcase
Unbedingt anschauen: der **[Quarkus Morphium Showcase](https://morphium.kopp-cloud.de/)** von Heiko Kopp ([Bardioc1977](https://github.com/Bardioc1977)) — eine interaktive Live-Demo von Morphium mit Quarkus, die CRUD, Caching, Aggregation-Pipelines, Geo-Abfragen, Messaging, Transaktionen, Jakarta Data und mehr zeigt. Perfekt, um Morphium auszuprobieren, bevor man die erste Zeile Code schreibt.

### Beitragen

Beiträge sind herzlich willkommen! Bereiche wo wir Hilfe brauchen:

- **InMemoryDriver**: Vollständigkeit von MongoDB-Features
- **Dokumentation**: Beispiele, Tutorials, Übersetzungen
- **Performance**: Optimierungen und Benchmarks
- **Tests**: Erweiterte Test-Szenarien

**So tragen Sie bei:**
1. Fork das Repository
2. Erstellen Sie einen Feature-Branch **von `develop`** (`git checkout -b feature/AmazingFeature develop`)
3. Commit Ihre Änderungen (`git commit -m 'Add AmazingFeature'`)
4. Push zum Branch (`git push origin feature/AmazingFeature`)
5. Öffnen Sie einen Pull Request **gegen `develop`** (nicht `master`)

**Wichtig:** `master` wird nur bei Releases aktualisiert. Alle PRs müssen `develop` als Ziel haben.

**Hinweise:**
- Beachten Sie die Test-Tags (`@Tag("inmemory")`, `@Tag("poppydb")`)
- Führen Sie `./runtests.sh --tags core` vor dem Commit aus
- Aktualisieren Sie die Dokumentation bei API-Änderungen

## 📜 Lizenz

Apache License 2.0 - Siehe [LICENSE](LICENSE) für Details

## 🙏 Danksagungen

Vielen Dank an alle Contributors, die diese Releases möglich gemacht haben, und an die MongoDB-Community für Support und Feedback.

Ein besonderer Dank geht an **Heiko Kopp** ([Bardioc1977](https://github.com/Bardioc1977)) für unzählige Beiträge, Praxis-Feedback aus großen Produktions-Deployments und den großartigen [Quarkus Morphium Showcase](https://morphium.kopp-cloud.de/).

---

**Fragen?** Öffnen Sie ein Issue auf [GitHub](https://github.com/sboesebeck/morphium/issues) oder schauen Sie in unsere [Dokumentation](docs/index.md).

**Upgrade geplant?** Siehe [Upgrade v6.1→v6.2](docs/howtos/migration-v6_1-to-v6_2.md) oder [Migration v5→v6](docs/howtos/migration-v5-to-v6.md).

Viel Erfolg mit Morphium! 🚀

*Stephan Bösebeck & das Morphium-Team*
