# Morphium 6.1.1

**Feature-reiches MongoDB ODM und Messaging-Framework für Java 21+**

Verfügbare Sprachen: [English](README.md) | Deutsch

Morphium ist eine umfassende Datenschicht-Lösung für MongoDB mit:
- 🗄️ **Leistungsstarkes Object Mapping** mit Annotation-basierter Konfiguration
- 📨 **Integrierte Message Queue** – nutzt MongoDB als Backend (keine zusätzliche Infrastruktur!)
- ⚡ **Multi-Level Caching** mit automatischer Cluster-Synchronisation
- 🔌 **Eigener MongoDB Wire-Protocol-Treiber** für direkte Kommunikation
- 🧪 **In-Memory-Treiber** für schnelle Tests (deutlich weniger Latenz, kein MongoDB nötig)
- 🎯 **JMS API (experimentell)** für standardbasiertes Messaging
- 🚀 **JDK 21** mit Virtual Threads für optimale Concurrency

[![Maven Central](https://img.shields.io/maven-central/v/de.caluga/morphium.svg)](https://search.maven.org/artifact/de.caluga/morphium)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 🎯 Warum Morphium?

**Einzigartig:** Morphium bietet **verteiltes Messaging ohne zusätzliche Infrastruktur**. Wenn Sie bereits MongoDB nutzen, haben Sie alles was Sie brauchen – kein RabbitMQ, Kafka oder ActiveMQ erforderlich.

### Schnellvergleich

| Feature | Morphium | Spring Data + RabbitMQ | Kafka |
|---------|----------|------------------------|-------|
| Infrastruktur | Nur MongoDB | MongoDB + RabbitMQ | MongoDB + Kafka |
| Setup-Komplexität | ⭐ Sehr niedrig | ⭐⭐⭐ Mittel | ⭐⭐⭐⭐⭐ Hoch |
| Nachrichten persistent | Standard | Optional | Standard |
| Nachrichtenpriorität | ✅ Ja | ✅ Ja | ❌ Nein |
| Distributed Locks | ✅ Ja | ❌ Nein | ❌ Nein |
| Durchsatz (interne Tests) | ~8K msg/s | 10K–50K msg/s | 100K+ msg/s |
| Betrieb | ⭐ Sehr einfach | ⭐⭐ Mittel | ⭐⭐⭐⭐ Komplex |

_* Richtwerte aus internen Messungen; tatsächliche Werte hängen von Hardware und Workload ab._

## 📚 Dokumentation

### Schnellzugriff
- **[Dokumentenportal](docs/index.md)** – Einstieg in sämtliche Guides
- **[Überblick](docs/overview.md)** – Kernkonzepte, Quickstart, Kompatibilität
- **[Migration v5→v6](docs/howtos/migration-v5-to-v6.md)** – Schritt-für-Schritt-Anleitung
- **[InMemory Driver Guide](docs/howtos/inmemory-driver.md)** – Features, Einschränkungen, Tests

### Weitere Ressourcen
- Aggregationsbeispiele: `docs/howtos/aggregation-examples.md`
- Messaging-Implementierungen: `docs/howtos/messaging-implementations.md`
- Performance-Guide: `docs/performance-scalability-guide.md`
- Production-Deployment: `docs/production-deployment-guide.md`
- Monitoring & Troubleshooting: `docs/monitoring-metrics-guide.md`

## 🚀 Neu in Version 6.1

### MorphiumServer – Der "Drop-in"-Ersatz
Morphium 6.1 macht den **MorphiumServer** zu einem echten "Drop-in"-Ersatz für MongoDB in Entwicklungs- und Testumgebungen:
- ✅ **Volle Wire-Protocol-Unterstützung**: Verwendung jedes Standard-MongoDB-Clients (mongosh, Compass, etc.)
- ✅ **CLI-Tooling**: Eigener `morphium-server-cli` für einfache Bereitstellung
- ✅ **Replica-Set-Emulation**: Testen von Multi-Node-Cluster-Verhalten ohne echtes MongoDB
- ✅ **Persistenz**: Snapshot-Unterstützung zur Bewahrung von In-Memory-Daten über Neustarts hinweg

## 🚀 Neu in Version 6.0

### JDK 21 & Moderne Java-Features
- **Virtual Threads**: Messaging-System optimiert für Project Loom
- **Pattern Matching**: Verbesserte Code-Klarheit und Typ-Sicherheit
- **Records Support**: Volle Unterstützung für Java Records als Entities
- **Sealed Classes**: Bessere Typ-Hierarchien in Domain-Models

### Treiber & Konnektivität
- **SSL/TLS-Unterstützung**: Sichere Verbindungen zu MongoDB-Instanzen (seit v6.0)
- **Virtual Threads** im Treiber für optimale Performance

### Verbessertes Messaging-System
- **Weniger Duplikate**: Optimierte Message-Processing-Logik
- **Virtual Thread Integration**: Bessere Concurrency-Performance
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

## Anforderungen & Abhaengigkeiten
- Java 21 oder neuer
- MongoDB 5.0+ fuer produktive Deployments
- Maven

Maven-Abhaengigkeiten:
```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>[6.1.1,)</version>
</dependency>
<dependency>
  <groupId>org.mongodb</groupId>
  <artifactId>bson</artifactId>
  <version>4.7.1</version>
</dependency>
```

Migration von v5? → `docs/howtos/migration-v5-to-v6.md`

## ⚡ Quick Start

### Maven Dependency

```xml
<dependency>
  <groupId>de.caluga</groupId>
  <artifactId>morphium</artifactId>
  <version>6.1.1</version>
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

## 🔧 MorphiumServer & InMemoryDriver

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

### MorphiumServer - Standalone MongoDB-Ersatz

MorphiumServer ist ein eigenständiger Prozess, der das MongoDB Wire Protocol implementiert:

```bash
# Server starten
java -jar target/morphium-6.1.1-server-cli.jar

# Clients verbinden (z.B. MongoDB Compass, mongosh)
mongosh mongodb://localhost:27017

# Start mit Persistenz (Snapshots)
java -jar target/morphium-6.1.1-server-cli.jar --dump-dir ./data --dump-interval 300
```

**Replica Set Unterstützung (experimentell)**

MorphiumServer unterstützt eine grundlegende Replica-Set-Emulation. Starten Sie mehrere Instanzen mit demselben Replica-Set-Namen und derselben Seed-Liste:

```bash
java -jar target/morphium-6.1.1-server-cli.jar --rs-name my-rs --rs-seed host1:17017,host2:17018
```

**Use Cases:**
- Lokale Entwicklung ohne MongoDB-Installation
- CI/CD-Umgebungen
- Embedded Database für Desktop-Anwendungen
- Testing von MongoDB-Tools (Compass, mongodump, etc.)

**Einschränkungen:**
- Keine Sharding-Unterstützung
- Einige erweiterte Aggregation-Operatoren und Joins fehlen noch (siehe `docs/howtos/inmemory-driver.md`)

Weitere Details zu Persistenz und Replica Sets finden Sie in `docs/morphium-server.md`.

## 🚀 Production Use Cases

Morphium wird produktiv eingesetzt in:

- **E-Commerce**: Order-Processing mit garantierter Zustellung
- **Finanzdienstleistungen**: Transaktions-Koordination über Microservices
- **Gesundheitswesen**: Patientendaten-Management mit HIPAA-Compliance
- **IoT-Plattformen**: Device-State-Synchronisation und Command-Distribution
- **Content Management**: Dokument-Workflows und Benachrichtigungen

## 🤝 Community & Mitmachen

### Ressourcen
- **Slack**: [Team Morphium](https://join.slack.com/t/team-morphium/shared_invite/enQtMjgwODMzMzEzMTU5LTA1MjdmZmM5YTM3NjRmZTE2ZGE4NDllYTA0NTUzYjU2MzkxZTJhODlmZGQ2MThjMGY0NmRkMWE1NDE2YmQxYjI)
- **Blog**: https://caluga.de
- **GitHub**: [sboesebeck/morphium](https://github.com/sboesebeck/morphium)
- **Issues**: Bug-Reports und Feature-Requests auf GitHub

### Beitragen

Beiträge sind herzlich willkommen! Bereiche wo wir Hilfe brauchen:

- **InMemoryDriver**: Vollständigkeit von MongoDB-Features
- **Dokumentation**: Beispiele, Tutorials, Übersetzungen
- **Performance**: Optimierungen und Benchmarks
- **Tests**: Erweiterte Test-Szenarien

**So tragen Sie bei:**
1. Fork das Repository
2. Erstellen Sie einen Feature-Branch (`git checkout -b feature/AmazingFeature`)
3. Commit Ihre Änderungen (`git commit -m 'Add AmazingFeature'`)
4. Push zum Branch (`git push origin feature/AmazingFeature`)
5. Öffnen Sie einen Pull Request

**Hinweise:**
- Beachten Sie die Test-Tags (`@Tag("inmemory")`, `@Tag("external")`)
- Führen Sie `./runtests.sh --tags core` vor dem Commit aus
- Aktualisieren Sie die Dokumentation bei API-Änderungen

## 📜 Lizenz

Apache License 2.0 - Siehe [LICENSE](LICENSE) für Details

## 🙏 Danksagungen

Vielen Dank an alle Contributors die diese Release möglich gemacht haben, und an die MongoDB-Community für Support und Feedback.

---

**Fragen?** Öffnen Sie ein Issue auf [GitHub](https://github.com/sboesebeck/morphium/issues) oder schauen Sie in unsere [Dokumentation](docs/index.md).

**Upgrade geplant?** Siehe [Migration Guide](docs/howtos/migration-v5-to-v6.md) für Schritt-für-Schritt-Anleitung.

Viel Erfolg mit Morphium 6.1.1! 🚀

*Stephan Bösebeck & das Morphium-Team*
