# Morphium

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="branding/morphium-logo-dark.svg">
    <img src="branding/morphium-logo.svg" alt="Morphium" width="640">
  </picture>
</p>

**Feature-reiches MongoDB ODM und Messaging-Framework für Java 21+**

Verfügbare Sprachen: [English](README.md) | Deutsch

Morphium ist eine umfassende Datenschicht-Lösung für MongoDB mit:
- 🗄️ **Leistungsstarkes Object Mapping** mit Annotation-basierter Konfiguration
- 📨 **Integrierte Message Queue** – nutzt MongoDB als Backend (keine zusätzliche Infrastruktur!)
- ⚡ **Multi-Level Caching** mit automatischer Cluster-Synchronisation
- 🔌 **Eigener MongoDB Wire-Protocol-Treiber** für direkte Kommunikation
- 🧪 **In-Memory-Treiber** für schnelle Tests (deutlich weniger Latenz, kein MongoDB nötig)
- 🌱 **[PoppyDB](https://sboesebeck.github.io/morphium/poppydb/)** — MongoDB-kompatibler In-Memory-Server: Replica Sets, Auth/TLS, Messaging-Backend
- 🎯 **JMS API (experimentell)** für standardbasiertes Messaging
- 🚀 **Java 21+** — moderne Sprachbasis (Pattern Matching, Sealed Types)

[![Maven Central](https://img.shields.io/maven-central/v/de.caluga/morphium.svg)](https://search.maven.org/artifact/de.caluga/morphium)
[![Tests](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fsboesebeck%2Fmorphium%2Ftest-results%2Fbadges%2Ftests.json)](https://github.com/sboesebeck/morphium/releases)
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
nennen übliche Hersteller-/Community-Angaben. **One-way** (Send→Empfang, keine Antwort)
läuft mit ~870 msg/s gegen ein 3-Node-MongoDB-Replica-Set; PoppyDB läuft in-process und
skaliert mit dem Host, von ~770 msg/s auf einem kleinen CI-Host bis ~2100 msg/s auf einer
Laptop-CPU. **Round-Trip** (Request→Response) zeigt die enge PoppyDB/Morphium-Messaging-
Integration: 223 msg/s bei 4,5 ms Latenz gegen PoppyDB vs. 89 msg/s bei 11,3 ms gegen das
MongoDB-Replica-Set — 2,5-facher Durchsatz bei weniger als halber Latenz. Volle Methodik
und der Kafka-Vergleich in [docs/v5-vs-v6-performance.md](docs/v5-vs-v6-performance.md)._

_**Wie hält sich Kafkas 100K+-Angabe?** Diese Zahl ist real für Kafkas normalen, gebatchten
Async-Modus — zwingt man Kafka aber in Morphiums Semantik (synchron, einzeln bestätigte
Sends) auf derselben Hardware, fällt Kafka auf ~8–10K msg/s vs. ~1.800 msg/s für
Morphium+PoppyDB — Faktor 4–5, nicht 100+. Der Unterschied ist Architektur, nicht
Implementierungsqualität: Kafkas Spitzendurchsatz kommt aus Batching, Morphium bestätigt
bewusst jede Message einzeln. Details in
[docs/v5-vs-v6-performance.md](docs/v5-vs-v6-performance.md)._

## 🌱 PoppyDB — MongoDB-kompatibler In-Memory-Server

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="branding/poppydb-logo-dark.svg">
    <img src="branding/poppydb-logo.svg" alt="PoppyDB" width="560">
  </picture>
</p>

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
    <version>6.3.9</version>
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
curl -O https://repo1.maven.org/maven2/de/caluga/poppydb/6.3.9/poppydb-6.3.9-cli.jar

# Start für einen Testlauf: --no-config hält den Lauf isoliert von einer
# versehentlichen ~/.config/poppydb/config auf Entwickler-Maschinen - gleiche
# Flags, gleiches Verhalten in der CI
java -jar poppydb-6.3.9-cli.jar --port 27017 --no-config
```

Test-Suite auf `mongodb://localhost:27017` zeigen lassen, Prozess danach beenden — der
Zustand ist weg (außer man will Persistenz, siehe unten). `--help` listet alle Optionen.

Die CLI ist aber nicht nur ein Test-Werkzeug: **Als Messaging-Backend ist sie
production-ready** — genau dafür existieren PoppyDBs serverseitige
Messaging-Optimierungen. Mit Snapshot-Persistenz, Replica Set für HA und Auth/TLS (alles
unten) hat man einen stehenden Message Broker aus einem einzigen Jar. Ein genereller
MongoDB-*Ersatz* ist sie nur für Dev/Test — als dediziertes Backend für Morphium Messaging
ist sie die Empfehlung, siehe das
[Deployment-Playbook](docs/howtos/poppydb-deployment.md).

### How-to: Standalone-Server mit Persistenz

```bash
java -jar poppydb-6.3.9-cli.jar --port 27017 --dump-dir ./data --dump-interval 300
```

Snapshots alle 5 Minuten, finaler Dump beim Shutdown, automatisches Restore beim nächsten
Start. Die Konfiguration kann auch in einer Properties-Datei liegen: `--cfg /etc/poppydb/config`
(vorab validieren mit `--check-config`, effektives Ergebnis inspizieren mit `--print-config`).

### How-to: 3-Node-Replica-Set

Ein Prozess pro Knoten, alle mit derselben Seed-Liste — die Wahl bestimmt den Primary,
Failover passiert automatisch:

```bash
java -jar poppydb-6.3.9-cli.jar -p 17017 --rs-name myrs \
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
exklusive Zustellung, Request/Response) mit einer einzigen Java-Dependency. Das ist ein
Produktions-Use-Case, kein Test-Trick: PoppyDB und Morphium Messaging sind aufeinander
optimiert, und eine Standalone-PoppyDB (CLI, mit Persistenz + Replica Set + Auth/TLS) ergibt
einen dedizierten Message Broker, ohne eine MongoDB zu betreiben:

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

📖 **Vertiefung:** [Online-Doku](https://sboesebeck.github.io/morphium/poppydb/) ·
[PoppyDB-Guide](docs/poppydb.md) ·
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

## 🚀 Neu in Version 6.3

### Zum 6.3.3 → 6.3.6 Release-Sturm (August 2026)

Vier Patch-Releases in einer Woche ist nicht unser übliches Tempo: Ein KI-gestützter Deep-Code-
Review des Change-Stream- und Replikationspfads deckte eine Klasse von last-only-Bugs auf
(stiller Event-Verlust bei Resume, Live-Events überholen den History-Replay, unbegrenztes
Memory-Pinning), die nie ein Nutzer gemeldet hatte — genau die Art, die kein Issue auslöst,
sondern Monate später als still divergierte Daten auftaucht. **6.3.4** brachte diese Fixes,
führte aber einen Client-seitigen Resume-Token-Loop ein (#329), noch am selben Tag in **6.3.5**
gefixt; **6.3.6** behob dann einen verwandten Connection-Pool-Topologie-Bug (#330) vom eigenen
Staging-Cluster. Die volle Geschichte pro Fix steht im [CHANGELOG](CHANGELOG.md).

**Wer auf einer 6.3.x-Version ist: direkt auf 6.3.6 upgraden**, in der Reihenfolge unten.

> ⚠️ **Upgrade-Reihenfolge beachten: erst die Clients, dann die Server — und 6.3.4 überspringen.**
>
> Jeder Client bis einschließlich 6.3.4 trägt einen Resume-Token-Bug
> ([#329](https://github.com/sboesebeck/morphium/issues/329)): Beendet der Server einen Stream
> mit `ChangeStreamHistoryLost`, verwirft der Monitor seinen Resume-Token und holt ihn sofort
> wieder zurück — endlose Retries ohne Backoff. Ein PoppyDB-Restart (oder ein aus dem
> Oplog-Fenster fallender MongoDB-Consumer) löst das in jedem verbundenen Client < 6.3.5
> gleichzeitig aus und DDoSt den Server faktisch, bis jeder Client-Prozess von Hand
> durchgestartet wird.
>
> **Rollout-Reihenfolge: 1)** erst alle Client-Anwendungen auf ≥ 6.3.5 heben, **2)** erst
> danach die PoppyDB-Server durchstarten — ein zuerst deployter Server schärft die Schleife in
> jedem noch nicht aktualisierten Client.
>
> Ab 6.3.5 persistiert ein PoppyDB-Server mit Dump-Verzeichnis außerdem seine
> Change-Stream-Sequenz über Restarts, sodass geordnete Restarts Resume-Tokens gar nicht mehr
> invalidieren.

### Zwei optionale Integrationsmodule
`morphium-jakarta-data` implementiert [Jakarta Data 1.0](https://jakarta.ee/specifications/data/1.0/) auf Basis von Morphiums Query-Engine — `@Repository`-Interfaces mit Query-Ableitung aus Methodennamen, JDQL über `@Query` (inklusive `GROUP BY`/`HAVING`, übersetzt in eine Aggregation-Pipeline), Offset- sowie Cursor-/Keyset-Pagination. `quarkus-morphium` setzt darauf auf und liefert die CDI-Integration: Config-Mapping, `@MorphiumTransactional`, Health-Checks, Dev Services, Dev UI, GraalVM-Native-Image-Support und Repository-Generierung zur Build-Zeit per Gizmo. Beide sind optional — der Core hängt von keinem der beiden ab, und `-DskipExtensions` erzeugt weiterhin einen reinen Core-Build. Siehe [Jakarta Data](docs/jakarta-data.md) und [Quarkus-Extension](docs/quarkus-extension.md).

**Hinweis:** Die Quarkus-Extension ist von `io.quarkiverse.morphium:quarkus-morphium:1.2.0` nach `de.caluga:quarkus-morphium:6.3.0` umgezogen. Nur die Koordinaten — keine Paketumbenennungen, keine API-Änderungen.

### DualChannelMessaging (Beta)
Eine dritte Messaging-Implementierung: die gewohnte einzelne Collection samt Cursor für Broadcast- und Topic-Verkehr, dazu eine eigene Collection pro Empfänger mit eigenem Cursor und Dispatcher-Thread für gerichtete Nachrichten und Antworten. Auswahl über `cfg.messagingSettings().setMessagingImplementation("DualChannelMessaging")`. Bewusst Beta — jenseits der Sättigung tauscht sie etwas Durchsatz gegen deutlich bessere Tail-Latenz. Siehe `docs/howtos/messaging-implementations.md`.

> ⚠️ **Alle Messaging-Teilnehmer einer Queue müssen dieselbe Implementierung fahren.** Das galt schon immer für `SingleCollectionMessaging` und `MultiCollectionMessaging` und gilt genauso für `DualChannelMessaging`: Die Implementierungen verwenden unterschiedliche Collection-Layouts, eine Brücke dazwischen gibt es nicht. Eine Abweichung schlägt *still* fehl — ein Standard-Knoten, der auf die Antwort eines Dual-Channel-Responders wartet, läuft ewig in den Timeout, weil die Antwort in der DM-Collection des Anfragenden landet, die Standard nie liest. Alle Knoten gemeinsam umstellen und Request/Reply-Verkehr währenddessen leeren oder pausieren.

### Messaging-Verbesserungen (alle Implementierungen)
Ein Datenbank-Roundtrip weniger pro nicht-exklusiver Nachricht (Verarbeitung direkt aus dem `fullDocument` des Change Streams), event-getriebene Zustellung von Requeue-Nachrichten, konfigurierbare Default-TTL und Fallback-Poll-Taktung, ein Fallback-Poll, der sich nach der Lebendigkeit des Change Streams richtet, und ein Trace der Verarbeitungsentscheidung zur Diagnose von Antwort-Timeouts.

### PoppyDB: betreibbar, nicht nur startbar
Echte SCRAM-SHA-1-/SCRAM-SHA-256-Authentifizierung mit optionaler Durchsetzung (`--auth`), deklarative Benutzerprovisionierung aus einer Datei (`--users-file`) und Benutzer, die über das ReplicaSet replizieren, statt nur auf einem Knoten zu existieren. Konfigurationsdateien (`--cfg`, `--print-config`, `--check-config`) halten Secrets von der Kommandozeile fern, `--log-level` beendet die DEBUG-Flut, und eine DevOps-Kommandofläche ergänzt Live-`currentOp`/`killOp`, `rs.conf()`, `listCommands`, `hostInfo`, `dbHash` sowie ein `validate`, das die Indizes wirklich abläuft.

### Speicher-Wasserstandsmarken und ehrliche Größenlimits
Zwei Heap-Marken (`--memory-warn` / `--memory-reject`, entschieden anhand des Live-Sets nach GC) lehnen dokumenterzeugende Schreibvorgänge mit einem wiederholbaren `ExceededMemoryLimit` ab, bevor der Heap stirbt — Updates, Deletes und TTL-Ablauf bleiben erlaubt, damit das System abfließen kann. Das 16-MB-BSON-Dokumentlimit wird jetzt wie bei mongod durchgesetzt statt nur angekündigt, und `maxMessageSizeBytes` wird durchgängig respektiert, inklusive byte-basierter Aufteilung von Schreib-Batches.

### InMemoryDriver: der Abstand zu mongod schrumpft
Neue Aggregation-Stages (`$merge`, `$documents`, `$densify`, `$fill`, `$setWindowFields`, `$collStats`, `$listSessions` und ein echtes `$out`), rund 40 zusätzliche Expression-Operatoren, die Positions-Operatoren `$`/`$[]`/`$[<identifier>]` mit `arrayFilters` sowie `$bit`. Dazu eine lange Liste von Korrektheitsfixes — darunter `$geoWithin` mit `$center`/`$centerSphere`/`$polygon`, das *jedes* Dokument traf, UTC-korrekte Datumsoperatoren mit 1-basiertem `$month` und ein `$project`-Inclusion-Modus, der die Ausgabe tatsächlich einschränkt.

### Härtung von Replikation und Failover
PoppyDBs Replikation ist jetzt verlustfrei, reihenfolgetreu und umfasst Indexdefinitionen. Behoben: ein neu synchronisierendes Secondary, das seinen Initial-Sync-Wipe als Change-Stream-Drop-Events verbreitete (womit sich `admin.system.users` während eines Stepdowns clusterweit zerstören ließ), ein degradierter Leader, der bei `primary == true` hängen blieb, ein `rs.status()`, das einen toten Peer für immer als SECONDARY meldete, und ein unverschlüsselter interner Wahl-/Replikationskanal, der `--auth`/`--ssl` im ReplicaSet wirkungslos machte. Auf Client-Seite konnte der Failover-Lesepfad eine nackte NPE an jedem Retry vorbei werfen.

### Performance
Die Duplikatsprüfung auf `_id` beim Insert ist ein O(1)-Indexzugriff statt eines vollständigen Scans unter dem Schreiblock, das Before-Image des Change Streams wird nicht mehr doppelt tief kopiert, und das Rebuild-Pingpong zwischen offener Transaktion und parallelen Lesern ist beseitigt.

Das Upgrade beschreibt der [Migrationsleitfaden](docs/howtos/migration-v6_2-to-v6_3.md) Schritt für Schritt; alle Details stehen im [CHANGELOG](CHANGELOG.md).

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

Zwei Breaking Changes: `MorphiumDriverException` erbt jetzt von `RuntimeException` statt
`Exception` (in Multi-Catches und `throws`-Klauseln entfernen), und der eingebettete Server
wurde in ein eigenes Modul ausgelagert und umbenannt: `MorphiumServer` → `PoppyDB`
(`de.caluga:poppydb`, Package `de.caluga.poppydb`, Tag `@Tag("poppydb")`) — der Wire-Handshake
antwortet weiterhin auf beide Namen, gemischte Replica Sets funktionieren also weiter.
Detaillierte Checkliste und Codebeispiele im
**[Migrationsleitfaden](docs/howtos/migration-v6_1-to-v6_2.md)**.

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
  <version>6.3.9</version>
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

`--rerunfailed` führt gezielt nur fehlgeschlagene Methoden erneut aus (nicht ganze Klassen).
Weitere Optionen zeigt `./runtests.sh --help`.

### Test-Konfiguration

`TestConfig` konsolidiert alle Test-Einstellungen. Priorität der Quellen:
1. System Properties (`-Dmorphium.*`)
2. Environment Variables (`MORPHIUM_*`, `MONGODB_URI`)
3. `src/test/resources/morphium-test.properties`
4. Defaults (localhost:27017)

## 🔧 InMemoryDriver

### Testing ohne MongoDB

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

Feature-Umfang und Einschränkungen siehe `docs/howtos/inmemory-driver.md`. Für PoppyDB — den
eigenständigen MongoDB-kompatiblen Server — siehe die
[🌱 PoppyDB-Sektion](#-poppydb--mongodb-kompatibler-in-memory-server) oben.

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
