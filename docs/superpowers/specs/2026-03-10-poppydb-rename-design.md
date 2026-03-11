# PoppyDB: MorphiumServer Rebranding & Modul-Extraktion

**Datum:** 2026-03-10
**Version:** Morphium 6.2.0-SNAPSHOT
**Status:** Draft

## Zusammenfassung

Der MorphiumServer (In-Memory MongoDB-kompatibler Server) wird umbenannt in **PoppyDB** und als eigenständiges Maven-Modul aus dem Morphium-Monolith extrahiert. Ziel: PoppyDB als eigenständiges, herunterladbares Produkt mit eigener Identität positionieren.

## Namensgebung

- **PoppyDB** — Poppy (Mohnblume) ist die Quelle von Morphium
- Tagline: "A lightweight, in-memory MongoDB-compatible server — optimized for messaging, built for dev & test"
- Kein Namenskonflikt im Java/DB-Ökosystem (geprüft: Maven Central, GitHub, npm, Trademarks)
- Package: `de.caluga.poppydb`

## Kernbotschaften

- Leichtgewichtig, schnell, In-Memory
- Drop-in MongoDB Replacement
- Optimiert für Messaging
- Ideal für Entwicklung und Tests
- Keine "echte" Persistenz (Snapshots ja, aber kein Disk-basierter Storage)

## Architektur-Entscheidung: Modul-Extraktion

### Ist-Zustand

Single-Module Maven-Projekt (`de.caluga:morphium:6.2.0-SNAPSHOT`), packaging `jar`. Server-Klassen leben in `de.caluga.morphium.server.*` (18 Klassen, ~6.355 LOC).

### Soll-Zustand

Multi-Module Maven-Projekt:

```
morphium/
  pom.xml                              ← Parent POM (packaging: pom)
  morphium-core/
    pom.xml                            ← artifactId: morphium (NICHT umbenennen!)
    src/main/java/de/caluga/morphium/  ← alles wie bisher, minus server/
    src/test/java/...
  poppydb/
    pom.xml                            ← artifactId: poppydb
    src/main/java/de/caluga/poppydb/
    src/main/assembly/server-cli.xml   ← Assembly für Fat-JAR
    src/test/java/...
```

### Designprinzip: Maximale Abwärtskompatibilität

- **Morphium-Nutzer merken nichts.** Die artifactId bleibt `morphium`, das Package bleibt `de.caluga.morphium.*`.
- Die Server-Klassen waren nie Teil der öffentlichen Morphium-API.
- Kein deprecated-Wrapper, kein Kompatibilitäts-Shim — harter Cut im Server-Package, da es keine externen Nutzer gibt.

## Detailplan

### 1. Parent POM

Das aktuelle `pom.xml` wird zum Parent POM:
- `packaging: pom`
- Gemeinsame Properties (Java 21, Encoding, Versionen)
- Gemeinsames Dependency-Management
- Plugin-Management
- `<modules>`: `morphium-core`, `poppydb`

### 2. Modul `morphium-core`

- **artifactId:** `morphium` (unverändert für Kompatibilität!)
- **groupId:** `de.caluga` (unverändert)
- Enthält alles was heute in Morphium ist, **minus** `de.caluga.morphium.server.*`
- Alle bestehenden Tests (minus MorphiumServerTest)
- Keine Änderungen an bestehenden Klassen nötig

### 3. Modul `poppydb`

- **artifactId:** `poppydb`
- **groupId:** `de.caluga`
- **Dependency:** `de.caluga:morphium:${project.version}`
- **Package:** `de.caluga.poppydb`

Klassen-Mapping (alt → neu):

| Alt (`de.caluga.morphium.server`) | Neu (`de.caluga.poppydb`) |
|---|---|
| `MorphiumServer` | `PoppyDB` |
| `MorphiumServerCLI` | `PoppyDBCLI` |
| `MongoCommandHandler` | `MongoCommandHandler` (unverändert) |
| `ReplicationManager` | `ReplicationManager` (unverändert) |
| `ReplicationCoordinator` | `ReplicationCoordinator` (unverändert) |
| `election/*` | `election/*` (unverändert) |
| `netty/*` | `netty/*` (unverändert) |
| `messaging/*` | `messaging/*` (unverändert) |

Nur die zwei Hauptklassen werden umbenannt. Alle internen Klassen behalten ihre Namen — nur das Package ändert sich.

### 4. Fat-JAR / CLI

- Assembly Descriptor wandert nach `poppydb/src/main/assembly/server-cli.xml`
- Output: `poppydb-cli.jar` (statt `morphium-server-cli.jar`)
- MainClass: `de.caluga.poppydb.PoppyDBCLI`

### 5. Scripts

| Alt | Neu |
|-----|-----|
| `scripts/startMorphiumServer.sh` | `scripts/startPoppyDB.sh` |
| `scripts/morphium_server.sh` | `scripts/poppydb.sh` |

Funktionsnamen in den Scripts anpassen (`_ms_local_*` → `_pdb_local_*` o.ä.)

### 6. Dokumentation

- README.md: Abschnitt "MorphiumServer" → "PoppyDB"
- CHANGELOG.md: Neuer Eintrag für 6.2.0 mit Rename-Info
- planned_features.md: Referenzen aktualisieren

### 7. Tests

- `MorphiumServerTest.java` → `PoppyDBTest.java` (wandert ins poppydb-Modul)
- Tag bleibt `@Tag("server")` (CI-Kompatibilität)

## Abhängigkeiten von PoppyDB auf Morphium

PoppyDB nutzt folgende Morphium-Interna:
- `de.caluga.morphium.driver.inmem.InMemoryDriver` — der eigentliche Datenspeicher
- `de.caluga.morphium.driver.wireprotocol.*` — Wire Protocol Encoding/Decoding
- `de.caluga.morphium.driver.wire.SslHelper` — SSL/TLS
- `de.caluga.morphium.driver.bson.*` — BSON-Handling

Diese müssen als public API von morphium-core zugänglich bleiben (sind sie bereits).

## Maven Central / Sonatype

### Ist-Zustand

- `de.caluga:morphium` wird auf Maven Central über Sonatype OSSRH deployed
- Das Assembly-Plugin baut das Fat-JAR (`morphium-*-server-cli.jar`) in der `package`-Phase
- Fat-JAR wird als **attached artifact** (mit Classifier) mit hochgeladen
- Netty ist als `<optional>true</optional>` markiert — Morphium-Nutzer bekommen es nicht transitiv
- Die Server-Klassen sind aber Teil der `morphium.jar`

### Soll-Zustand

**`de.caluga:morphium` (morphium-core):**
- Server-Klassen sind raus → JAR wird kleiner
- Netty-Dependency entfällt komplett (war nur für Server nötig)
- Assembly-Plugin entfällt (kein Fat-JAR mehr in diesem Modul)
- Bestehende Nutzer profitieren: kleinere JAR, weniger optionale Dependencies

**`de.caluga:poppydb` (neues Artefakt):**
- Neues Artefakt unter existierender GroupId `de.caluga` — kein neuer Sonatype-Namespace nötig
- Netty als compile-Dependency (nicht mehr optional, PoppyDB braucht es immer)
- Assembly-Plugin für Fat-JAR (`poppydb-*-cli.jar`)
- Braucht eigene Sonatype-Konfiguration im POM:
  - `maven-source-plugin` (sources JAR)
  - `maven-javadoc-plugin` (javadoc JAR)
  - GPG-Signing im Release-Profil
  - SCM/Developer/License-Metadaten (können vom Parent POM geerbt werden)

### Deployment-Ablauf

Bei `mvn deploy` im Parent werden beide Module deployed:
1. `de.caluga:morphium:6.2.0` → Maven Central (wie bisher)
2. `de.caluga:poppydb:6.2.0` → Maven Central (neu)
3. `poppydb-6.2.0-cli.jar` → als attached artifact von poppydb

### Nutzung durch Endanwender

**Als Library (Maven-Dependency):**
```xml
<dependency>
    <groupId>de.caluga</groupId>
    <artifactId>poppydb</artifactId>
    <version>6.2.0</version>
</dependency>
```

**Als Standalone-Server (Download):**
```bash
# Fat-JAR von Maven Central herunterladen
java -jar poppydb-6.2.0-cli.jar -p 27017
```

## Risiken

1. **Multi-Module-Umbau:** Maven-Multi-Module kann bei Plugins (Surefire, Assembly, etc.) Überraschungen bringen. Sorgfältig testen.
2. **CI-Anpassung:** Test-Runner-Scripts müssen das neue Modul kennen.
3. **InMemoryDriver-Kopplung:** PoppyDB ist eng an den InMemoryDriver gekoppelt. Wenn sich dessen API ändert, bricht PoppyDB. Akzeptables Risiko, da beides im selben Repo lebt.

## Nicht im Scope

- Eigenes Git-Repository für PoppyDB (bleibt im Morphium-Monorepo)
- Eigene Website / Domain
- Logo-Design
- Separate Versionierung (PoppyDB folgt der Morphium-Version)
