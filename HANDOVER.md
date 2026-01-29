# Morphium Handover Guide
## Für erfahrene Entwickler

*Stand: 2026-01-29 | Version: 6.1.x*

---

## TL;DR

Morphium ist ein **MongoDB ODM + Messaging Framework** für Java 21+. Einzigartig: Die Message Queue läuft *in* MongoDB — kein RabbitMQ/Kafka nötig.

```
┌─────────────────────────────────────────────────────┐
│                    Morphium                         │
│  ┌─────────┐  ┌───────────┐  ┌─────────┐           │
│  │   ODM   │  │ Messaging │  │  Cache  │           │
│  └────┬────┘  └─────┬─────┘  └────┬────┘           │
│       └─────────────┼─────────────┘                │
│              ┌──────┴──────┐                       │
│              │   Driver    │ (3 Varianten)         │
│              └──────┬──────┘                       │
└─────────────────────┼───────────────────────────────┘
                      │
               ┌──────┴──────┐
               │   MongoDB   │
               └─────────────┘
```

---

## Projekt-Struktur

```
morphium/
├── src/main/java/de/caluga/morphium/
│   ├── Morphium.java              # Zentrale Fassade (130KB!)
│   ├── MorphiumConfig.java        # Konfiguration
│   ├── ObjectMapperImpl.java      # POJO ↔ BSON Mapping
│   ├── driver/                    # Wire Protocol Implementierung
│   │   ├── inmem/                 # In-Memory Driver (Testing)
│   │   ├── wire/                  # MongoDB Wire Protocol
│   │   └── *.java                 # Connection Pool, etc.
│   ├── messaging/                 # Message Queue System
│   ├── cache/                     # Multi-Level Caching
│   ├── aggregation/               # Aggregation Pipeline
│   ├── annotations/               # @Entity, @Id, @Property, etc.
│   └── changestream/              # MongoDB Change Streams
├── src/test/java/                 # JUnit 5 Tests
├── docs/                          # MkDocs Dokumentation
├── runtests.sh                    # Test-Runner (wichtig!)
└── scripts/                       # Helper-Scripts
```

---

## Die 3 Driver-Varianten

| Driver | Use Case | Connection Handling |
|--------|----------|---------------------|
| **PooledDriver** | Produktion | Connection Pool, Failover, Heartbeat |
| **SingleMongoConnectDriver** | Low-Concurrency | Eine Connection pro Operation |
| **InMemoryDriver** | Tests | Kein MongoDB nötig, ~93% Feature Coverage |

**Wichtig:** Der InMemory-Driver emuliert MongoDB vollständig im RAM — perfekt für schnelle Unit-Tests ohne externe Abhängigkeiten.

---

## Messaging System

Das Alleinstellungsmerkmal: Message Queue direkt in MongoDB.

```java
// Producer
Messaging m = new Messaging(morphium, 100, true);
m.sendMessage(new Msg("topic", "payload", "value"));

// Consumer
m.addMessageListener((messaging, msg) -> {
    // Handle message
    return null; // oder Antwort-Message
});
```

**Zwei Implementierungen:**
1. **ChangeStream-basiert** (Default) — Push-Modell, effizient
2. **Polling-basiert** — für ältere MongoDB-Versionen

**Features:**
- Message Priority
- Request/Response Pattern
- Distributed Locks
- TTL & Timeouts
- Broadcast & Direct Messages

---

## Bekannte Pitfalls (aus MEMORY.md)

### 1. Hostname Case-Mismatch (Pool Exhaustion)
MongoDB meldet Hostnamen anders als Seed (`SERV-MSG1` vs `serv-msg1`).
**Fix (2026-01-28):** Alles auf lowercase normalisieren.

### 2. Lock-TTL bei `timingOut=false`
Messages ohne Timeout hatten `TTL=0` → Lock lief sofort ab.
**Fix:** 7-Tage-Fallback wenn kein TTL gesetzt.

### 3. ChangeStreamMonitor Stabilität
"connection closed" führte zu permanentem Stop.
**Fix:** Retry-Logic + besseres Logging.

---

## Build & Test

```bash
# Schneller Build (ohne Tests)
mvn clean install -DskipTests

# Alle Tests mit InMemory-Driver (schnellste Variante)
./runtests.sh

# Tests mit echtem MongoDB
./runtests.sh --driver pooled --uri "mongodb://host:27017/test"

# Nur bestimmte Tags
./runtests.sh --tags core
./runtests.sh --tags messaging
./runtests.sh --tags driver

# Parallele Ausführung
./runtests.sh --parallel 4

# Einzelne Testklasse
./runtests.sh --test-class de.caluga.test.morphium.BasicFunctionalityTests
```

**Test-Kategorien (JUnit Tags):**
- `core` — ODM Basisfunktionalität
- `messaging` — Message Queue
- `cache` — Caching Layer
- `driver` — Wire Protocol Driver

---

## Wichtige Klassen (Entry Points)

| Klasse | Verantwortung |
|--------|---------------|
| `Morphium.java` | Zentrale API, alle CRUD Ops |
| `MorphiumConfig.java` | Konfiguration (DB, Pool, Timeouts) |
| `ObjectMapperImpl.java` | Java ↔ BSON Konvertierung |
| `Query.java` | Fluent Query Builder |
| `Messaging.java` | Message Queue API |
| `PooledDriver.java` | Production Driver mit Pool |
| `InMemDriver.java` | Test Driver (In-Memory) |

---

## Konfiguration (Minimal)

```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("mydb");
cfg.addHostToSeed("localhost:27017");

Morphium morphium = new Morphium(cfg);
```

**Für Tests:**
```java
MorphiumConfig cfg = new MorphiumConfig();
cfg.setDatabase("test");
cfg.setDriverName(InMemoryDriver.class.getName());

Morphium morphium = new Morphium(cfg);
```

---

## Annotations Quick Reference

```java
@Entity(collectionName = "users")
public class User {
    @Id
    private MorphiumId id;
    
    @Property(fieldName = "user_name")
    @Index
    private String username;
    
    @Reference
    private Department department;
    
    @Embedded
    private Address address;
    
    @Transient
    private String tempData;  // Nicht persistiert
}
```

---

## Release-Prozess

```bash
# Release vorbereiten (setzt Version, erstellt Tag)
./release.sh

# Dokumentation deployen
./deploy_docs.sh
```

**Maven Central Deployment:** Automatisch via GitHub Actions bei Tag-Push.

---

## Weiterführende Dokumentation

| Thema | Datei |
|-------|-------|
| Architektur-Details | `docs/architecture-overview.md` |
| Konfiguration komplett | `docs/configuration-reference.md` |
| Performance Tuning | `docs/performance-scalability-guide.md` |
| Production Deployment | `docs/production-deployment-guide.md` |
| Migration v5→v6 | `docs/howtos/migration-v5-to-v6.md` |
| InMemory Driver Details | `docs/inmemory-driver.md` |
| Messaging Deep-Dive | `docs/messaging.md` |
| Troubleshooting | `docs/troubleshooting-guide.md` |

---

## Offene Punkte / TODOs

- [ ] Test-Stabilisierung (aktuell laufend — James)
- [ ] IPv6 Support im Driver (aktuell nicht unterstützt)
- [ ] MorphiumServer in Produktion bringen

---

## Kontakt

**Maintainer:** Stephan Bösebeck
**Repo:** [GitHub](https://github.com/sboesebeck/morphium)
**Issues:** GitHub Issues verwenden

---

*Erstellt von Nigel 🎩 für das Übergabe-Team*
